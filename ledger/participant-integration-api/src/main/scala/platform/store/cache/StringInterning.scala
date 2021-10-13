// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.lf.data.Ref

trait StringInterning {
  def templateId: StringInterningDomain[Ref.Identifier]
  def party: StringInterningDomain[Ref.Party]
}

trait StringInterningDomain[T] extends StringInterningAccessor[T] {
  def unsafe: StringInterningAccessor[String]
}

trait StringInterningAccessor[T] {
  def internalize(t: T): Int
  def tryInternalize(t: T): Option[Int]
  def externalize(id: Int): T
  def tryExternalize(id: Int): Option[T]
}

case class RawStringInterningCache(
    map: Map[String, Int],
    idMap: Map[Int, String], // TODO fixme this is not necessary for the Indexer side (extension over this for the read side? guava bimap? Vector?)
    lastId: Int,
)

class StringInterningCache(initialRaw: RawStringInterningCache = RawStringInterningCache.from(Nil))
    extends StringInterning {
  @volatile var raw: RawStringInterningCache = initialRaw

  private val TemplatePrefix = "t|"
  private val PartyPrefix = "p|"

  def allRawEntries(
      templateEntries: Iterator[String],
      partyEntries: Iterator[String],
  ): Iterator[String] =
    templateEntries.map(TemplatePrefix + _).++(partyEntries.map(PartyPrefix + _))

  override val templateId: StringInterningDomain[Ref.Identifier] =
    new StringInterningDomain[Ref.Identifier] {
      override val unsafe: StringInterningAccessor[String] = new StringInterningAccessor[String] {
        override def internalize(t: String): Int = raw.map(TemplatePrefix + t)

        override def tryInternalize(t: String): Option[Int] = raw.map.get(TemplatePrefix + t)

        override def externalize(id: Int): String = raw.idMap(id).substring(TemplatePrefix.length)

        override def tryExternalize(id: Int): Option[String] =
          raw.idMap.get(id).map(_.substring(TemplatePrefix.length))
      }

      override def internalize(t: Ref.Identifier): Int = unsafe.internalize(t.toString)

      override def tryInternalize(t: Ref.Identifier): Option[Int] =
        unsafe.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.Identifier =
        Ref.Identifier.assertFromString(unsafe.externalize(id))

      override def tryExternalize(id: Int): Option[Ref.Identifier] =
        unsafe.tryExternalize(id).map(Ref.Identifier.assertFromString)
    }

  override def party: StringInterningDomain[Ref.Party] = new StringInterningDomain[Ref.Party] {
    override val unsafe: StringInterningAccessor[String] = new StringInterningAccessor[String] {
      override def internalize(t: String): Int = raw.map(PartyPrefix + t)

      override def tryInternalize(t: String): Option[Int] = raw.map.get(PartyPrefix + t)

      override def externalize(id: Int): String = raw.idMap(id).substring(PartyPrefix.length)

      override def tryExternalize(id: Int): Option[String] =
        raw.idMap.get(id).map(_.substring(PartyPrefix.length))
    }

    override def internalize(t: Ref.Party): Int = unsafe.internalize(t.toString)

    override def tryInternalize(t: Ref.Party): Option[Int] = unsafe.tryInternalize(t.toString)

    override def externalize(id: Int): Ref.Party =
      Ref.Party.assertFromString(unsafe.externalize(id))

    override def tryExternalize(id: Int): Option[Ref.Party] =
      unsafe.tryExternalize(id).map(Ref.Party.assertFromString)
  }
}

object RawStringInterningCache {

  def from(
      entries: Iterable[(Int, String)],
      rawStringInterningCache: RawStringInterningCache =
        RawStringInterningCache(Map.empty, Map.empty, 0),
  ): RawStringInterningCache =
    if (entries.isEmpty)
      rawStringInterningCache
    else
      RawStringInterningCache(
        map = rawStringInterningCache.map ++ entries.view.map(_.swap),
        idMap = rawStringInterningCache.idMap ++ entries,
        lastId = entries.view.map(_._1).max,
      )

  def newEntries(
      strings: Iterator[String],
      rawStringInterningCache: RawStringInterningCache,
  ): Vector[(Int, String)] =
    strings
      .filterNot(rawStringInterningCache.map.contains)
      .distinct
      .zipWithIndex
      .map { case (string, index) =>
        (index + 1 + rawStringInterningCache.lastId, string)
      }
      .toVector
}
