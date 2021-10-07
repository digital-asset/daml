// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

case class StringInterningCache(
    map: Map[String, Int],
    idMap: Map[Int, String], // TODO fixme this is not necessary for the Indexer side (extension onver this for the read side? guava bimap?)
    lastId: Int,
)

object StringInterningCache {

  def from(
      entries: Iterable[(Int, String)],
      stringInterningCache: StringInterningCache = StringInterningCache(Map.empty, Map.empty, 0),
  ): StringInterningCache =
    if (entries.isEmpty)
      stringInterningCache
    else
      StringInterningCache(
        map = stringInterningCache.map ++ entries.view.map(_.swap),
        idMap = stringInterningCache.idMap ++ entries,
        lastId = entries.view.map(_._1).max,
      )

  def newEntries(
      strings: Iterator[String],
      stringInterningCache: StringInterningCache,
  ): Vector[(Int, String)] =
    strings
      .filterNot(stringInterningCache.map.contains)
      .distinct
      .zipWithIndex
      .map { case (string, index) =>
        (index + 1 + stringInterningCache.lastId, string)
      }
      .toVector
}
