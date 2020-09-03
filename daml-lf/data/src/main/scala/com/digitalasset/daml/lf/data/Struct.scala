// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import com.daml.lf.data.Ref.Name

/** We use this container to describe structural record as sorted flat list in various parts of the codebase.
    `entries` are sorted by their first component without duplicate.
  */
final case class Struct[+X] private (private val sortedFields: ImmArray[(Ref.Name, X)])
    extends NoCopy {

  /** O(n) */
  @throws[IndexOutOfBoundsException]
  def apply(name: Ref.Name): X = sortedFields(indexOf(name))._2

  /** O(n) */
  def indexOf(name: Ref.Name): Int = sortedFields.indexWhere(_._1 == name)

  /** O(n) */
  def lookup(name: Ref.Name): Option[X] = sortedFields.find(_._1 == name).map(_._2)

  /** O(n) */
  def mapValues[Y](f: X => Y) = new Struct(sortedFields.map { case (k, v) => k -> f(v) })

  /** O(1) */
  def toImmArray: ImmArray[(Ref.Name, X)] = sortedFields

  /** O(1) */
  def names: Iterator[Ref.Name] = iterator.map(_._1)

  /** O(1) */
  def values: Iterator[X] = iterator.map(_._2)

  /** O(1) */
  def iterator: Iterator[(Ref.Name, X)] = sortedFields.iterator

  /** O(1) */
  def size: Int = sortedFields.length

  /** O(n) */
  override def toString: String = iterator.mkString("Struct(", ",", ")")

}

object Struct {

  /** Constructs a Struct.
    * In case one of the field name is duplicated, return it as Left.
    * O(n log n)
    */
  def fromSeq[X](fields: Seq[(Name, X)]): Either[Name, Struct[X]] =
    if (fields.isEmpty) rightEmpty
    else {
      val struct = Struct(ImmArray(fields.sortBy(_._1: String)))
      val names = struct.names
      var previous = names.next()
      names
        .find { name =>
          val found = name == previous
          previous = name
          found
        }
        .toLeft(struct)
    }

  def assertFromSeq[X](fields: Seq[(Name, X)]): Struct[X] =
    fromSeq(fields).fold(
      name =>
        throw new IllegalArgumentException(s"name $name duplicated when trying to build Struct"),
      identity,
    )

  val Empty: Struct[Nothing] = new Struct(ImmArray.empty)

  private[this] val rightEmpty = Right(Empty)

}
