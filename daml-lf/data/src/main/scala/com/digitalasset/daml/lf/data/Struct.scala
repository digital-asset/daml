// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import IdString.`Name order instance`
import Ref.Name
import ScalazEqual._

import scala.collection
import scalaz.std.iterable._
import scalaz.std.tuple._
import scalaz.syntax.order._
import scalaz.{Equal, Order}

/** We use this container to describe structural record as sorted flat list in various parts of the codebase.
  *    `entries` are sorted by their first component without duplicate.
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
  def fromSeq[X](fields: collection.Seq[(Name, X)]): Either[Name, Struct[X]] =
    if (fields.isEmpty) rightEmpty
    else {
      val struct = Struct(fields.sortBy(_._1: String).to(ImmArray))
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

  private[this] def assertSuccess[X](either: Either[String, X]) =
    either.fold(
      name =>
        throw new IllegalArgumentException(s"name $name duplicated when trying to build Struct"),
      identity,
    )

  def assertFromSeq[X](fields: Seq[(Name, X)]): Struct[X] =
    assertSuccess(fromSeq(fields))

  def fromNameSeq[X](names: Seq[Name]): Either[Name, Struct[Unit]] =
    fromSeq(names.map(_ -> (())))

  def assertFromNameSeq[X](names: Seq[Name]): Struct[Unit] =
    assertSuccess(fromNameSeq(names))

  val Empty: Struct[Nothing] = new Struct(ImmArray.Empty)

  private[this] val rightEmpty = Right(Empty)

  implicit def structEqualInstance[X: Equal]: Equal[Struct[X]] =
    _.toImmArray === _.toImmArray

  implicit def structOrderInstance[X: Order]: Order[Struct[X]] =
    // following daml-lf specification, this considers first names, then values.
    orderBy(
      s => (toIterableForScalazInstances(s.names), toIterableForScalazInstances(s.values)),
      true,
    )

}
