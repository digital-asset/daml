// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import IdString.`Name order instance`
import Ref.Name
import ScalazEqual._

import scalaz.std.iterable._
import scalaz.std.tuple._
import scalaz.syntax.order._
import scalaz.{Equal, Order}

import scala.annotation.tailrec
import scala.collection.immutable._

final class Struct[+X] private (protected val sortedFields: ArraySeq[(Ref.Name, X)])
    extends AnyVal {

  /** O(log n) */
  @throws[IndexOutOfBoundsException]
  def apply(name: Ref.Name): X = sortedFields(indexOf(name))._2

  /** O(log n) */
  def indexOf(name: Ref.Name): Int = {
    @tailrec
    def loop(from: Int, to: Int): Int =
      if (to < from)
        -1
      else {
        val idx = (from + to) / 2
        val c = name compareTo sortedFields(idx)._1
        if (c < 0) loop(from, idx - 1)
        else if (c > 0) loop(idx + 1, to)
        else idx
      }
    loop(0, size - 1)
  }

  /** O(log n) */
  def get(name: Ref.Name): Option[X] = {
    val idx = indexOf(name)
    if (idx < 0) None else Some(sortedFields(idx)._2)
  }

  /** O(n) */
  def mapValues[Y](f: X => Y): Struct[Y] = new Struct(sortedFields.map { case (k, v) => k -> f(v) })

  /** O(1) */
  def toImmArray: ImmArray[(Ref.Name, X)] = ImmArray.fromArraySeq(sortedFields)

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

  def unapplySeq[X](v: Struct[X]): Some[Seq[(Ref.Name, X)]] =
    Some(v.sortedFields)

  /** Constructs a Struct.
    * In case one of the field name is duplicated, return it as Left.
    * O(n log n)
    */
  def fromSeq[X](fields: collection.Seq[(Name, X)]): Either[Name, Struct[X]] =
    if (fields.isEmpty) rightEmpty
    else {
      val sortedFields = fields.to(ArraySeq).sortBy(_._1: String)
      val names = sortedFields.iterator.map(_._1)
      var previous = names.next()
      names
        .find { name =>
          val found = name == previous
          previous = name
          found
        }
        .toLeft(new Struct(sortedFields))
    }

  private[this] def assertSuccess[X](either: Either[String, X]): X =
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

  val Empty: Struct[Nothing] = new Struct(ArraySeq.empty)

  private[this] val rightEmpty = Right(Empty)

  implicit def structEqualInstance[X: Equal]: Equal[Struct[X]] =
    _.sortedFields === _.sortedFields

  implicit def structOrderInstance[X: Order]: Order[Struct[X]] =
    // following daml-lf specification, this considers first names, then values.
    orderBy(
      s => (toIterableForScalazInstances(s.names), toIterableForScalazInstances(s.values)),
      true,
    )

}
