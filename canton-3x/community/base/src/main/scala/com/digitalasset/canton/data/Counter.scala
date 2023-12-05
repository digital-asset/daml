// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.{GetResult, SetParameter}

final case class Counter[Discr](v: Long) extends Ordered[Counter[Discr]] with PrettyPrinting {
  def unwrap: Long = v
  def toProtoPrimitive: Long = v

  def +(i: Long): Counter[Discr] = Counter(v + i)
  def -(i: Long): Counter[Discr] = Counter(v - i)

  def +(i: Int): Counter[Discr] = Counter(v + i)

  def increment: Either[String, Counter[Discr]] =
    Either.cond(this.isNotMaxValue, this + 1, "Counter Overflow")

  def -(i: Int): Counter[Discr] = Counter(v - i)

  def +(other: Counter[Discr]): Counter[Discr] = Counter(v + other.v)
  def -(other: Counter[Discr]): Long = v - other.v

  def max(other: Counter[Discr]): Counter[Discr] = Counter[Discr](v.max(other.v))

  def until(end: Counter[Discr]): Seq[Counter[Discr]] = (v until end.v).map(Counter(_))
  def to(end: Counter[Discr]): Seq[Counter[Discr]] = (v to end.v).map(Counter(_))

  def isMaxValue: Boolean = v == Long.MaxValue
  def isNotMaxValue: Boolean = !isMaxValue

  override def compare(that: Counter[Discr]): Int = v.compare(that.v)

  override def pretty: Pretty[Counter.this.type] = prettyOfString(_ => v.toString)
}

trait CounterCompanion[T] {

  /** The request counter assigned to the first request in the lifetime of a participant */
  val Genesis: Counter[T] = Counter[T](0)
  val MaxValue: Counter[T] = Counter[T](Long.MaxValue)
  val MinValue: Counter[T] = Counter[T](Long.MinValue)

  def apply(i: Long): Counter[T] = Counter[T](i)

  def apply(i: Int): Counter[T] = Counter[T](i.toLong)

  def unapply(sc: Counter[T]): Option[Long] = Some(sc.unwrap)
}

object Counter {
  def MaxValue[Discr]: Counter[Discr] = Counter[Discr](Long.MaxValue)
  def MinValue[Discr]: Counter[Discr] = Counter[Discr](Long.MinValue)

  implicit def getResult[Discr]: GetResult[Counter[Discr]] =
    GetResult(r => Counter[Discr](r.nextLong()))
  implicit def getResultO[Discr]: GetResult[Option[Counter[Discr]]] =
    GetResult(r => r.nextLongOption().map(Counter[Discr]))

  implicit def setParameter[Discr]: SetParameter[Counter[Discr]] = { (value, pp) => pp >> value.v }
  implicit def setParameterO[Discr]: SetParameter[Option[Counter[Discr]]] = { (value, pp) =>
    pp >> value.map(_.v)
  }
}
