// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.ProtoDeserializationError.TimestampConversionError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.RefinedDuration
import com.digitalasset.canton.util.TryUtil
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import slick.jdbc.{GetResult, SetParameter}

import java.time.{Duration, Instant}
import java.util.Date

/** A timestamp implementation for canton, which currently uses a [[LfTimestamp]].
  * @param underlying A [[LfTimestamp]], holding the value of this [[CantonTimestamp]].
  */
final case class CantonTimestamp(underlying: LfTimestamp)
    extends Ordered[CantonTimestamp]
    with Timestamp {

  def minus(d: Duration): CantonTimestamp = new CantonTimestamp(
    underlying.add(Duration.ZERO.minus(d))
  )

  def plus(d: Duration): CantonTimestamp = new CantonTimestamp(underlying.add(d))

  def add(d: Duration): CantonTimestamp = new CantonTimestamp(underlying.add(d))

  def addMicros(micros: Long): CantonTimestamp = new CantonTimestamp(underlying.addMicros(micros))

  def plusSeconds(seconds: Long): CantonTimestamp =
    new CantonTimestamp(underlying.add(Duration.ofSeconds(seconds)))

  def minusSeconds(seconds: Long): CantonTimestamp = this.minus(Duration.ofSeconds(seconds))

  def plusMillis(millis: Long): CantonTimestamp = new CantonTimestamp(
    underlying.add(Duration.ofMillis(millis))
  )

  def minusMillis(millis: Long): CantonTimestamp =
    new CantonTimestamp(underlying.add(Duration.ZERO.minus(Duration.ofMillis(millis))))

  def immediatePredecessor: CantonTimestamp = new CantonTimestamp(underlying.addMicros(-1L))

  def immediateSuccessor: CantonTimestamp = new CantonTimestamp(underlying.addMicros(1L))

  override def compare(that: CantonTimestamp): Int = underlying.compare(that.underlying)

  override def compareTo(other: CantonTimestamp): Int = underlying.compareTo(other.underlying)

  def min(that: CantonTimestamp): CantonTimestamp = if (compare(that) > 0) that else this

  def max(that: CantonTimestamp): CantonTimestamp = if (compare(that) > 0) this else that

  def -(other: CantonTimestamp): Duration =
    Duration.ofNanos(1000L * (this.underlying.micros - other.underlying.micros))

  def +(duration: RefinedDuration): CantonTimestamp = plus(duration.unwrap)
  def -(duration: RefinedDuration): CantonTimestamp = minus(duration.unwrap)

  def <=(other: CantonTimestampSecond): Boolean = this <= other.forgetRefinement
  def <(other: CantonTimestampSecond): Boolean = this < other.forgetRefinement

  def >=(other: CantonTimestampSecond): Boolean = this >= other.forgetRefinement
  def >(other: CantonTimestampSecond): Boolean = this > other.forgetRefinement
}

object CantonTimestamp {

  def Epoch: CantonTimestamp = new CantonTimestamp(LfTimestamp.Epoch)

  def MinValue: CantonTimestamp = new CantonTimestamp(LfTimestamp.MinValue)

  def MaxValue: CantonTimestamp = new CantonTimestamp(LfTimestamp.MaxValue)

  def fromProtoPrimitive(ts: ProtoTimestamp): ParsingResult[CantonTimestamp] = {
    for {
      instant <- ProtoConverter.InstantConverter.fromProtoPrimitive(ts)
      ts <- LfTimestamp.fromInstant(instant).left.map(err => TimestampConversionError(err))
    } yield new CantonTimestamp(ts)
  }

  def ofEpochSecond(seconds: Long): CantonTimestamp =
    new CantonTimestamp(LfTimestamp.assertFromLong(micros = seconds * 1000 * 1000))

  def ofEpochMilli(milli: Long): CantonTimestamp =
    new CantonTimestamp(LfTimestamp.assertFromLong(micros = milli * 1000))

  def ofEpochMicro(micros: Long): CantonTimestamp = assertFromLong(micros)

  /** Get Instant.now (try to use clock.now instead!)
    *
    *  Generally, try to use clock.now except for tests. Clock.now supports sim-clock such that
    *  we can perform static time tests.
    */
  def now(): CantonTimestamp = new CantonTimestamp(LfTimestamp.assertFromInstant(Instant.now()))

  def fromInstant(i: Instant): Either[String, CantonTimestamp] =
    LfTimestamp.fromInstant(i).map(t => new CantonTimestamp(t))

  def fromDate(javaDate: Date): Either[String, CantonTimestamp] =
    for {
      instant <- TryUtil.tryCatchInterrupted(javaDate.toInstant).toEither.leftMap(_.getMessage)
      cantonTimestamp <- CantonTimestamp.fromInstant(instant)
    } yield cantonTimestamp

  def assertFromInstant(i: Instant) = new CantonTimestamp(LfTimestamp.assertFromInstant(i))
  def assertFromLong(micros: Long) = new CantonTimestamp(LfTimestamp.assertFromLong(micros))

  implicit val orderCantonTimestamp: Order[CantonTimestamp] = Order.fromOrdering

  // Timestamps are stored as microseconds relative to EPOCH in a `bigint` rather than a SQL `timestamp`.
  // This avoids all the time zone conversions introduced by various layers that are hard to make consistent
  // across databases.
  implicit val setParameterTimestamp: SetParameter[CantonTimestamp] = (v, pp) =>
    pp.setLong(v.toMicros)
  implicit val setParameterOptionTimestamp: SetParameter[Option[CantonTimestamp]] = (v, pp) =>
    pp.setLongOption(v.map(_.toMicros))
  implicit val getResultTimestamp: GetResult[CantonTimestamp] =
    GetResult(r => CantonTimestamp.assertFromLong(r.nextLong()))
  implicit val getResultOptionTimestamp: GetResult[Option[CantonTimestamp]] =
    GetResult(r => r.nextLongOption().map(CantonTimestamp.assertFromLong))
}
