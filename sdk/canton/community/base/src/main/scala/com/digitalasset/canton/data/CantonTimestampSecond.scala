// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.kernel.Order
import cats.syntax.either.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.{LfTimestamp, ProtoDeserializationError}
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import slick.jdbc.{GetResult, SetParameter}

import java.time.{Duration, Instant}

/** A timestamp implementation for canton, which currently uses a [[LfTimestamp]],
  * which is rounded to the second.
  *
  * @param underlying A [[LfTimestamp]], holding the value of this [[CantonTimestampSecond]].
  */
final case class CantonTimestampSecond private (underlying: LfTimestamp)
    extends Ordered[CantonTimestampSecond]
    with Timestamp {

  require(microsOverSecond() == 0, s"Timestamp $underlying must be rounded to the second")

  def forgetRefinement: CantonTimestamp = CantonTimestamp(underlying)

  def plusSeconds(seconds: Long): CantonTimestampSecond =
    CantonTimestampSecond(underlying.add(Duration.ofSeconds(seconds)))

  def minusSeconds(seconds: Long): CantonTimestampSecond = plusSeconds(-seconds)

  override def compare(that: CantonTimestampSecond): Int = underlying.compare(that.underlying)

  override def compareTo(other: CantonTimestampSecond): Int = underlying.compareTo(other.underlying)

  def -(other: CantonTimestampSecond): Duration =
    Duration.ofNanos(1000L * (this.underlying.micros - other.underlying.micros))

  def +(duration: PositiveSeconds): CantonTimestampSecond = CantonTimestampSecond(
    underlying.add(duration.duration)
  )

  def -(duration: PositiveSeconds): CantonTimestampSecond = CantonTimestampSecond(
    underlying.add(Duration.ZERO.minus(duration.duration))
  )

  def >(other: CantonTimestamp): Boolean = forgetRefinement > other
  def >=(other: CantonTimestamp): Boolean = forgetRefinement >= other

  def <(other: CantonTimestamp): Boolean = forgetRefinement < other
  def <=(other: CantonTimestamp): Boolean = forgetRefinement <= other

}

object CantonTimestampSecond {
  def max(
      timestamp: CantonTimestampSecond,
      timestamps: CantonTimestampSecond*
  ): CantonTimestampSecond = {
    timestamps.foldLeft(timestamp) { case (a, b) =>
      if (a > b) a else b
    }
  }

  def min(
      timestamp: CantonTimestampSecond,
      timestamps: CantonTimestampSecond*
  ): CantonTimestampSecond = {
    timestamps.foldLeft(timestamp) { case (a, b) =>
      if (a < b) a else b
    }
  }

  def Epoch = CantonTimestampSecond(LfTimestamp.Epoch)

  def MinValue = CantonTimestampSecond(LfTimestamp.MinValue)

  def fromProtoTimestamp(
      ts: ProtoTimestamp,
      field: String,
  ): ParsingResult[CantonTimestampSecond] = {
    for {
      instant <- ProtoConverter.InstantConverter.fromProtoPrimitive(ts)
      ts <- CantonTimestampSecond
        .fromInstant(instant)
        .left
        .map(ProtoDeserializationError.InvariantViolation(field, _))
    } yield ts
  }

  def fromProtoPrimitive(field: String, ts: Long): ParsingResult[CantonTimestampSecond] = {
    for {
      timestamp <- CantonTimestamp.fromProtoPrimitive(ts)
      seconds <- CantonTimestampSecond
        .fromCantonTimestamp(timestamp)
        .leftMap(ProtoDeserializationError.InvariantViolation(field, _))
    } yield seconds
  }

  def ofEpochSecond(seconds: Long): CantonTimestampSecond =
    CantonTimestampSecond(LfTimestamp.assertFromLong(micros = seconds * 1000 * 1000))

  def fromInstant(i: Instant): Either[String, CantonTimestampSecond] =
    for {
      _ <- Either.cond(i.getNano == 0, (), s"Timestamp $i is not rounded to the second")
      ts <- LfTimestamp.fromInstant(i)
    } yield CantonTimestampSecond(ts)

  def fromCantonTimestamp(ts: CantonTimestamp): Either[String, CantonTimestampSecond] =
    Either.cond(
      ts.microsOverSecond() == 0,
      CantonTimestampSecond(ts.underlying),
      s"Timestamp $ts is not rounded to the second",
    )

  /** @param ts
    * @return `ts` if `ts` is already rounded to the second, the previous rounded timestamp otherwise.
    */
  def floor(ts: CantonTimestamp): CantonTimestampSecond =
    if (ts.microsOverSecond() == 0) CantonTimestampSecond(ts.underlying)
    else CantonTimestampSecond.ofEpochSecond(ts.getEpochSecond)

  /** @param ts
    * @return `ts` if `ts` is already rounded to the second, the next rounded timestamp otherwise.
    */
  def ceil(ts: CantonTimestamp): CantonTimestampSecond =
    if (ts.microsOverSecond() == 0) CantonTimestampSecond(ts.underlying)
    else CantonTimestampSecond.ofEpochSecond(ts.getEpochSecond + 1)

  def assertFromInstant(i: Instant) = CantonTimestampSecond(LfTimestamp.assertFromInstant(i))
  def assertFromLong(micros: Long) = CantonTimestampSecond(LfTimestamp.assertFromLong(micros))

  implicit val orderCantonTimestampSecond: Order[CantonTimestampSecond] = Order.fromOrdering

  // Timestamps are stored as microseconds relative to EPOCH in a `bigint` rather than a SQL `timestamp`.
  // This avoids all the time zone conversions introduced by various layers that are hard to make consistent
  // across databases.
  implicit val setParameterTimestamp: SetParameter[CantonTimestampSecond] = (v, pp) =>
    pp.setLong(v.toMicros)
  implicit val setParameterOptionTimestamp: SetParameter[Option[CantonTimestampSecond]] = (v, pp) =>
    pp.setLongOption(v.map(_.toMicros))
  implicit val getResultTimestamp: GetResult[CantonTimestampSecond] =
    GetResult(r => CantonTimestampSecond.assertFromLong(r.nextLong()))
  implicit val getResultOptionTimestamp: GetResult[Option[CantonTimestampSecond]] =
    GetResult(r => r.nextLongOption().map(CantonTimestampSecond.assertFromLong))
}
