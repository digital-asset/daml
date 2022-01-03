// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.api.util

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.v1.value.Value
import com.google.protobuf.timestamp.{Timestamp => ProtoTimestamp}
import com.daml.lf.data.Time.{Timestamp => LfTimestamp}

object TimestampConversion {
  val MIN = Instant parse "0001-01-01T00:00:00Z"
  val MAX = Instant parse "9999-12-31T23:59:59.999999Z"

  def microsToInstant(micros: Value.Sum.Timestamp): Instant = {
    val seconds = TimeUnit.MICROSECONDS.toSeconds(micros.value)
    val deltaMicros = micros.value - TimeUnit.SECONDS.toMicros(seconds)
    Instant.ofEpochSecond(seconds, TimeUnit.MICROSECONDS.toNanos(deltaMicros))
  }

  def instantToMicros(t: Instant): Value.Sum.Timestamp = {
    if (t.getNano % 1000 != 0)
      throw new IllegalArgumentException(
        s"Conversion of Instant $t to microsecond granularity would result in loss of precision."
      )
    else
      Value.Sum.Timestamp(
        TimeUnit.SECONDS.toMicros(t.getEpochSecond) + TimeUnit.NANOSECONDS
          .toMicros(t.getNano.toLong)
      )

  }

  def roundInstantToMicros(t: Instant): Value.Sum.Timestamp = {
    instantToMicros(roundToMicros(t, ConversionMode.HalfUp))
  }

  def toInstant(protoTimestamp: ProtoTimestamp): Instant = {
    Instant.ofEpochSecond(protoTimestamp.seconds, protoTimestamp.nanos.toLong)
  }

  def fromInstant(instant: Instant): ProtoTimestamp = {
    new ProtoTimestamp().withSeconds(instant.getEpochSecond).withNanos(instant.getNano)
  }

  def toLf(protoTimestamp: ProtoTimestamp, mode: ConversionMode): LfTimestamp = {
    val instant = roundToMicros(toInstant(protoTimestamp), mode)
    LfTimestamp.assertFromInstant(instant)
  }

  def fromLf(timestamp: LfTimestamp): ProtoTimestamp = {
    fromInstant(timestamp.toInstant)
  }

  private def roundToMicros(t: Instant, mode: ConversionMode): Instant = {
    val fractionNanos = t.getNano % 1000L
    if (fractionNanos != 0) {
      mode match {
        case ConversionMode.Exact =>
          throw new IllegalArgumentException(
            s"Conversion of $t to microsecond granularity would result in loss of precision."
          )
        case ConversionMode.HalfUp =>
          t.plusNanos(if (fractionNanos >= 500L) 1000L - fractionNanos else -fractionNanos)
      }
    } else {
      t
    }
  }

  sealed trait ConversionMode
  object ConversionMode {

    /** Throw an exception if the input can not be represented in microsecond resolution */
    case object Exact extends ConversionMode

    /** Round to the nearest microsecond */
    case object HalfUp extends ConversionMode
  }
}
