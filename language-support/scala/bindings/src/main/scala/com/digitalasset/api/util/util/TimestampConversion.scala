// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.api.util

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.v1.value.Value
import com.google.protobuf.timestamp.Timestamp

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
        s"Conversion of Instant $t to microsecond granularity would result in loss of precision.")
    else
      Value.Sum.Timestamp(
        TimeUnit.SECONDS.toMicros(t.getEpochSecond) + TimeUnit.NANOSECONDS
          .toMicros(t.getNano.toLong))

  }

  def toInstant(protoTimestamp: Timestamp): Instant = {
    Instant.ofEpochSecond(protoTimestamp.seconds, protoTimestamp.nanos.toLong)
  }

  def fromInstant(instant: Instant): Timestamp = {
    new Timestamp().withSeconds(instant.getEpochSecond).withNanos(instant.getNano)
  }
}
