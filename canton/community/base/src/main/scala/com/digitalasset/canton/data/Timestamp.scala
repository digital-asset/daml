// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp

import java.time.Instant
import java.util.Date

trait Timestamp extends PrettyPrinting {
  def underlying: LfTimestamp

  def toLf: LfTimestamp = underlying

  def isAfter(t: CantonTimestamp): Boolean = underlying.compareTo(t.underlying) > 0

  def isBefore(t: CantonTimestamp): Boolean = underlying.compareTo(t.underlying) < 0

  /** Use this method to convert the timestamp into a [[com.google.protobuf.timestamp.Timestamp]].
    * This Protobuf type should be used in messages for external APIs such as the Canton admin APIs.
    *
    * @see toProtoPrimitive
    */
  def toProtoTimestamp: ProtoTimestamp =
    ProtoConverter.InstantConverter.toProtoPrimitive(underlying.toInstant)

  /** Use this method to convert the timestamp into UTC microseconds since UNIX epoch.
    * This Protobuf type should be used in all protocol messages that obey the protocol versioning regime.
    *
    * @see toProtoTimestamp
    */
  def toProtoPrimitive: Long = underlying.micros

  def getEpochSecond: Long = underlying.toInstant.getEpochSecond

  def toEpochMilli: Long = underlying.toInstant.toEpochMilli

  def toInstant: Instant = underlying.toInstant

  def toDate: Date = Date.from(underlying.toInstant)

  def toMicros: Long = underlying.micros

  def microsOverSecond(): Long = {
    val nanos = underlying.toInstant.getNano
    assert(nanos % 1000 == 0)
    nanos / 1000L
  }

  override def pretty: Pretty[this.type] = prettyOfParam(_.underlying)
}
