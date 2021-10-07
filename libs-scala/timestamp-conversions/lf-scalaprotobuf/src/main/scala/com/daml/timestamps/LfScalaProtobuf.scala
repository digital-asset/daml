// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.lf.data.Time.{Timestamp => LfTimestamp}
import com.google.protobuf.timestamp.{Timestamp => ProtoTimestamp}

import java.util.concurrent.TimeUnit

object LfScalaProtobuf {
  implicit class LfToScalaProtoTimestampConversions(timestamp: LfTimestamp) {
    def asScalaProto: ProtoTimestamp =
      ProtoTimestamp.of(
        seconds = timestamp.micros / 1000000,
        nanos = ((timestamp.micros % 1000000) * 1000).toInt,
      )
  }

  implicit class ScalaProtoToLfTimestampConversions(timestamp: ProtoTimestamp) {
    def asLf: LfTimestamp = {
      val micros =
        TimeUnit.SECONDS.toMicros(timestamp.seconds) +
          TimeUnit.NANOSECONDS.toMicros(timestamp.nanos.toLong)
      LfTimestamp.assertFromLong(micros)
    }
  }
}
