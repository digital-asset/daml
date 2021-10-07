// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.lf.data.Time.{Timestamp => LfTimestamp}
import com.google.protobuf.{Timestamp => ProtoTimestamp}

import java.util.concurrent.TimeUnit

object LfJavaProtobuf {
  implicit class LfToJavaProtoTimestampConversions(timestamp: LfTimestamp) {
    def asJavaProto: ProtoTimestamp =
      ProtoTimestamp.newBuilder
        .setSeconds(timestamp.micros / 1000000)
        .setNanos(((timestamp.micros % 1000000) * 1000).toInt)
        .build
  }

  implicit class JavaProtoToLfTimestampConversions(timestamp: ProtoTimestamp) {
    def asLf: LfTimestamp = {
      val micros =
        TimeUnit.SECONDS.toMicros(timestamp.getSeconds) +
          TimeUnit.NANOSECONDS.toMicros(timestamp.getNanos.toLong)
      LfTimestamp.assertFromLong(micros)
    }
  }
}
