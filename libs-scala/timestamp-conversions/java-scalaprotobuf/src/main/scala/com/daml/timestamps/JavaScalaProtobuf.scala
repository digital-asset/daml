// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.google.protobuf.duration.{Duration => ScalaProtoDuration}
import com.google.protobuf.timestamp.{Timestamp => ScalaProtoTimestamp}

import java.time.{Duration => JavaDuration, Instant => JavaTimestamp}

object JavaScalaProtobuf {
  implicit class JavaToProtoDurationConversions(duration: JavaDuration) {
    def asScalaProto: ScalaProtoDuration =
      ScalaProtoDuration.of(duration.getSeconds, duration.getNano)
  }

  implicit class ScalaProtoToJavaDurationConversions(duration: ScalaProtoDuration) {
    def asJava: JavaDuration =
      duration.asJavaDuration
  }

  implicit class JavaToProtoTimestampConversions(timestamp: JavaTimestamp) {
    def asScalaProto: ScalaProtoTimestamp =
      ScalaProtoTimestamp.of(timestamp.getEpochSecond, timestamp.getNano)
  }

  implicit class ScalaProtoToJavaTimestampConversions(timestamp: ScalaProtoTimestamp) {
    def asJava: JavaTimestamp =
      timestamp.asJavaInstant
  }
}
