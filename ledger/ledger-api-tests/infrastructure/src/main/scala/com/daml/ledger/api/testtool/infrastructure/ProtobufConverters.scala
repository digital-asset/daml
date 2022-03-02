// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.google.protobuf

import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object ProtobufConverters {

  implicit class JavaDurationConverter(duration: java.time.Duration) {
    def asProtobuf: protobuf.duration.Duration =
      new protobuf.duration.Duration(duration.getSeconds, duration.getNano)
  }

  implicit class JavaInstantConverter(instant: java.time.Instant) {
    def asProtobuf: protobuf.timestamp.Timestamp =
      new protobuf.timestamp.Timestamp(instant.getEpochSecond, instant.getNano)
  }

  implicit class ScalaDurationConverter(duration: scala.concurrent.duration.FiniteDuration) {
    def asProtobuf: protobuf.duration.Duration =
      duration.toJava.asProtobuf
  }

  implicit class ProtobufDurationConverter(duration: protobuf.duration.Duration) {
    def asJava: java.time.Duration =
      java.time.Duration.ofSeconds(duration.seconds, duration.nanos.toLong)

    def asScala: scala.concurrent.duration.Duration =
      asJava.toScala
  }

  implicit class ProtobufTimestampConverter(timestamp: protobuf.timestamp.Timestamp) {
    def asJava: java.time.Instant =
      java.time.Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong)
  }
}
