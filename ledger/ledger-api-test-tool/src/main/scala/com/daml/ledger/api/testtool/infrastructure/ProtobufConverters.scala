package com.daml.ledger.api.testtool.infrastructure

import java.time.{Duration, Instant}

import com.google.protobuf

object ProtobufConverters {
  implicit class JavaDurationConverter(duration: Duration) {
    def asProtobuf: protobuf.duration.Duration =
      new protobuf.duration.Duration(duration.getSeconds, duration.getNano)
  }

  implicit class JavaInstantConverter(instant: Instant) {
    def asProtobuf: protobuf.timestamp.Timestamp =
      new protobuf.timestamp.Timestamp(instant.getEpochSecond, instant.getNano)
  }

  implicit class ProtobufDurationConverter(duration: protobuf.duration.Duration) {
    def asJava: Duration = Duration.ofSeconds(duration.seconds, duration.nanos.toLong)
  }

  implicit class ProtobufTimestampConverter(timestamp: protobuf.timestamp.Timestamp) {
    def asJava: Instant = Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong)
  }
}
