// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TraceContextOuterClass;
import com.google.protobuf.StringValue;

import java.time.Duration;
import java.time.Instant;

public class Utils {
  private Utils() {}

  public static com.google.protobuf.Timestamp instantToProto(Instant instant) {
    return com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  public static Instant instantFromProto(com.google.protobuf.Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  public static com.google.protobuf.Duration durationToProto(Duration duration) {
    return com.google.protobuf.Duration.newBuilder()
        .setSeconds(duration.getSeconds())
        .setNanos(duration.getNano())
        .build();
  }

  public static Duration durationFromProto(com.google.protobuf.Duration duration) {
    return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
  }

  public static TraceContextOuterClass.TraceContext newProtoTraceContext(
      String traceParent, String traceState) {
    return TraceContextOuterClass.TraceContext.newBuilder()
        .setTraceparent(StringValue.of(traceParent))
        .setTracestate(StringValue.of(traceState))
        .build();
  }
}
