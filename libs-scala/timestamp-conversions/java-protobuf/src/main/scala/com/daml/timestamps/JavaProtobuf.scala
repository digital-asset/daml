// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.google.protobuf.{Duration => JavaProtoDuration}

import java.time.{Duration => JavaDuration}

object JavaProtobuf {
  implicit class JavaToJavaProtoDurationConversions(duration: JavaDuration) {
    def asJavaProto: JavaProtoDuration =
      JavaProtoDuration.newBuilder.setSeconds(duration.getSeconds).setNanos(duration.getNano).build
  }

  implicit class JavaProtoToJavaDurationConversions(duration: JavaProtoDuration) {
    def asJava: JavaDuration =
      JavaDuration.ofSeconds(duration.getSeconds, duration.getNanos.toLong)
  }
}
