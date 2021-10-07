// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.google.protobuf.duration.{Duration => ScalaProtoDuration}

import java.time.{Duration => JavaDuration}

object JavaScalaProtobuf {
  implicit class JavaToProtoDurationConversions(duration: JavaDuration) {
    def asScalaProto: ScalaProtoDuration =
      ScalaProtoDuration.of(duration.getSeconds, duration.getNano)
  }

  implicit class ScalaProtoToJavaDurationConversions(duration: ScalaProtoDuration) {
    def asJava: JavaDuration =
      duration.asJavaDuration
  }
}
