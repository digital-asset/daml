// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.timestamps.AsScalaAsJava._
import com.daml.timestamps.JavaProtobuf._
import com.google.protobuf.{Duration => JavaProtoDuration}

import scala.concurrent.duration.{FiniteDuration => ScalaDuration}

object ScalaJavaProtobuf {
  implicit class ScalaToJavaProtoDurationConversions(duration: ScalaDuration) {
    def asJavaProto: JavaProtoDuration =
      duration.asJava.asJavaProto
  }

  implicit class JavaProtoToScalaDurationConversions(duration: JavaProtoDuration) {
    def asScala: ScalaDuration =
      duration.asJava.asScala
  }
}
