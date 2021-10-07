// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.timestamps.JavaScala._
import com.daml.timestamps.JavaScalaProtobuf._
import com.google.protobuf.duration.{Duration => ScalaProtoDuration}

import scala.concurrent.duration.{FiniteDuration => ScalaDuration}

object ScalaProtobuf {
  implicit class ScalaToScalaProtoDurationConversions(duration: ScalaDuration) {
    def asScalaProto: ScalaProtoDuration =
      duration.asJava.asScalaProto
  }

  implicit class ScalaProtoToScalaDurationConversions(duration: ScalaProtoDuration) {
    def asScala: ScalaDuration =
      duration.asJava.asScala
  }
}
