// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import java.time.{Duration => JavaDuration}
import scala.compat.java8.DurationConverters
import scala.concurrent.duration.{FiniteDuration => ScalaDuration}

object JavaScala {
  implicit class JavaToScalaDurationConversions(duration: JavaDuration) {
    def asScala: ScalaDuration = DurationConverters.toScala(duration)
  }

  implicit class ScalaToJavaDurationConversions(duration: ScalaDuration) {
    def asJava: JavaDuration = DurationConverters.toJava(duration)
  }
}
