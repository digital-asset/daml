// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.scalautil.Statement.discard
import com.daml.timestamps.AsScalaAsJava._
import com.daml.timestamps.ScalaJavaProtobuf._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{Duration => JavaDuration}
import scala.concurrent.duration.{FiniteDuration => ScalaDuration}

final class ScalaJavaProtobufDurationConversionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "converting a Scala duration to a Java Protocol Buffers duration" should {
    "convert to and fro" in {
      forAll { (duration: ScalaDuration) =>
        whenever(isConvertibleToScala(duration.asJava)) {
          duration.asJavaProto.asScala should be(duration)
        }
      }
    }
  }

  private def isConvertibleToScala(duration: JavaDuration) =
    try {
      discard(duration.asScala)
      true
    } catch {
      case _: IllegalArgumentException => false
    }
}
