// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.timestamps.JavaScalaProtobuf._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{Duration => JavaDuration}

final class JavaScalaProtobufDurationConversionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "converting a Java duration to a Scala Protocol Buffers duration" should {
    "convert to and fro" in {
      forAll { (duration: JavaDuration) =>
        duration.asScalaProto.asJava should be(duration)
      }
    }
  }
}
