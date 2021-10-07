// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.timestamps.JavaProtobuf._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{Duration => JavaDuration}

final class JavaProtobufDurationConversionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "converting a Java duration to a Protocol Buffers duration" should {
    "convert to and fro" in {
      forAll { (duration: JavaDuration) =>
        duration.asJavaProto.asJava should be(duration)
      }
    }
  }
}
