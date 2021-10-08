// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.scalautil.Statement.discard
import com.daml.timestamps.AsScalaAsJava._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scala.concurrent.duration.{FiniteDuration => ScalaDuration}

import java.time.{Duration => JavaDuration}

final class JavaScalaDurationConversionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "converting a Java duration to a Scala duration" should {
    "convert to and fro" in {
      // The set of Java durations is much larger than the set of Scala durations, so we generate the latter.
      forAll { (duration: ScalaDuration) =>
        whenever(isConvertibleToScala(duration.asJava)) {
          duration.asJava.asScala should be(duration)
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
