// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class RefinedDurationsTest extends AnyWordSpec with BaseTest {
  lazy val zero = Duration.ZERO
  lazy val oneSec = Duration.ofSeconds(1)

  "NonNegativeFiniteDuration" should {
    "have create method" in {
      Seq(zero, oneSec, Duration.ofMillis(1)).foreach { d =>
        NonNegativeFiniteDuration.create(d).value.unwrap shouldBe d
      }

      NonNegativeFiniteDuration.create(Duration.ofSeconds(-1)).left.value shouldBe a[String]
    }
  }

  "NonNegativeSeconds" should {
    "have create method" in {
      Seq(zero, oneSec).foreach { d =>
        NonNegativeFiniteDuration.create(d).value.unwrap shouldBe d
      }

      NonNegativeSeconds.create(Duration.ofSeconds(-1)).left.value shouldBe a[String]
      NonNegativeSeconds.create(Duration.ofMillis(1)).left.value shouldBe a[String]
    }
  }

  "PositiveSeconds" should {
    "have create method" in {
      NonNegativeFiniteDuration.create(oneSec).value.unwrap shouldBe oneSec

      PositiveSeconds.create(Duration.ofSeconds(-1)).left.value shouldBe a[String]
      PositiveSeconds.create(zero).left.value shouldBe a[String]
      PositiveSeconds.create(Duration.ofMillis(1)).left.value shouldBe a[String]
    }
  }
}
