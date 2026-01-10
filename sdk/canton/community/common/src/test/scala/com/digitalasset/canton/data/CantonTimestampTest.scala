// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CantonTimestampTest extends AnyWordSpec with BaseTest {

  "assertFromInstant" should {

    "not fail when the instant must lose precision" in {

      val instantWithNanos = Instant.EPOCH.plusNanos(300L)
      val cantonTimestamp = CantonTimestamp.assertFromInstant(instantWithNanos)
      cantonTimestamp shouldEqual CantonTimestamp.Epoch
    }
  }

  "out of bounds CantonTimestamp" should {

    "throw exception for underflow" in {
      assertThrows[IllegalArgumentException]({
        val tooLow = CantonTimestamp.MinValue.getEpochSecond - 1
        CantonTimestamp.ofEpochSecond(tooLow)
      })
    }

    "throw exception for overflow" in {
      assertThrows[IllegalArgumentException]({
        val tooLarge = CantonTimestamp.MaxValue.getEpochSecond + 1
        CantonTimestamp.ofEpochSecond(tooLarge)
      })
    }
  }

  "fromString and tryFromString" should {
    "return correct result" in {
      val str = "2025-11-27T06:50:55.123Z"
      val t = Instant.parse(str)
      val expectedResult = CantonTimestamp.assertFromInstant(t)

      CantonTimestamp.assertFromString(str) shouldBe expectedResult
      CantonTimestamp.fromString(str).value shouldBe expectedResult
    }

    "compose to identity with toString" in {
      val t = CantonTimestamp.now()
      CantonTimestamp.assertFromString(t.toString) shouldBe t
    }

    "fail on invalid string" in {
      val str = "meh"

      CantonTimestamp.fromString(str).left.value should include(
        s"Unable to parse $str as CantonTimestamp"
      )
      val exception = intercept[IllegalArgumentException](CantonTimestamp.assertFromString(str))
      exception.getMessage should include(s"Unable to parse $str as CantonTimestamp")
    }
  }
}
