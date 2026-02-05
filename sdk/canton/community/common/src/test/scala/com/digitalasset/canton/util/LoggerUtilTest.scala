// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTestWordSpec

import scala.concurrent.duration.{DurationInt, DurationLong}

class LoggerUtilTest extends BaseTestWordSpec {

  lazy val testString =
    """I hate bananas
  |I actually do like em
  |But I prefer bockwurst
  |""".stripMargin

  "string truncation" should {
    "not truncate when staying within limits" in {
      LoggerUtil.truncateString(4, 100)(testString) shouldBe testString
    }
    "truncate when seeing too many lines" in {
      LoggerUtil.truncateString(maxLines = 1, maxSize = 100)(
        testString
      ) shouldBe ("I hate bananas\n ...")
    }
    "truncate when seeing too many characters" in {
      LoggerUtil.truncateString(maxLines = 40, maxSize = 25)(
        testString
      ) shouldBe "I hate bananas\nI actually ..."
    }
  }

  "round durations for humans" should {
    "retain the unit if the significant digits are small" in {
      LoggerUtil.roundDurationForHumans(12.seconds) shouldBe "12 seconds"
      LoggerUtil.roundDurationForHumans(1.seconds) shouldBe "1 second"
      LoggerUtil.roundDurationForHumans(123.seconds, keep = 3) shouldBe "123 seconds"
    }

    "go to coarser units without losing precision" in {
      LoggerUtil.roundDurationForHumans(60.minutes) shouldBe "1 hour"
      LoggerUtil.roundDurationForHumans(600.minutes) shouldBe "10 hours"
      LoggerUtil.roundDurationForHumans(60000.minutes, keep = 4) shouldBe "1000 hours"
    }

    "move to coarser units even with precision loss" in {
      LoggerUtil.roundDurationForHumans(1_000_000_001.nanos) shouldBe "1 second"
      LoggerUtil.roundDurationForHumans(
        (15L * 24 * 60 * 60 * 1000 * 1000 * 1000 + 1).nanos,
        keep = 3,
      ) shouldBe "15 days"
    }
  }

}
