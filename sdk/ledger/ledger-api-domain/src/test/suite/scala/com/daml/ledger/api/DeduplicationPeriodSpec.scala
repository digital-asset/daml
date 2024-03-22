// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.lf.data.Time.Timestamp
import java.time.Duration

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class DeduplicationPeriodSpec extends AnyWordSpec with Matchers {
  "calculating deduplication until" should {
    val time = Timestamp.assertFromLong(100 * 1000 * 1000)

    "return expected result when sending duration" in {
      val deduplicateUntil = DeduplicationPeriod.deduplicateUntil(
        time,
        DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(3)),
      )
      deduplicateUntil shouldEqual Success(time.add(Duration.ofSeconds(3)))
    }

    "accept long durations" in {
      noException should be thrownBy DeduplicationPeriod.DeduplicationDuration(
        Duration.ofDays(365 * 10000)
      )
    }

    "accept zero durations" in {
      noException should be thrownBy DeduplicationPeriod.DeduplicationDuration(
        Duration.ZERO
      )
    }

    "not accept negative durations" in {
      an[IllegalArgumentException] should be thrownBy DeduplicationPeriod.DeduplicationDuration(
        Duration.ofSeconds(-1)
      )
    }

    "accept microsecond durations" in {
      noException should be thrownBy DeduplicationPeriod.DeduplicationDuration(
        Duration.ofNanos(1000)
      )
    }

    "not accept nanosecond durations" in {
      an[IllegalArgumentException] should be thrownBy DeduplicationPeriod.DeduplicationDuration(
        Duration.ofNanos(1001)
      )
    }
  }
}
