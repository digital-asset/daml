// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.lf.data.Time.Timestamp

import java.time.Duration
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DeduplicationPeriodSpec extends AnyWordSpec with Matchers {
  "calculating deduplication until" should {
    val time = Timestamp.assertFromLong(100 * 1000 * 1000)

    "return expected result when sending duration" in {
      val deduplicateUntil = DeduplicationPeriod.deduplicateUntil(
        time,
        DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(3)),
      )
      deduplicateUntil shouldEqual Timestamp.assertFromInstant(time.toInstant.plusSeconds(3))
    }

  }
}
