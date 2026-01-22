// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.elements.dvp.TraderDriver
import org.scalatest.wordspec.AnyWordSpec

class TraderDriverTest extends AnyWordSpec with BaseTest {

  private def compute(
      freeAssets: Int = 100,
      numOpenApprovals: Int = 10,
      proposalsSubmitted: Int = 10,
      acceptanceSubmitted: Int = 10,
      totalCycles: Int = 1000,
      targetLatencyMs: Int = 5000,
      maxSubmissionPerIterationFactor: Double = 0.5,
      batchSize: Int = 3,
      pending: Int = 10,
      currentRate: Double = 1.0,
      maxRate: Double = 11,
      maxProposalsAheadSeconds: Double = 5,
  ) = {
    val submissionRateSettings =
      SubmissionRateSettings.TargetLatency(targetLatencyMs = targetLatencyMs)

    val available = Math.max(0, Math.round(maxRate - currentRate))
    TraderDriver.computeSubmissions(
      freeAssets,
      numOpenApprovals,
      proposalsSubmitted,
      acceptanceSubmitted,
      totalCycles,
      submissionRateSettings,
      maxSubmissionPerIterationFactor,
      batchSize,
      pending,
      available.toInt,
      maxRate,
      maxProposalsAheadSeconds,
    )
  }

  "submission rates" when {
    "the trader is leading with proposals" should {
      "prefer approvals" in {
        compute(proposalsSubmitted = 50, numOpenApprovals = 30) shouldBe ((0, 15))
      }
      "use excess bandwidth" in {
        compute(proposalsSubmitted = 20, numOpenApprovals = 5) shouldBe ((12, 3))
      }
      "not create more than we have to even if open accepts is 0" in {
        compute(
          proposalsSubmitted = 995,
          acceptanceSubmitted = 990,
          numOpenApprovals = 0,
        ) shouldBe ((5, 0))
      }
      "stop submitting if others are slow" in {
        compute(
          proposalsSubmitted = 100,
          numOpenApprovals = 0,
          maxProposalsAheadSeconds = 1,
          maxRate = 2.0,
        ) shouldBe ((0, 0))
      }

    }
    "the trader is operating on high load" should {
      "stop with many pending" in {
        compute(pending = 60) shouldBe ((0, 0))
      }
      "stop if we are exceeding our rate" in {
        compute(pending = 1, currentRate = 11) shouldBe ((0, 0))
      }
      "throttle with many pending" in {
        compute(pending = 54) shouldBe ((0, 3))
      }
      "throttle on high rates" in {
        compute(currentRate = 10, acceptanceSubmitted = 11) shouldBe ((3, 0))
      }
      "don't die fully" in {
        compute(
          proposalsSubmitted = 11,
          acceptanceSubmitted = 0,
          numOpenApprovals = 0,
          currentRate = 0.3,
          maxRate = 3.0,
          pending = 0,
          totalCycles = 30,
        ) shouldBe ((3, 0))
      }
    }
    "the trader is lagging with proposals" should {
      "prefer proposals" in {
        compute(acceptanceSubmitted = 100) shouldBe ((15, 0))
      }
      "be balanced" in {
        compute(acceptanceSubmitted = 15) shouldBe ((6, 9))
      }
      "not create more than we have to" in {
        compute(
          proposalsSubmitted = 998,
          acceptanceSubmitted = 999,
          numOpenApprovals = 1,
        ) shouldBe ((2, 0))
      }
      "stop batching accepts when we are done" in {
        compute(
          proposalsSubmitted = 1000,
          acceptanceSubmitted = 995,
          numOpenApprovals = 2,
        ) shouldBe ((0, 2))
      }
      "not spend more assets than we have" in {
        compute(freeAssets = 7, acceptanceSubmitted = 20) shouldBe ((3, 3))
      }
      "limit with many pending" in {
        compute(pending = 60, acceptanceSubmitted = 70) shouldBe ((0, 0))
      }

    }
  }

}
