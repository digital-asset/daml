// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.MetricsUtils
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.synchronizer.metrics.{SequencerHistograms, SequencerMetrics}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.{
  CircuitBreakerByMessageTypeConfig,
  CircuitBreakerConfig,
  IndividualCircuitBreakerConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.SchedulerTestUtil.mockScheduler
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.Traced
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

class BlockSequencerCircuitBreakerTest extends AsyncWordSpec with BaseTest with MetricsUtils {
  private def sequencerMetrics = {
    val histogramInventory = new HistogramInventory()
    val sequencerHistograms = new SequencerHistograms(MetricName.Daml)(histogramInventory)
    val factory = metricsFactory(histogramInventory)
    new SequencerMetrics(
      sequencerHistograms,
      factory,
    )
  }

  private def createConfig(
      allowedBlockDelay: NonNegativeFiniteDuration,
      maxFailures: Int,
      resetTimeout: NonNegativeFiniteDuration,
  ): CircuitBreakerConfig = {
    val individual = createIndividualConfig(allowedBlockDelay, maxFailures, resetTimeout)
    CircuitBreakerConfig(messages =
      CircuitBreakerByMessageTypeConfig(confirmationRequest = individual)
    )
  }

  private def createIndividualConfig(
      allowedBlockDelay: NonNegativeFiniteDuration,
      maxFailures: Int,
      resetTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
  ): IndividualCircuitBreakerConfig =
    IndividualCircuitBreakerConfig(allowedBlockDelay, maxFailures, resetTimeout)

  "BlockSequencerCircuitBreaker" should {
    "reject requests after the block delay is too long more times than the maxFailures" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)
      val breaker =
        new BlockSequencerCircuitBreaker(
          createConfig(
            allowedBlockDelay = NonNegativeFiniteDuration.ofSeconds(10),
            maxFailures = 2,
            resetTimeout = NonNegativeFiniteDuration.ofSeconds(10),
          ),
          clock,
          sequencerMetrics,
          mockScheduler(clock),
          loggerFactory,
        )

      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false

      // the block time is more than the allowedBlockDelay behind now, so this will be considered a failure
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false

      // repeated timestamps are disregarded
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false

      // after failing twice, the circuit breaker opens and requests will be rejected
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true
    }

    "go to half-open state after resetTimeout" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)
      val resetTimeout = 20.seconds
      val breaker = new BlockSequencerCircuitBreaker(
        createConfig(
          allowedBlockDelay = NonNegativeFiniteDuration.ofSeconds(10),
          maxFailures = 2,
          resetTimeout = NonNegativeFiniteDuration.ofSeconds(20),
        ),
        clock,
        sequencerMetrics,
        mockScheduler(clock),
        loggerFactory,
      )

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true

      val now2 = now.add(resetTimeout.toJava)
      clock.advanceTo(now2)
      // circuit breaker is now in half-open state, accepting requests
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false

      // in this state, just one failure is enough to take it back to open
      breaker.registerLastBlockTimestamp(Traced(now2.minusSeconds(20)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true

      val now3 = now2.add(resetTimeout.toJava)
      clock.advanceTo(now3)
      // half-open again
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false

      // now a success will take it to closed state with reset number of failures
      breaker.registerLastBlockTimestamp(Traced(now3))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false
    }

    "be able to configure separate circuit breakers by message type" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)
      val allowedBlockDelay = NonNegativeFiniteDuration.ofSeconds(10)

      val config = CircuitBreakerConfig(messages =
        CircuitBreakerByMessageTypeConfig(
          confirmationRequest = createIndividualConfig(allowedBlockDelay, maxFailures = 2),
          commitment = createIndividualConfig(allowedBlockDelay, maxFailures = 3),
          confirmationResponse = createIndividualConfig(allowedBlockDelay, maxFailures = 4),
        )
      )

      val breaker = new BlockSequencerCircuitBreaker(
        config,
        clock,
        sequencerMetrics,
        mockScheduler(clock),
        loggerFactory,
      )

      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false
      breaker.shouldRejectRequests(SubmissionRequestType.Commitment) shouldBe false
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationResponse) shouldBe false

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true
      breaker.shouldRejectRequests(SubmissionRequestType.Commitment) shouldBe false
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationResponse) shouldBe false

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(18)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true
      breaker.shouldRejectRequests(SubmissionRequestType.Commitment) shouldBe true
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationResponse) shouldBe false

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(17)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true
      breaker.shouldRejectRequests(SubmissionRequestType.Commitment) shouldBe true
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationResponse) shouldBe true
    }

    "allow requests if circuit breaker is disabled" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)

      val breaker = new BlockSequencerCircuitBreaker(
        createConfig(
          allowedBlockDelay = NonNegativeFiniteDuration.ofSeconds(10),
          maxFailures = 2,
          resetTimeout = NonNegativeFiniteDuration.ofSeconds(20),
        ),
        clock,
        sequencerMetrics,
        mockScheduler(clock),
        loggerFactory,
      )

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true

      breaker.disable()
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe false

      breaker.enable()
      breaker.shouldRejectRequests(SubmissionRequestType.ConfirmationRequest) shouldBe true
    }
  }

}
