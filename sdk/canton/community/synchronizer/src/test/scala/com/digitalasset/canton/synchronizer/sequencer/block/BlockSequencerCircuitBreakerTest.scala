// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.MetricsUtils
import com.digitalasset.canton.synchronizer.metrics.{SequencerHistograms, SequencerMetrics}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerCircuitBreakerTest.scheduler
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.tracing.Traced
import org.apache.pekko.actor.{Cancellable, Scheduler}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, *}
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

  "BlockSequencerCircuitBreaker" should {
    "reject requests after the block delay is too long more times than the maxFailures" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)
      val breaker = new BlockSequencerCircuitBreaker(
        allowedBlockDelay = 10.seconds,
        maxFailures = 2,
        resetTimeout = 10.seconds,
        exponentialBackoffFactor = 1.0,
        maxResetTimeout = 36500.days,
        clock,
        sequencerMetrics,
        scheduler(clock),
        loggerFactory,
      )

      breaker.shouldRejectRequests shouldBe false

      // the block time is more than the allowedBlockDelay behind now, so this will be considered a failure
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.shouldRejectRequests shouldBe false

      // repeated timestamps are disregarded
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.shouldRejectRequests shouldBe false

      // after failing twice, the circuit breaker opens and requests will be rejected
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests shouldBe true
    }

    "go to half-open state after resetTimeout" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)
      val resetTimeout = 20.seconds
      val breaker = new BlockSequencerCircuitBreaker(
        allowedBlockDelay = 10.seconds,
        maxFailures = 2,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = 1.0,
        maxResetTimeout = 36500.days,
        clock,
        sequencerMetrics,
        scheduler(clock),
        loggerFactory,
      )

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests shouldBe true

      val now2 = now.add(resetTimeout.toJava)
      clock.advanceTo(now2)
      // circuit breaker is now in half-open state, accepting requests
      breaker.shouldRejectRequests shouldBe false

      // in this state, just one failure is enough to take it back to open
      breaker.registerLastBlockTimestamp(Traced(now2.minusSeconds(20)))
      breaker.shouldRejectRequests shouldBe true

      val now3 = now2.add(resetTimeout.toJava)
      clock.advanceTo(now3)
      // half-open again
      breaker.shouldRejectRequests shouldBe false

      // now a success will take it to closed state with reset number of failures
      breaker.registerLastBlockTimestamp(Traced(now3))
      breaker.shouldRejectRequests shouldBe false
    }

    "allow requests if circuit breaker is disabled" in {
      val now = CantonTimestamp.Epoch
      val clock = new SimClock(start = now, loggerFactory = loggerFactory)
      val resetTimeout = 20.seconds
      val breaker = new BlockSequencerCircuitBreaker(
        allowedBlockDelay = 10.seconds,
        maxFailures = 2,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = 1.0,
        maxResetTimeout = 36500.days,
        clock,
        sequencerMetrics,
        scheduler(clock),
        loggerFactory,
      )

      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(20)))
      breaker.registerLastBlockTimestamp(Traced(now.minusSeconds(19)))
      breaker.shouldRejectRequests shouldBe true

      breaker.disable()
      breaker.shouldRejectRequests shouldBe false

      breaker.enable()
      breaker.shouldRejectRequests shouldBe true
    }
  }

}

object BlockSequencerCircuitBreakerTest {
  def scheduler(clock: Clock) =
    new Scheduler {

      override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit
          executor: ExecutionContext
      ): Cancellable = {
        val cancelled = new AtomicBoolean(false)
        val cancellable = new Cancellable {
          override def cancel(): Boolean = cancelled.compareAndSet(false, true)
          override def isCancelled: Boolean = cancelled.get()
        }
        val _ = clock.scheduleAfter(
          _ => if (!cancelled.get()) runnable.run(),
          delay.toJava,
        )
        cancellable
      }

      override def schedule(
          initialDelay: FiniteDuration,
          interval: FiniteDuration,
          runnable: Runnable,
      )(implicit executor: ExecutionContext): Cancellable = ???
      override def maxFrequency: Double = 42
    }

}
