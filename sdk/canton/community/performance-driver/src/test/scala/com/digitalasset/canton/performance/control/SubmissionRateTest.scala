// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.control

import com.daml.metrics.api.MetricName
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.performance.control.SubmissionRate.{FixedRate, TargetLatency}
import com.google.common.util.concurrent.AtomicDouble
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContext, Promise}

abstract class SubmissionRateTest extends AnyWordSpec with BaseTest {

  protected implicit val ec: ExecutionContext = directExecutionContext

  type SR <: SubmissionRate
  protected def mkSubmissionRate(now: () => CantonTimestamp): SR

  protected val prefix: MetricName = MetricName("testing")
  protected def expectedInitialRate: Double

  protected class Setup {

    val clock = new AtomicReference[CantonTimestamp](CantonTimestamp.Epoch)
    val sr: SR = mkSubmissionRate(() => clock.get())

    def progressTimeMs(ms: Int): Unit = clock.updateAndGet(_.plusMillis(ms.toLong))

    def newSubmission(): Promise[Boolean] = {
      val p = Promise[Boolean]()
      sr.newSubmission(p.future)
      p
    }
  }

  protected def latencyTest(): Setup = {
    val setup = new Setup()
    (1 to 100).foreach { _ =>
      setup.progressTimeMs(10)
      setup.sr.latencyObservation(100)
      assert(setup.sr.latencyMs > 99.0 && setup.sr.latencyMs < 101.0, setup.sr.latencyMs)
    }
    setup
  }

  "submission rate" should {
    "correctly account new submissions" in {
      val setup = new Setup()
      val sr = setup.sr
      sr.currentRate shouldBe expectedInitialRate
      val p = setup.newSubmission()
      sr.pending shouldBe 1
      p.success(true)
      sr.pending shouldBe 0
      sr.succeeded shouldBe 1
      sr.observed shouldBe 0
    }

    "correctly account for failed submissions" in {
      val setup = new Setup()

      setup.newSubmission().success(false)

      assertResult(0)(setup.sr.pending)
      assertResult(1)(setup.sr.failed)
      assertResult(0)(setup.sr.succeeded)
    }
  }

  "latency observation" should {
    "remain constant if latency is constant" in {
      latencyTest()
    }
  }
}

final class TargetLatencyTest extends SubmissionRateTest {
  override type SR = TargetLatency

  // Nothing is going on yet
  override protected def expectedInitialRate: Double = 0.0

  override protected def mkSubmissionRate(now: () => CantonTimestamp): TargetLatency =
    new TargetLatency(
      startMaxRate = 10,
      startTargetLatencyMs = 1000,
      startAdjustFactor = 1.15,
      prefix,
      new InMemoryMetricsFactory,
      loggerFactory,
      now,
    )

  "submission rate" should {
    "correctly compute rates" in {
      val setup = new Setup()
      // scale up rate
      (1 to 100).foreach { ff =>
        setup.newSubmission().success(true)
        assert(setup.sr.currentRate > ff - 1, setup.sr.currentRate)
        assert(setup.sr.currentRate < ff + 1, (setup.sr.currentRate, ff))
      }
      assert(setup.sr.currentRate < 100.01, setup.sr.currentRate) // + epsilon
      // keep steady state
      (1 to 200).foreach { _ =>
        setup.progressTimeMs(100)
        setup.newSubmission().success(true)
        assert(setup.sr.currentRate > 99.9, setup.sr.currentRate)
        assert(setup.sr.currentRate < 101.01, setup.sr.currentRate) // + epsilon
      }
    }
  }

  "max rate computation" should {
    def run(setup: Setup, latency: Long, iterations: Int = 20, delta: Int = 100) = {
      (1 to iterations).foreach { _ =>
        setup.progressTimeMs(delta)
        setup.newSubmission().success(true)
        setup.sr.latencyObservation(latency)
        setup.sr.adjustMaxRate()
      }
      setup
    }

    "adjust up if latency is below threshold" in {
      // quickly reach max rate as otherwise the max rate will be first scaled down (as it is unused)
      val setup = run(new Setup(), latency = 200, iterations = 10, delta = 1)
      run(setup, latency = 200, iterations = 100, delta = 50)
      assert(setup.sr.maxRate > 13)
    }
    "reduce down if latency is above threshold" in {
      val setup = run(new Setup(), 2000, iterations = 100, delta = 500)
      assert(setup.sr.maxRate < 5, setup.sr.maxRate)
    }
    "never reduce to zero" in {
      val setup = run(new Setup(), 2000, 1000)
      assert(setup.sr.maxRate > 0.0, setup.sr.maxRate)
    }
    "not change if latency is at threshold" in {
      val setup = run(new Setup(), 1000)
      assert(setup.sr.maxRate >= 9.9999 && setup.sr.maxRate <= 10.001, setup.sr.maxRate)
    }
    "not change if we aren't exceeding the current rate" in {
      val setup =
        run(new Setup(), latency = 200, iterations = 10, delta = 10)
      run(setup, latency = 200, iterations = 30)
      assert(setup.sr.maxRate >= 9 && setup.sr.maxRate <= 12, setup.sr.maxRate)
    }
  }

  "latency computation" should {
    "converge from one level to another" in {
      val setup = latencyTest() // latency should be at 100ms
      val last = new AtomicDouble(setup.sr.latencyMs)
      (1 to 400).foreach { _ =>
        setup.progressTimeMs(10)
        setup.sr.latencyObservation(200)
        // ensure it converges
        assert(setup.sr.latencyMs > last.get(), (setup.sr.latencyMs, last.get()))
        last.set(setup.sr.latencyMs)
      }
      assert(setup.sr.latencyMs > 198, setup.sr.latencyMs)
    }
  }
}

final class FixedRateTest extends SubmissionRateTest {
  override type SR = FixedRate

  private val rate: Double = 2.5

  override protected def expectedInitialRate: Double = rate

  override protected def mkSubmissionRate(now: () => CantonTimestamp): FixedRate = new FixedRate(
    rate = rate,
    prefix,
    new InMemoryMetricsFactory,
    loggerFactory,
    now,
  )

  "availability computation" should {
    "return correct results" in {
      val setup = new Setup()
      val expectedAvailable = new AtomicInteger(0) // nothing yet

      rate shouldBe 2.5

      setup.sr.available shouldBe expectedAvailable.get()

      setup.progressTimeMs(2000)
      setup.sr.updateRate(setup.clock.get())
      expectedAvailable.set((rate * 2).toInt) // time progressed by 2 seconds
      setup.sr.available shouldBe expectedAvailable.get()

      setup.newSubmission()
      expectedAvailable.decrementAndGet() // one is in flight
      setup.sr.available shouldBe expectedAvailable.get()

      setup.progressTimeMs(1000)
      expectedAvailable.updateAndGet(_ + 2) // rate is rounded down
      setup.sr.updateRate(setup.clock.get())
      setup.sr.available shouldBe expectedAvailable.get()
    }
  }
}
