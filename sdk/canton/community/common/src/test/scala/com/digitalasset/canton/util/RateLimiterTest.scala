// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveNumeric}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.prop.TableFor2

import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

class RateLimiterTest extends BaseTestWordSpec with HasExecutionContext {

  lazy val testCases: TableFor2[Double, Int] = Table(
    ("maxTasksPerSecond", "initialBurst"),
    (0.5, 0),
    (1, 1),
    (9, 5),
    (10, 1),
    (100, 10),
    (200, 20),
  )

  @tailrec
  private def go(
      limiter: RateLimiter,
      nanotime: AtomicLong,
      deltaNanos: Long,
      total: Int,
      success: Int,
  ): Int = {
    def submitAndReturnOneOnSuccess(limiter: RateLimiter): Int =
      if (limiter.checkAndUpdateRate()) 1 else 0
    if (total == 0) {
      success
    } else {
      nanotime.updateAndGet(_ + deltaNanos).discard
      go(limiter, nanotime, deltaNanos, total - 1, success + submitAndReturnOneOnSuccess(limiter))
    }
  }

  private def deltaNanos(maxTasksPerSecond: Double, factor: Double): Long = {
    ((1.0 / maxTasksPerSecond) * 1e9 * factor).toLong
  }

  private def testRun(
      maxTasksPerSecond: Double,
      initialBurst: Int,
      throttle: Double,
  ): (Int, Int) = {
    val nanotime = new AtomicLong(0)
    val limiter = new RateLimiter(
      NonNegativeNumeric.tryCreate(maxTasksPerSecond),
      PositiveNumeric.tryCreate(Math.max(initialBurst.toDouble / maxTasksPerSecond, 1e-6)),
      nanotime.get(),
    )
    val total = ((100 * maxTasksPerSecond) / throttle).toInt
    val deltaN = deltaNanos(maxTasksPerSecond, throttle)
    val burst = (1 to initialBurst).count(_ => limiter.checkAndUpdateRate())
    burst shouldBe initialBurst
    val res = go(limiter, nanotime, deltaN, total, 0)
    (total, res)
  }

  "A decay rate limiter" when {
    testCases.forEvery { case (maxTasksPerSecond, initialBurst) =>
      s"the maximum rate is $maxTasksPerSecond" must {
        "submission below max will never be rejected" in {
          val (total, res) = testRun(maxTasksPerSecond, initialBurst, 2)
          assert(res == total)
        }

        "submission above max will be throttled to max" in {
          val factor = 0.25
          val (total, res) = testRun(maxTasksPerSecond, initialBurst, factor)
          res.toDouble should be > (total * 0.24)
          res.toDouble should be < (total * 0.26)
        }
      }
    }
  }

  "zero is zero" in {
    val limiter = new RateLimiter(NonNegativeNumeric.tryCreate(0.0), PositiveNumeric.tryCreate(1.0))
    assert(!limiter.checkAndUpdateRate())
  }

}
