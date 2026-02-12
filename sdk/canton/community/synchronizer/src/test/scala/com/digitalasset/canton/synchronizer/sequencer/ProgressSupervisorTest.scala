// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{BaseTest, HasExecutorService, config}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.*

class ProgressSupervisorTest extends AsyncWordSpec with BaseTest with HasExecutorService {

  def make(): (FutureSupervisor.Impl, ProgressSupervisor, AtomicInteger) = {
    val futureSupervisor =
      new FutureSupervisor.Impl(config.NonNegativeDuration.ofMillis(100), loggerFactory)(
        scheduledExecutor()
      )
    val warnActionCount = new AtomicInteger(0)
    val progressSupervisor = new ProgressSupervisor(
      logLevel = org.slf4j.event.Level.INFO,
      logAfter = 500.millis,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
      warnAction = warnActionCount.incrementAndGet(),
    )
    (futureSupervisor, progressSupervisor, warnActionCount)
  }

  "ProgressSupervisor" should {
    "detect arm called without subsequent disarm" in {
      val (futureSupervisor, progressSupervisor, warnActionCounter) = make()
      progressSupervisor.arm(CantonTimestamp.Epoch)
      Threading.sleep(2000)
      val warnCount = warnActionCounter.get()
      progressSupervisor.disarm(CantonTimestamp.Epoch)
      futureSupervisor.stop()
      warnCount should be > 0
    }

    "not trigger when both arm and disarm us called" in {
      val (futureSupervisor, progressSupervisor, warnActionCounter) = make()
      progressSupervisor.arm(CantonTimestamp.Epoch)
      Threading.sleep(200)
      progressSupervisor.disarm(CantonTimestamp.Epoch)
      Threading.sleep(2000)
      val warnCount = warnActionCounter.get()
      progressSupervisor.disarm(CantonTimestamp.Epoch)
      futureSupervisor.stop()
      warnCount shouldBe 0
    }

    "allow disarm before arm" in {
      val (futureSupervisor, progressSupervisor, warnActionCounter) = make()
      progressSupervisor.disarm(CantonTimestamp.Epoch)
      Threading.sleep(200)
      progressSupervisor.arm(CantonTimestamp.Epoch)
      Threading.sleep(2000)
      val warnCount = warnActionCounter.get()
      progressSupervisor.disarm(CantonTimestamp.Epoch)
      futureSupervisor.stop()
      warnCount shouldBe 0
    }

    "work with a high concurrecy" in {
      val (futureSupervisor, progressSupervisor, warnActionCounter) = make()
      val n = 1000
      val timestamps = (1 to n).map(i => CantonTimestamp.Epoch.plusMillis(i.toLong))
      def armsF = Future {
        timestamps.foreach(ts => progressSupervisor.arm(ts))
      }
      def disarmsF = Future {
        timestamps.foreach { ts =>
          progressSupervisor.disarm(ts)
        }
      }
      val arms = armsF
      val disarms = disarmsF
      for {
        _ <- arms
        _ <- disarms
      } yield {
        val warnCount = warnActionCounter.get()
        futureSupervisor.stop()
        warnCount shouldBe 0
      }
    }

    "not trigger below the lower bound" in {
      val (futureSupervisor, progressSupervisor, warnActionCounter) = make()
      progressSupervisor.arm(CantonTimestamp.Epoch)
      // this should cancel the previous arm/disarm calls and prevent any new ones below the bound
      progressSupervisor.ignoreTimestampsBefore(CantonTimestamp.Epoch.plusMillis(1))
      progressSupervisor.disarm(CantonTimestamp.Epoch.plusMillis(1))
      Threading.sleep(2000)
      progressSupervisor.disarm(CantonTimestamp.Epoch)
      val warnCount = warnActionCounter.get()
      futureSupervisor.stop()
      warnCount shouldBe 0
    }
  }
}
