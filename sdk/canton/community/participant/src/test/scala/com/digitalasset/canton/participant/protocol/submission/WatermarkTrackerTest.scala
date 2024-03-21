// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class WatermarkTrackerTest extends AsyncWordSpec with BaseTest {

  def mk(): WatermarkTracker[CantonTimestamp] =
    new WatermarkTracker[CantonTimestamp](
      CantonTimestamp.MinValue,
      loggerFactory,
      FutureSupervisor.Noop,
    )

  "highWatermark" should {
    "return the initial watermark on an empty tracker" in {
      val tracker = mk()
      tracker.highWatermark shouldBe CantonTimestamp.MinValue
    }
  }

  "runIfAboveWatermark" should {
    "succeed on an empty tracker" in {
      val tracker = mk()
      for {
        () <- tracker
          .runIfAboveWatermark(CantonTimestamp.Epoch, Future.unit)
          .valueOrFail("running failed")
      } yield succeed
    }

    "fail if starting at or below the initial watermark" in {
      val tracker = mk()
      val error = leftOrFail(tracker.runIfAboveWatermark(CantonTimestamp.MinValue, Future.unit))(
        "running succeeded"
      )
      error shouldBe WatermarkTracker.MarkTooLow(CantonTimestamp.MinValue)
    }

    "support concurrent running for the same mark" in {
      val tracker = mk()
      for {
        () <- tracker
          .runIfAboveWatermark(
            CantonTimestamp.Epoch,
            tracker
              .runIfAboveWatermark(CantonTimestamp.Epoch, Future.unit)
              .valueOrFail("second run failed"),
          )
          .valueOrFail("first run failed")
      } yield succeed
    }

    "support concurrent running for different marks" in {
      val tracker = mk()
      for {
        () <- tracker
          .runIfAboveWatermark(
            CantonTimestamp.Epoch,
            tracker
              .runIfAboveWatermark(CantonTimestamp.Epoch.plusSeconds(1), Future.unit)
              .valueOrFail("second run failed"),
          )
          .valueOrFail("first run failed")
      } yield succeed
    }

    "support concurrent running for reversed marks" in {
      val tracker = mk()
      for {
        () <- tracker
          .runIfAboveWatermark(
            CantonTimestamp.Epoch,
            tracker
              .runIfAboveWatermark(CantonTimestamp.Epoch.minusSeconds(1), Future.unit)
              .valueOrFail("second run failed"),
          )
          .valueOrFail("first run failed")
      } yield succeed
    }

    "fail if watermark was raised before" in {
      val tracker = mk()
      val ts = CantonTimestamp.ofEpochSecond(1)
      for {
        () <- tracker.increaseWatermark(ts)
        watermark = tracker.highWatermark
        error0 = leftOrFail(tracker.runIfAboveWatermark(CantonTimestamp.Epoch, Future.unit))(
          "run below watermark"
        )
        error1 = leftOrFail(tracker.runIfAboveWatermark(ts, Future.unit))("run at watermark")
      } yield {
        watermark shouldBe ts
        error0 shouldBe WatermarkTracker.MarkTooLow(ts)
        error1 shouldBe WatermarkTracker.MarkTooLow(ts)
      }
    }

    "record finishing if the task fails" in {
      val tracker = mk()
      val ex = new RuntimeException("RUN FAILURE")
      for {
        error <- tracker
          .runIfAboveWatermark(CantonTimestamp.Epoch, Future.failed(ex))
          .valueOrFail("run failed")
          .failed
        obs = tracker.increaseWatermark(CantonTimestamp.Epoch)
        () <- obs // should have been completed immediately
      } yield {
        error shouldBe ex
      }
    }

    "interleave running and raising" in {
      val tracker = mk()
      for {
        () <- tracker
          .runIfAboveWatermark(
            CantonTimestamp.ofEpochSecond(2),
            tracker.increaseWatermark(CantonTimestamp.Epoch),
          )
          .valueOrFail("running failed")
      } yield {
        tracker.highWatermark shouldBe CantonTimestamp.Epoch
      }
    }
  }

  "increaseWatermark" should {

    "block until running tasks are done" in {
      val tracker = mk()
      val ts = CantonTimestamp.Epoch
      val () = tracker.registerBegin(ts).valueOrFail("begin failed")
      val obs1 = tracker.increaseWatermark(ts.plusSeconds(1))
      tracker.highWatermark shouldBe ts.plusSeconds(1)
      val obs0 = tracker.increaseWatermark(ts)
      obs0.isCompleted shouldBe false
      obs1.isCompleted shouldBe false
      tracker.highWatermark shouldBe ts.plusSeconds(1)
      val () = tracker.registerEnd(ts)
      for {
        () <- obs0
        () <- obs1
      } yield succeed
    }

    "unblock incrementally" in {
      val tracker = mk()
      val ts = CantonTimestamp.ofEpochSecond(10)
      val () = tracker.registerBegin(ts).valueOrFail("begin 1 failed")
      val () = tracker.registerBegin(ts).valueOrFail("begin 2 failed")
      val () = tracker.registerBegin(ts.plusSeconds(2)).valueOrFail("begin 3 failed")
      val obs0 = tracker.increaseWatermark(ts)
      tracker.highWatermark shouldBe ts
      val obs1 = tracker.increaseWatermark(ts.plusSeconds(1))
      tracker.highWatermark shouldBe ts.plusSeconds(1)
      val obs1a = tracker.increaseWatermark(
        ts.plusSeconds(1)
      ) // We can increase several times to the same mark
      val obs2 = tracker.increaseWatermark(ts.plusSeconds(2))
      tracker.highWatermark shouldBe ts.plusSeconds(2)
      obs0.isCompleted shouldBe false
      obs1.isCompleted shouldBe false
      obs1a.isCompleted shouldBe false
      val () = tracker.registerEnd(ts)
      obs0.isCompleted shouldBe false
      obs1.isCompleted shouldBe false
      obs1a.isCompleted shouldBe false
      val () = tracker.registerEnd(ts)
      for {
        () <- obs0
        () <- obs1
        () <- obs1a
        _ = obs2.isCompleted shouldBe false
        () = tracker.registerEnd(ts.plusSeconds(2))
        () <- obs2
      } yield succeed
    }

    "block further tasks with low marks" in {
      val tracker = mk()

      val ts = CantonTimestamp.ofEpochSecond(100)
      val ts1 = ts.plusSeconds(1)
      val () = tracker.registerBegin(ts).valueOrFail("begin 1 failed")
      val obs1 = tracker.increaseWatermark(ts1)
      val error = leftOrFail(tracker.registerBegin(ts))("begin 2 succeeded")
      error shouldBe WatermarkTracker.MarkTooLow(ts1)
      val () = tracker.registerEnd(ts)
      for {
        () <- obs1
      } yield succeed
    }
  }
}
