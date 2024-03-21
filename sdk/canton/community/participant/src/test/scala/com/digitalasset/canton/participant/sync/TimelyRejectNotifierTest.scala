// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class TimelyRejectNotifierTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  import TimelyRejectNotifierTest.*

  "TimelyRejectNotifier.notifyAsync" should {
    "notify sequentially" in {
      // Records the order of calls and returns. The Boolean marks calls.
      val callsAndReturns = new AtomicReference[Seq[(CantonTimestamp, Boolean)]](Seq.empty)
      val cell = new SingleUseCell[TimelyRejectNotifier]

      val rejecter = new TimelyRejectNotifier.TimelyRejecter {
        override def notify(upToInclusive: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = {
          val firstNotification = callsAndReturns.getAndUpdate(_ :+ (upToInclusive -> true)).isEmpty
          if (firstNotification) {
            cell.get.value.notifyAsync(
              Traced(CursorPrehead(SequencerCounter.Genesis, upToInclusive.immediateSuccessor))
            )
          }
          callsAndReturns.getAndUpdate(_ :+ (upToInclusive -> false))
          FutureUnlessShutdown.unit
        }
      }
      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)
      cell.putIfAbsent(notifier)
      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.Epoch)))
      eventually() {
        callsAndReturns.get() shouldBe Seq(
          (CantonTimestamp.Epoch, true),
          (CantonTimestamp.Epoch, false),
          (CantonTimestamp.Epoch.immediateSuccessor, true),
          (
            CantonTimestamp.Epoch.immediateSuccessor,
            false,
          ),
        )
      }
    }

    "conflate concurrent calls" in {
      val calls = new AtomicReference[Seq[CantonTimestamp]](Seq.empty)
      val cell = new SingleUseCell[TimelyRejectNotifier]

      val rejecter = new TimelyRejectNotifier.TimelyRejecter {
        override def notify(upToInclusive: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = {
          val length = calls.getAndUpdate(_ :+ upToInclusive).size
          if (length == 0) {
            cell.get.value.notifyAsync(
              Traced(CursorPrehead(SequencerCounter.Genesis, upToInclusive.immediateSuccessor))
            )
            cell.get.value.notifyAsync(
              Traced(CursorPrehead(SequencerCounter.MaxValue, upToInclusive.plusSeconds(1)))
            )
          }
          FutureUnlessShutdown.unit
        }
      }
      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)
      cell.putIfAbsent(notifier)
      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.Epoch)))
      eventually() {
        calls.get() shouldBe Seq(
          CantonTimestamp.Epoch,
          CantonTimestamp.Epoch.plusSeconds(1),
        )
      }
    }

    val timeout = 50.milliseconds

    "notify only if the timestamp is in the correct relation with the current bound" in {
      val rejecter = new MockTimelyRejecter(abort = false)
      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)

      notifier.notifyIfInPastAsync(CantonTimestamp.MinValue) shouldBe false
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.Epoch)))
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.Epoch)
      }
      rejecter.clearInvocations()

      notifier.notifyAsync(
        Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.ofEpochSecond(-2)))
      )
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }

      notifier.notifyIfInPastAsync(CantonTimestamp.Epoch) shouldBe true
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.Epoch)
      }
      rejecter.clearInvocations()

      notifier.notifyIfInPastAsync(CantonTimestamp.ofEpochSecond(1)) shouldBe false
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }

      notifier.notifyAsync(
        Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.ofEpochMilli(10)))
      )
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.ofEpochMilli(10))
      }
      rejecter.clearInvocations()
    }

    "stop upon AbortedDueToShutdown" in {
      val rejecter = new MockTimelyRejecter(abort = true)
      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.Epoch)))
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.Epoch)
      }
      rejecter.clearInvocations()

      notifier.notifyAsync(
        Traced(CursorPrehead(SequencerCounter.MaxValue, CantonTimestamp.MaxValue))
      )
      notifier.notifyIfInPastAsync(CantonTimestamp.MinValue)
      eventually(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }
      rejecter.clearInvocations()
    }

    "deal with a lot of concurrent aborts" in {
      val rejecter = new MockTimelyRejecter(abort = true)
      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)

      for (i <- 1 to 100) {
        notifier.notifyAsync(
          Traced(CursorPrehead(SequencerCounter(i), CantonTimestamp.ofEpochSecond(i.toLong)))
        )
      }
      eventually(timeout) {
        rejecter.invocations should have size 1
      }
      always(timeout) {
        rejecter.invocations should have size 1
      }
    }

    "take initial bound into account" in {
      val rejecter = new MockTimelyRejecter(abort = false)
      val notifier =
        new TimelyRejectNotifier(rejecter, Some(CantonTimestamp.ofEpochSecond(1)), loggerFactory)

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.Epoch)))
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }

      notifier.notifyIfInPastAsync(CantonTimestamp.ofEpochMilli(1))
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.ofEpochMilli(1))
      }
      rejecter.clearInvocations()
    }
  }
}

object TimelyRejectNotifierTest {
  class MockTimelyRejecter(abort: Boolean)(implicit ec: ExecutionContext)
      extends TimelyRejectNotifier.TimelyRejecter {
    private val invocationsRef = new AtomicReference[Seq[CantonTimestamp]](Seq.empty)

    def invocations: Seq[CantonTimestamp] = invocationsRef.get()
    def clearInvocations(): Unit = invocationsRef.set(Seq.empty)

    override def notify(upToInclusive: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown(Future {
      invocationsRef.getAndUpdate(_ :+ upToInclusive)
      if (abort) UnlessShutdown.AbortedDueToShutdown
      else UnlessShutdown.unit
    })
  }
}
