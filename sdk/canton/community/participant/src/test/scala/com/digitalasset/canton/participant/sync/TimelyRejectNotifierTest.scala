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

        override def notifyAgain(upToInclusive: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = ???
      }
      val notifier = new TimelyRejectNotifier(rejecter, CantonTimestamp.MinValue, loggerFactory)
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

        override def notifyAgain(upToInclusive: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = ???
      }
      val notifier = new TimelyRejectNotifier(rejecter, CantonTimestamp.MinValue, loggerFactory)
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
      val notifier = new TimelyRejectNotifier(
        rejecter,
        CantonTimestamp.MinValue,
        loggerFactory,
      )

      notifier.notifyIfInPastAsync(CantonTimestamp.MinValue.immediateSuccessor) shouldBe false
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.Epoch)))
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.Epoch -> Notify)
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
        rejecter.invocations shouldBe Seq(CantonTimestamp.Epoch -> NotifyAgain)
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
        rejecter.invocations shouldBe Seq(CantonTimestamp.ofEpochMilli(10) -> Notify)
      }
      rejecter.clearInvocations()
    }

    "repeat notifications in the past" in {
      // Records the order of calls.
      val calls = new AtomicReference[Seq[(CantonTimestamp, NotificationType)]](Seq.empty)
      val cell = new SingleUseCell[TimelyRejectNotifier]

      // Performs three calls to the notifier
      // 1. notifyAsync
      // 2. notifyIfInPastAsync
      // 3. notifyIfInPastAsync
      // Calls 2 and 3 happen while the previous notification is running (calls 1 and 2, resp.)
      // We ensure the interleaving by making the calls 2 and 3 fron the notification handler.

      val rejecter = new TimelyRejectNotifier.TimelyRejecter {
        override def notify(upToInclusive: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = {
          calls.getAndUpdate(_ :+ (upToInclusive -> Notify))
          // Perform call 2
          cell.get.value.notifyIfInPastAsync(upToInclusive)
          FutureUnlessShutdown.unit
        }

        override def notifyAgain(upToInclusive: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = {
          // If this is call 2, then perform call 3, otherwise stop.
          val firstCall = calls.getAndUpdate(_ :+ (upToInclusive -> NotifyAgain)).sizeIs <= 1
          if (firstCall) {
            // Use an earlier timestamp to check that notifier does not decrease the reported bound
            cell.get.value.notifyIfInPastAsync(upToInclusive.immediatePredecessor)
          }
          FutureUnlessShutdown.unit
        }
      }
      val notifier = new TimelyRejectNotifier(rejecter, CantonTimestamp.MinValue, loggerFactory)
      cell.putIfAbsent(notifier)

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.Epoch)))
      eventually() {
        calls.get() shouldBe Seq(
          (CantonTimestamp.Epoch, Notify),
          (CantonTimestamp.Epoch, NotifyAgain),
          (CantonTimestamp.Epoch, NotifyAgain),
        )
      }

    }

    "stop upon AbortedDueToShutdown" in {
      val rejecter = new MockTimelyRejecter(abort = true)
      val notifier = new TimelyRejectNotifier(rejecter, CantonTimestamp.MinValue, loggerFactory)

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.Epoch)))
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.Epoch -> Notify)
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
      val notifier = new TimelyRejectNotifier(rejecter, CantonTimestamp.MinValue, loggerFactory)

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
        new TimelyRejectNotifier(rejecter, CantonTimestamp.ofEpochSecond(1), loggerFactory)

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.Epoch)))
      always(timeout) {
        rejecter.invocations shouldBe Seq.empty
      }

      notifier.notifyIfInPastAsync(CantonTimestamp.ofEpochMilli(1))
      eventually(timeout) {
        rejecter.invocations shouldBe Seq(CantonTimestamp.ofEpochMilli(1) -> NotifyAgain)
      }
      rejecter.clearInvocations()
    }
  }
}

object TimelyRejectNotifierTest {
  private class MockTimelyRejecter(abort: Boolean)(implicit ec: ExecutionContext)
      extends TimelyRejectNotifier.TimelyRejecter {
    private val invocationsRef =
      new AtomicReference[Seq[(CantonTimestamp, NotificationType)]](Seq.empty)

    def invocations: Seq[(CantonTimestamp, NotificationType)] = invocationsRef.get()
    def clearInvocations(): Unit = invocationsRef.set(Seq.empty)

    override def notify(upToInclusive: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown(Future {
      invocationsRef.getAndUpdate(_ :+ (upToInclusive -> Notify))
      if (abort) UnlessShutdown.AbortedDueToShutdown
      else UnlessShutdown.unit
    })

    override def notifyAgain(upToInclusive: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown(Future {
      invocationsRef.getAndUpdate(_ :+ (upToInclusive -> NotifyAgain))
      if (abort) UnlessShutdown.AbortedDueToShutdown
      else UnlessShutdown.unit
    })
  }

  private sealed trait NotificationType extends Product with Serializable
  private case object Notify extends NotificationType
  private case object NotifyAgain extends NotificationType
}
