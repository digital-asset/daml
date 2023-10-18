// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

class TimelyRejectNotifierTest extends AnyWordSpec with BaseTest with HasExecutionContext {

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

    "notify only if the timestamp is in the correct relation with the current bound" in {
      val rejecter = mock[TimelyRejectNotifier.TimelyRejecter]
      when(rejecter.notify(any[CantonTimestamp])(anyTraceContext))
        .thenReturn(FutureUnlessShutdown.unit)

      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)
      notifier.notifyIfInPastAsync(CantonTimestamp.MinValue) shouldBe false
      always(50.milliseconds) {
        verify(rejecter, never).notify(eqTo(CantonTimestamp.MinValue))(anyTraceContext)
        ()
      }

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.Epoch)))
      verify(rejecter).notify(eqTo(CantonTimestamp.Epoch))(anyTraceContext)

      notifier.notifyAsync(
        Traced(CursorPrehead(SequencerCounter.MinValue, CantonTimestamp.ofEpochSecond(-2)))
      )
      always(50.milliseconds) {
        verify(rejecter, never).notify(eqTo(CantonTimestamp.ofEpochSecond(-2)))(anyTraceContext)
        ()
      }

      notifier.notifyIfInPastAsync(CantonTimestamp.Epoch) shouldBe true
      verify(rejecter, times(2)).notify(eqTo(CantonTimestamp.Epoch))(anyTraceContext)

      notifier.notifyIfInPastAsync(CantonTimestamp.ofEpochSecond(-1)) shouldBe true
      verify(rejecter).notify(eqTo(CantonTimestamp.ofEpochSecond(-1)))(anyTraceContext)

      notifier.notifyIfInPastAsync(CantonTimestamp.ofEpochSecond(1)) shouldBe false
      always(50.milliseconds) {
        verify(rejecter, never).notify(eqTo(CantonTimestamp.ofEpochSecond(1)))(anyTraceContext)
        ()
      }

      notifier.notifyAsync(
        Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.ofEpochMilli(10)))
      )
      verify(rejecter).notify(eqTo(CantonTimestamp.ofEpochMilli(10)))(anyTraceContext)
    }

    "stop upon AbortedDueToShutdown" in {
      val rejecter = mock[TimelyRejectNotifier.TimelyRejecter]
      when(rejecter.notify(any[CantonTimestamp])(anyTraceContext))
        .thenReturn(FutureUnlessShutdown.abortedDueToShutdown)
      val notifier = new TimelyRejectNotifier(rejecter, None, loggerFactory)

      notifier.notifyAsync(Traced(CursorPrehead(SequencerCounter.Genesis, CantonTimestamp.Epoch)))
      verify(rejecter).notify(eqTo(CantonTimestamp.Epoch))(anyTraceContext)

      notifier.notifyAsync(
        Traced(CursorPrehead(SequencerCounter.MaxValue, CantonTimestamp.MaxValue))
      )
      notifier.notifyIfInPastAsync(CantonTimestamp.MinValue)
      always(50.milliseconds) {
        verify(rejecter, never).notify(eqTo(CantonTimestamp.MaxValue))(anyTraceContext)
        verify(rejecter).notify(eqTo(CantonTimestamp.Epoch))(anyTraceContext)
        ()
      }
    }
  }
}
