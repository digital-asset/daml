// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError, Envelope, SequencedEvent}
import com.digitalasset.canton.sequencing.{
  BoxedEnvelope,
  HandlerResult,
  SubscriptionStart,
  UnsignedEnvelopeBox,
  UnsignedProtocolEventHandler,
}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

/** Maintains a watermark of the sequencing times of all observed envelopes with
  * [[com.digitalasset.canton.sequencing.protocol.AllMembersOfSynchronizer]] as a recipient.
  */
trait BroadcastTimeTracker {
  def lastBroadcastTimestamp: CantonTimestamp
}

class BroadcastTimeTrackerImpl(override protected val loggerFactory: NamedLoggerFactory)
    extends BroadcastTimeTracker
    with UnsignedProtocolEventHandler
    with NamedLogging {

  private val lastBroadcastSequencingTime: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)

  override def lastBroadcastTimestamp: CantonTimestamp = lastBroadcastSequencingTime.get()

  override val name: String = this.getClass.getSimpleName

  override def subscriptionStartsAt(
      start: SubscriptionStart,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    start match {
      case SubscriptionStart.FreshSubscription =>
      case SubscriptionStart.CleanHeadResubscriptionStart(prehead) =>
        advanceTo(prehead)
      case SubscriptionStart.ReplayResubscriptionStart(firstReplayed, _) =>
        advanceTo(firstReplayed)
    }
    FutureUnlessShutdown.unit
  }

  private def advanceTo(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    val newBound = lastBroadcastSequencingTime.updateAndGet(_ max timestamp)
    logger.debug(s"Advanced broadcast time bound to $newBound")
  }

  override def apply(
      box: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
  ): HandlerResult = {
    val lastBroadcastInBatch =
      box.value.findLast(tracedEvent => containsBroadcast(tracedEvent.value))
    lastBroadcastInBatch.foreach { event =>
      implicit val traceContext: TraceContext = event.traceContext
      advanceTo(event.value.timestamp)
    }
    HandlerResult.done
  }

  private def containsBroadcast[Env <: Envelope[?]](event: SequencedEvent[Env]): Boolean =
    event match {
      case deliver: Deliver[?] => deliver.batch.isBroadcast
      case _: DeliverError => false
    }

}
