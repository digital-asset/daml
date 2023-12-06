// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Counter,
  PeanoQueue,
  SynchronizedPeanoTreeQueue,
}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.sequencing.{HandlerResult, PossiblyIgnoredApplicationHandler}
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.{CursorPrehead, SequencerCounterTrackerStore}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.TryUtil.*

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Application handler transformer that tracks the sequencer counters for which the
  * given application handler has successfully completed the asynchronous processing.
  *
  * @param onUpdate Handler to be called after the clean sequencer counter prehead has been updated.
  *                 Calls are synchronized only in the sense that the supplied prehead is at least the persisted prehead
  *                 (except for rewinding during crash recovery), but the observed preheads need not increase
  *                 monotonically from the handler's perspective due to out-of-order execution of futures.
  */
class CleanSequencerCounterTracker(
    store: SequencerCounterTrackerStore,
    onUpdate: Traced[SequencerCounterCursorPrehead] => Unit,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Counter for the batches of events that we hand out to the application handler. */
  private case object EventBatchCounterDiscriminator
  private type EventBatchCounterDiscriminator = EventBatchCounterDiscriminator.type
  private type EventBatchCounter = Counter[EventBatchCounterDiscriminator]

  /** The counter for the next batch of events that goes to the application handler.
    * The [[EventBatchCounter]] is not persisted anywhere and can therefore be reset upon a restart.
    */
  private val eventBatchCounterRef: AtomicLong = new AtomicLong(0L)

  /** A Peano queue to track the event batches that have been processed successfully (both synchronously and asynchronously).
    * The [[SequencerCounter]] belongs to the last event in the corresponding event batch.
    */
  private val eventBatchQueue
      : PeanoQueue[EventBatchCounter, Traced[SequencerCounterCursorPrehead]] =
    new SynchronizedPeanoTreeQueue[
      EventBatchCounterDiscriminator,
      Traced[SequencerCounterCursorPrehead],
    ](Counter[EventBatchCounterDiscriminator](0L))

  def apply[E <: Envelope[_]](
      handler: PossiblyIgnoredApplicationHandler[E]
  )(implicit callerCloseContext: CloseContext): PossiblyIgnoredApplicationHandler[E] =
    handler.replace { tracedEvents =>
      tracedEvents.withTraceContext { implicit batchTraceContext => events =>
        events.lastOption match {
          case None => HandlerResult.done // ignore empty event batches
          case Some(lastEvent) =>
            val lastSc = lastEvent.counter
            val lastTs = lastEvent.timestamp
            val eventBatchCounter = allocateEventBatchCounter()
            handler(tracedEvents).map { asyncF =>
              val asyncFSignalled = asyncF.andThenF { case () =>
                store.performUnlessClosingF("signal-clean-event-batch")(
                  signalCleanEventBatch(eventBatchCounter, lastSc, lastTs)
                )
              }
              asyncFSignalled
            }
        }
      }
    }

  private[this] def allocateEventBatchCounter(): EventBatchCounter =
    Counter[EventBatchCounterDiscriminator](eventBatchCounterRef.getAndIncrement())

  private[this] def signalCleanEventBatch(
      eventBatchCounter: EventBatchCounter,
      lastSc: SequencerCounter,
      lastTs: CantonTimestamp,
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] = {
    val atLeastHead =
      eventBatchQueue.insert(eventBatchCounter, Traced(CursorPrehead(lastSc, lastTs)))
    if (!atLeastHead) {
      logger.debug(s"Ignoring event batch counter $eventBatchCounter")
    }
    // Update the store if we can advance the cursor
    drainAndUpdate()
  }

  private[this] def drainAndUpdate()(implicit callerCloseContext: CloseContext): Future[Unit] =
    eventBatchQueue.dropUntilFront() match {
      case None => Future.unit
      case Some((_, tracedPrehead)) =>
        tracedPrehead.withTraceContext { implicit traceContext => prehead =>
          store.advancePreheadSequencerCounterTo(prehead).map { _ =>
            // Signal the new prehead and make sure that the update handler cannot interfere by throwing exceptions
            Try(onUpdate(tracedPrehead)).forFailed { ex =>
              logger.error("onUpdate handler failed", ex)
            }
          }
        }
    }
}
