// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.chaining.scalaUtilChainingOps

/** Buffer to hold Ledger API Indexer events for a particular connected synchronizer's RecordOrderPublisher
  * for the duration of Online Party Replication (OPR) while OPR-replicated ACS contracts are published
  * as they arrive at the target participant.
  *
  * Note: All the public methods of this class are not thread-safe and must only be called from the RecordOrderPublisher
  * TaskScheduler such that only a single method is executed at a time.
  */
private[event] final class EventBuffer(
    recordTimeBufferBegin: CantonTimestamp,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  // Buffered Ledger API indexer updates/events held back for the duration of Online Party Replication
  private val bufferedEvents: ConcurrentLinkedQueue[Update] = new ConcurrentLinkedQueue[Update]()

  // Track the last buffered update's record time so that we can ensure that events are buffered in non-decreasing
  // record time order.
  private val lastBufferedRecordTime = new AtomicReference[CantonTimestamp](recordTimeBufferBegin)

  /** Buffer/hold back a concurrent Ledger API indexer update/event during OPR such that OPR-replicated ACS contracts
    * can be published "before" with respect to record time order.
    */
  def bufferEvent(event: Update)(implicit traceContext: TraceContext): Unit = {
    ErrorUtil.requireState(
      event.recordTime >= lastBufferedRecordTime.get(),
      "Events must not be buffered out of record time order",
    )
    bufferedEvents.add(event).discard
  }

  /** Mark the OPR ACS chunk event with the buffer begin timestamp as record time.
    */
  def markEventWithRecordTime(eventWithRecordTime: CantonTimestamp => Update)(implicit
      traceContext: TraceContext
  ): Update = {
    val queueRecordTime = recordTimeBufferBegin
    logger.debug(
      s"Marking replicated contracts indexer event with record time ${queueRecordTime.toMicros}"
    )
    eventWithRecordTime(queueRecordTime)
  }

  /** Extracts and clear the buffered events. This is meant to be called before closing the buffer.
    */
  def extractAndClearBufferedEvents(): Seq[Update] =
    bufferedEvents.iterator().asScala.toSeq.tap(_ => bufferedEvents.clear())
}
