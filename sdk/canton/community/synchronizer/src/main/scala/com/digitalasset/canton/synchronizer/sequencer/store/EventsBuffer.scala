// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BytesUnit, ErrorUtil}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.math.Numeric.Implicits.*
import scala.math.Ordering.Implicits.*

/** This in-memory fan-out buffer allows a DatabaseSequencer.single(...) instance to fully bypass
  * reading events back from the database, since there's only a single writer.
  *
  * Multiple readers may request an immutable snapshot of the buffer at any time. We use immutable
  * [[scala.collection.immutable.Vector]] to allow efficient random access to find the reading start
  * point using binary search.
  *
  * The user of this buffer is responsible for providing the events in the right order.
  *
  * The buffer is configured with a memory limit and only holds events up to this limit. The memory
  * used by an event is merely an approximated value and not intended to be very precise, see
  * [[com.digitalasset.canton.synchronizer.sequencer.store.EventsBuffer.approximateEventSize]].
  * There is no direct bound on the number of events that can be buffered, as long as the total sum
  * of the memory used by all events is below the memory limit. This means there could be a few very
  * big events or a lot of small events, or anything in between.
  *
  * There are some invariants/restrictions:
  *   - The [[EventsBuffer]] always keeps at least 1 element, even if the single event exceeds the
  *     memory limit.
  *   - After adding elements to the buffer, old elements are removed from the buffer until the
  *     memory usage drops below the memory limit.
  *   - Only a single process may write to the buffer.
  *
  * @param maxEventsBufferMemory
  *   the maximum amount of memory the buffered events may occupy
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class EventsBuffer(
    maxEventsBufferMemory: BytesUnit,
    override val loggerFactory: NamedLoggerFactory,
    sequencerMetrics: SequencerMetrics,
) extends NamedLogging {

  // This implementation was tested against an implementation with mutable.ArrayDeque and was considered significantly faster
  // (up to 3 orders of magnitudes) when dealing with 100_000 and max memory consumption of ~62MB with payloads of 0 bytes (for
  // testing just the overhead).
  // It becomes even more significant when increasing the order of magnitude by number of elements
  private val eventsBuffer: AtomicReference[Vector[Sequenced[IdOrPayload]]] = new AtomicReference(
    Vector.empty
  )
  private val memoryUsed = new AtomicReference[BytesUnit](BytesUnit(0))

  implicit private val metricsContext: MetricsContext = MetricsContext.Empty

  sequencerMetrics.eventBuffer.registerSizeGauge(() => eventsBuffer.get.size.toLong)
  sequencerMetrics.eventBuffer.registerWeightGauge(() => memoryUsed.get.bytes)

  private def updateTimestampMetrics(): Unit = {
    sequencerMetrics.headTimestamp.updateValue(
      eventsBuffer.get().headOption.map(_.timestamp.toMicros).getOrElse(0L)
    )
    sequencerMetrics.lastTimestamp.updateValue(
      eventsBuffer.get().lastOption.map(_.timestamp.toMicros).getOrElse(0L)
    )
  }

  /** Appends events up to the memory limit to the buffer. May drop already buffered events and may
    * not buffer all provided events to stay within the memory limit.
    */
  final def bufferEvents(
      events: NonEmpty[Seq[Sequenced[IdOrPayload]]]
  )(implicit traceContext: TraceContext): Unit = addElementsInternal(events, append = true).discard

  /** Prepends events to the buffer up to the memory limit. May not buffer all provided events to
    * stay within the memory limits.
    * @return
    *   true if the buffer is at the memory limit or some events had to be dropped again to stay
    *   within the memory limit.
    */
  final def prependEventsForPreloading(events: NonEmpty[Seq[Sequenced[IdOrPayload]]])(implicit
      traceContext: TraceContext
  ): Boolean =
    addElementsInternal(events, append = false)

  private def checkedUpdate(eventsBefore: Vector[Sequenced[IdOrPayload]], memoryBefore: BytesUnit)(
      events: Vector[Sequenced[IdOrPayload]],
      memory: BytesUnit,
  )(implicit traceContext: TraceContext): Unit =
    ErrorUtil.requireState(
      eventsBuffer.compareAndSet(eventsBefore, events) &&
        memoryUsed.compareAndSet(memoryBefore, memory),
      "Concurrent modification of event buffer would lead to out of order events",
    )

  private def addElementsInternal(
      events: NonEmpty[Seq[Sequenced[IdOrPayload]]],
      append: Boolean,
  )(implicit traceContext: TraceContext) = {

    // prepare the buffer so that the backing array is prepared for the right size
    val currentBuffer = eventsBuffer.get()

    val targetSize = currentBuffer.size + events.size
    val bufferWithAddedElements =
      if (append) {
        currentBuffer.appendedAll(events)
      } else {
        currentBuffer.prependedAll(events)
      }
    val currentMemory = memoryUsed.get()
    val newMemory = currentMemory + EventsBuffer.approximateSize(events)

    val memoryToFree = BytesUnit.zero.max(newMemory - maxEventsBufferMemory)
    if (memoryToFree > BytesUnit.zero) {
      // drop all events starting with the head of the deque (while keeping at least 1) until we are at or below the max memory usage
      val indexToSplitAt = bufferWithAddedElements.iterator
        .scanLeft((BytesUnit.zero, 0)) { case ((accumulatedMemoryFromOldest, index), event) =>
          (accumulatedMemoryFromOldest + EventsBuffer.approximateEventSize(event), index + 1)
        }
        .find { case (accumulatedMemoryFromOldest, _) =>
          accumulatedMemoryFromOldest >= memoryToFree
        }

      indexToSplitAt match {
        // if we didn't find an index (which means that the last element exceeds the memory limit),
        // or we would split at the index after the last element,
        // we explicitly retain the last element only so that there's always something to serve
        case None | Some((_, `targetSize`)) =>
          val shrinkedBuffer = bufferWithAddedElements.takeRight(1)
          val shrinkedMemory = EventsBuffer.approximateSize(shrinkedBuffer)
          checkedUpdate(currentBuffer, currentMemory)(shrinkedBuffer, shrinkedMemory)
          sequencerMetrics.eventBuffer.evictionWeight.inc((newMemory - shrinkedMemory).bytes)
          sequencerMetrics.eventBuffer.evictionCount.inc(targetSize.toLong - 1)
          updateTimestampMetrics()
        case Some((memoryFreedUp, indexToSplitAt)) =>
          val (_, bufferBelowMemoryLimit) =
            bufferWithAddedElements.splitAt(indexToSplitAt)
          checkedUpdate(currentBuffer, currentMemory)(
            bufferBelowMemoryLimit,
            newMemory - memoryFreedUp,
          )
          sequencerMetrics.eventBuffer.evictionWeight.inc(memoryFreedUp.bytes)
          sequencerMetrics.eventBuffer.evictionCount.inc(indexToSplitAt.toLong)
          updateTimestampMetrics()
      }
    } else {
      checkedUpdate(currentBuffer, currentMemory)(bufferWithAddedElements, newMemory)
      updateTimestampMetrics()
    }

    // signal that the buffer is at or exceeded the memory limit
    memoryUsed.get() == maxEventsBufferMemory || eventsBuffer.get().sizeIs < targetSize
  }

  /** Empty the buffer for events, i.e. in case of a writer failure
    */
  final def invalidateBuffer()(implicit traceContext: TraceContext): Unit = {
    checkedUpdate(eventsBefore = eventsBuffer.get(), memoryBefore = memoryUsed.get())(
      Vector.empty,
      BytesUnit.zero,
    )
    updateTimestampMetrics()
  }

  final def snapshot(): Vector[Sequenced[IdOrPayload]] = eventsBuffer.get()
}

object EventsBuffer {
  // Node overhead ~32B + 16B bytes for each member
  private val perMemberOverhead = 48L

  // Trace context, timestamp, message id, sender, topology timestamp, traffic receipt
  private val otherFieldsOverheadEstimate = 600L

  @VisibleForTesting
  def approximateEventSize(event: Sequenced[IdOrPayload]): BytesUnit = {
    val payloadSize = event.event.payloadO
      .map {
        case BytesPayload(_, bytes) => bytes.size.toLong
        case PayloadId(_) => 8L // 64-bit Long / CantonTimestamp
      }
      .getOrElse(0L)
    val membersSizeEstimate = event.event.members.size * perMemberOverhead
    BytesUnit(payloadSize + membersSizeEstimate + otherFieldsOverheadEstimate)
  }

  @VisibleForTesting
  def approximateSize(events: Seq[Sequenced[IdOrPayload]]): BytesUnit =
    events.map(approximateEventSize).sum
}
