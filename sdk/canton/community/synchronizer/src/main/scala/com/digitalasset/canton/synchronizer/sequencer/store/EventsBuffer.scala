// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.util.BytesUnit
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.blocking
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
) extends NamedLogging {

  // This implementation was tested against an implementation with mutable.ArrayDeque and was considered significantly faster
  // (up to 3 orders of magnitudes) when dealing with 100_000 and max memory consumption of ~62MB with payloads of 0 bytes (for
  // testing just the overhead).
  // It becomes even more significant when increasing the order of magnitude by number of elements
  @volatile
  private var eventsBuffer: Vector[Sequenced[BytesPayload]] = Vector.empty
  @volatile
  private var memoryUsed = BytesUnit(0)

  /** Appends events up to the memory limit to the buffer. May drop already buffered events and may
    * not buffer all provided events to stay within the memory limit.
    */
  final def bufferEvents(
      events: NonEmpty[Seq[Sequenced[BytesPayload]]]
  ): Unit = addElementsInternal(events, append = true).discard

  /** Prepends events to the buffer up to the memory limit. May not buffer all provided events to
    * stay within the memory limits.
    * @return
    *   true if the buffer is at the memory limit or some events had to be dropped again to stay
    *   within the memory limit.
    */
  final def prependEventsForPreloading(events: NonEmpty[Seq[Sequenced[BytesPayload]]]): Boolean =
    addElementsInternal(events, append = false)

  private def addElementsInternal(events: NonEmpty[Seq[Sequenced[BytesPayload]]], append: Boolean) =
    blocking(synchronized { // synchronized to enforce that there is only 1 writer

      // prepare the buffer so that the backing array is prepared for the right size
      val targetSize = eventsBuffer.size + events.size
      val bufferWithAddedElements =
        if (append) {
          eventsBuffer.appendedAll(events)
        } else {
          eventsBuffer.prependedAll(events)
        }
      memoryUsed += EventsBuffer.approximateSize(events)

      val memoryToFree = BytesUnit.zero.max(memoryUsed - maxEventsBufferMemory)
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
            eventsBuffer = bufferWithAddedElements.takeRight(1)
            memoryUsed = EventsBuffer.approximateSize(eventsBuffer)
          case Some((memoryFreedUp, indexToSplitAt)) =>
            val (_, bufferBelowMemoryLimit) =
              bufferWithAddedElements.splitAt(indexToSplitAt)
            eventsBuffer = bufferBelowMemoryLimit
            memoryUsed -= memoryFreedUp
        }
      } else {
        eventsBuffer = bufferWithAddedElements
      }

      // signal that the buffer is at or exceeded the memory limit
      memoryUsed == maxEventsBufferMemory || eventsBuffer.sizeIs < targetSize
    })

  /** Empty the buffer for events, i.e. in case of a writer failure
    */
  final def invalidateBuffer(): Unit = blocking(synchronized {
    eventsBuffer = Vector.empty
    memoryUsed = BytesUnit.zero
  })

  final def snapshot(): Vector[Sequenced[BytesPayload]] = eventsBuffer
}

object EventsBuffer {
  // Node overhead ~32B + 16B bytes for each member
  private val perMemberOverhead = 48L

  // Trace context, timestamp, message id, sender, topology timestamp, traffic receipt
  private val otherFieldsOverheadEstimate = 600L

  @VisibleForTesting
  def approximateEventSize(event: Sequenced[BytesPayload]): BytesUnit = {
    val payloadSize = event.event.payloadO.map(_.content.size.toLong).getOrElse(0L)
    val membersSizeEstimate = event.event.members.size * perMemberOverhead
    BytesUnit(payloadSize + membersSizeEstimate + otherFieldsOverheadEstimate)
  }

  @VisibleForTesting
  def approximateSize(events: Seq[Sequenced[BytesPayload]]): BytesUnit =
    events.map(approximateEventSize).sum
}
