// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BytesUnit, LoggerUtil}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.math.Numeric.Implicits.*
import scala.util.Random

class EventsBufferTest extends AnyWordSpec with BaseTest {

  "EventsBuffer" should {
    "always store at least 1 event" in {
      val buf = new EventsBuffer(BytesUnit(0L), loggerFactory)

      val events = generateEvents(5, BytesUnit(100))
      buf.bufferEvents(events)

      buf.snapshot().loneElement.event.messageId.unwrap shouldBe "5"
    }

    "store all events if the memory limit is not hit" in {
      val events = generateEvents(10, BytesUnit(100L))
      val memoryNeeded = EventsBuffer.approximateSize(events)
      val buf = new EventsBuffer(memoryNeeded * 3, loggerFactory)

      buf.bufferEvents(events)

      buf.snapshot() should contain theSameElementsInOrderAs events
    }

    "drop the oldest elements when new elements get added" in {
      val events = generateEvents(num = 10, payloadSize = BytesUnit(100))
      val memoryNeededPerEvent = EventsBuffer.approximateEventSize(events.head1)
      val buf = new EventsBuffer(memoryNeededPerEvent * 5, loggerFactory)

      buf.bufferEvents(events)
      buf.snapshot() should contain theSameElementsInOrderAs events.takeRight(5)

      // this also works when events get added one by one
      buf.invalidateBuffer()
      events.foreach(e => buf.bufferEvents(NonEmpty(Seq, e)))
      buf.snapshot() should contain theSameElementsInOrderAs events.takeRight(5)
    }

    "always contain at least one event" in {
      val smallEvents = generateEvents(num = 10, payloadSize = BytesUnit(100))
      val singleBigEvent = generateEvents(1, BytesUnit.MB(1), startIndex = 11)
      val memoryNeededForAllSmallEvents = EventsBuffer.approximateSize(smallEvents)
      val buf = new EventsBuffer(memoryNeededForAllSmallEvents, loggerFactory)

      memoryNeededForAllSmallEvents shouldBe <(EventsBuffer.approximateSize(singleBigEvent))

      buf.bufferEvents(smallEvents)
      buf.snapshot() should contain theSameElementsInOrderAs smallEvents

      buf.bufferEvents(singleBigEvent)
      buf.snapshot() should contain theSameElementsInOrderAs singleBigEvent
    }

    "handle a large amount of events with no payload" in {
      def measureAddingEvents(
          buf: EventsBuffer,
          events: NonEmpty[List[Sequenced[BytesPayload]]],
      ): Unit = {
        val start = System.nanoTime()
        buf.bufferEvents(events)
        val elapsed = System.nanoTime() - start
        logger.debug(
          s"${LoggerUtil.roundDurationForHumans(elapsed.nanos)} for buffering ${events.size} events"
        )
      }

      // Test with increasing number of events, equivalent to
      List(1_000 /* ~600 KB*/, 10_000 /* ~6 MB */, 100_000 /* ~60 MB */, 1_000_000 /* ~600 MB */ )
        .foreach { numEvents =>
          val initialEvents = generateEvents(numEvents, BytesUnit.zero, startIndex = 0)
          val memoryRequiredForEvents = EventsBuffer.approximateSize(initialEvents)
          val numNewEvents = 100

          logger.debug(
            s"Starting test for a buffer of $numEvents with max capacity of $memoryRequiredForEvents"
          )
          val buf = new EventsBuffer(memoryRequiredForEvents, loggerFactory)
          measureAddingEvents(buf, initialEvents)
          (0 to 10).foreach { i =>
            val newEvents =
              generateEvents(numNewEvents, BytesUnit.zero, numEvents + i * numNewEvents)
            measureAddingEvents(buf, newEvents)
          }
        }
    }
  }

  private def generateEvents(num: Int, payloadSize: BytesUnit, startIndex: Int = 1) = {
    val events = List.tabulate(num) { i =>
      val index = i + startIndex
      val ts = CantonTimestamp.Epoch.plusSeconds(index.toLong)
      Sequenced(
        ts,
        DeliverStoreEvent(
          sender = SequencerMemberId(0),
          messageId = MessageId(String73.tryCreate(s"$index")),
          members = NonEmpty(SortedSet, SequencerMemberId(1)),
          payload =
            BytesPayload(PayloadId(ts), ByteString.copyFrom(Random.nextBytes(payloadSize.toInt))),
          topologyTimestampO = None,
          traceContext = TraceContext.empty,
          trafficReceiptO = None,
        ),
      )
    }
    NonEmpty.from(events).getOrElse(fail("generated events must not be empty"))
  }
}
