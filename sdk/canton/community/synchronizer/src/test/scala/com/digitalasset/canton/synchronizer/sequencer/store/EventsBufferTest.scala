// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.CacheMetrics
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BytesUnit, LoggerUtil}
import com.google.protobuf.ByteString
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.math.Numeric.Implicits.*
import scala.util.Random

class EventsBufferTest extends FixtureAnyWordSpec with BaseTest {

  "EventsBuffer" should {
    "always store at least 1 event" in { env =>
      import env.*

      val buf = new EventsBuffer(BytesUnit(0L), loggerFactory, cacheMetrics)
      val events = generateEvents(5, BytesUnit(100))
      buf.bufferEvents(events)

      buf.snapshot().loneElement.event.messageId.unwrap shouldBe "5"

      val notFittingEventsSize = EventsBuffer.approximateSize(events.dropRight(1))

      metricCounterValue("evictions") shouldBe 4L
      metricCounterValue("evicted_weight") shouldBe notFittingEventsSize.bytes
    }

    "store all events if the memory limit is not hit" in { env =>
      import env.*

      val events = generateEvents(10, BytesUnit(100L))
      val eventsSize = EventsBuffer.approximateSize(events)
      val memoryNeeded = EventsBuffer.approximateSize(events)
      val buf = new EventsBuffer(memoryNeeded * 3, loggerFactory, cacheMetrics)

      buf.bufferEvents(events)

      buf.snapshot() should contain theSameElementsInOrderAs events
      metricGaugeValue("weight") shouldBe eventsSize.bytes
      metricGaugeValue("size") shouldBe events.size

    }

    "drop the oldest elements when new elements get added" in { env =>
      import env.*
      val events = generateEvents(num = 10, payloadSize = BytesUnit(100))
      val memoryNeededPerEvent = EventsBuffer.approximateEventSize(events.head1)
      val buf = new EventsBuffer(memoryNeededPerEvent * 5, loggerFactory, cacheMetrics)

      buf.bufferEvents(events)
      buf.snapshot() should contain theSameElementsInOrderAs events.takeRight(5)

      metricCounterValue("evictions") shouldBe 5L
      metricGaugeValue("weight") shouldBe memoryNeededPerEvent.bytes * 5L
      metricGaugeValue("size") shouldBe 5L

      // this also works when events get added one by one
      buf.invalidateBuffer()
      events.foreach(e => buf.bufferEvents(NonEmpty(Seq, e)))
      buf.snapshot() should contain theSameElementsInOrderAs events.takeRight(5)

      metricCounterValue("evictions") shouldBe 10L
      metricGaugeValue("weight") shouldBe memoryNeededPerEvent.bytes * 5L
      metricGaugeValue("size") shouldBe 5L
    }

    "always contain at least one event" in { env =>
      import env.*
      val smallEvents = generateEvents(num = 10, payloadSize = BytesUnit(100))
      val singleBigEvent = generateEvents(1, BytesUnit.MB(1), startIndex = 11)
      val memoryNeededForAllSmallEvents = EventsBuffer.approximateSize(smallEvents)
      val buf = new EventsBuffer(memoryNeededForAllSmallEvents, loggerFactory, cacheMetrics)

      memoryNeededForAllSmallEvents shouldBe <(EventsBuffer.approximateSize(singleBigEvent))

      buf.bufferEvents(smallEvents)
      buf.snapshot() should contain theSameElementsInOrderAs smallEvents

      buf.bufferEvents(singleBigEvent)
      buf.snapshot() should contain theSameElementsInOrderAs singleBigEvent
    }

    "handle a large amount of events with no payload" in { env =>
      import env.*
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
          val buf = new EventsBuffer(memoryRequiredForEvents, loggerFactory, cacheMetrics)
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

  class MetricsFixture {
    val factory = new InMemoryMetricsFactory
    private val metricsContext = MetricsContext("cache" -> "events-buffer")
    val cacheMetrics = new CacheMetrics("events-buffer", factory)
    def metricCounterValue(name: String): Long =
      factory.metrics
        .counters(MetricName.Daml :+ "cache" :+ name)(metricsContext)
        .markers(metricsContext)
        .get()

    def metricGaugeValue(name: String): Long =
      factory.metrics
        .asyncGauges(MetricName.Daml :+ "cache" :+ name)(metricsContext)()
        .asInstanceOf[Long]
  }
  override protected def withFixture(test: OneArgTest): Outcome = {
    val fixture = new MetricsFixture()
    val result = test(fixture)
    fixture.cacheMetrics.closeAcquired()
    result
  }

  override protected type FixtureParam = MetricsFixture
}
