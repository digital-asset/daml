// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.synchronizer.metrics.{SequencerHistograms, SequencerMetrics}
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

      val buf = new EventsBuffer(BytesUnit(0L), loggerFactory, sequencerMetrics)
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
      val buf = new EventsBuffer(memoryNeeded * 3, loggerFactory, sequencerMetrics)

      buf.bufferEvents(events)

      buf.snapshot() should contain theSameElementsInOrderAs events
      metricGaugeValue("weight") shouldBe eventsSize.bytes
      metricGaugeValue("size") shouldBe events.size

    }

    "drop the oldest elements when new elements get added" in { env =>
      import env.*
      val events = generateEvents(num = 10, payloadSize = BytesUnit(100))
      val memoryNeededPerEvent = EventsBuffer.approximateEventSize(events.head1)
      val buf =
        new EventsBuffer(memoryNeededPerEvent * 5, loggerFactory, sequencerMetrics)

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
      val buf = new EventsBuffer(
        memoryNeededForAllSmallEvents,
        loggerFactory,
        sequencerMetrics,
      )

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
          val buf =
            new EventsBuffer(memoryRequiredForEvents, loggerFactory, sequencerMetrics)
          measureAddingEvents(buf, initialEvents)
          (0 to 10).foreach { i =>
            val newEvents =
              generateEvents(numNewEvents, BytesUnit.zero, numEvents + i * numNewEvents)
            measureAddingEvents(buf, newEvents)
          }
        }
    }

    "track head and last timestamps when buffer is empty" in { env =>
      import env.*
      new EventsBuffer(BytesUnit.MB(1), loggerFactory, sequencerMetrics)

      // Initially both timestamps should be 0 for empty buffer
      sequencerMetricGaugeValue("head_timestamp") shouldBe 0L
      sequencerMetricGaugeValue("last_timestamp") shouldBe 0L
    }

    "track head and last timestamps when events are added" in { env =>
      import env.*
      val buf = new EventsBuffer(BytesUnit.MB(1), loggerFactory, sequencerMetrics)
      val events = generateEvents(5, BytesUnit(100))

      buf.bufferEvents(events)

      // Head timestamp should be from the first event (index 1, epoch + 1 second)
      val expectedHeadTimestamp = CantonTimestamp.Epoch.plusSeconds(1L).toMicros
      sequencerMetricGaugeValue("head_timestamp") shouldBe expectedHeadTimestamp

      // Last timestamp should be from the last event (index 5, epoch + 5 seconds)
      val expectedLastTimestamp = CantonTimestamp.Epoch.plusSeconds(5L).toMicros
      sequencerMetricGaugeValue("last_timestamp") shouldBe expectedLastTimestamp
    }

    "update head timestamp when old events are evicted" in { env =>
      import env.*
      val events = generateEvents(num = 10, payloadSize = BytesUnit(100))
      val memoryNeededPerEvent = EventsBuffer.approximateEventSize(events.head1)
      val buf =
        new EventsBuffer(memoryNeededPerEvent * 5, loggerFactory, sequencerMetrics)

      buf.bufferEvents(events)

      // Buffer should keep only the last 5 events (indices 6-10)
      // Head timestamp should be from event at index 6 (epoch + 6 seconds)
      val expectedHeadTimestamp = CantonTimestamp.Epoch.plusSeconds(6L).toMicros
      sequencerMetricGaugeValue("head_timestamp") shouldBe expectedHeadTimestamp

      // Last timestamp should be from event at index 10 (epoch + 10 seconds)
      val expectedLastTimestamp = CantonTimestamp.Epoch.plusSeconds(10L).toMicros
      sequencerMetricGaugeValue("last_timestamp") shouldBe expectedLastTimestamp
    }

    "update timestamps when buffer is invalidated" in { env =>
      import env.*
      val buf = new EventsBuffer(BytesUnit.MB(1), loggerFactory, sequencerMetrics)
      val events = generateEvents(5, BytesUnit(100))

      buf.bufferEvents(events)

      // Verify timestamps are set
      sequencerMetricGaugeValue("head_timestamp") should be > 0L
      sequencerMetricGaugeValue("last_timestamp") should be > 0L

      buf.invalidateBuffer()

      // After invalidation, both timestamps should be reset to 0
      sequencerMetricGaugeValue("head_timestamp") shouldBe 0L
      sequencerMetricGaugeValue("last_timestamp") shouldBe 0L
    }

    "have same head and last timestamp for single event buffer" in { env =>
      import env.*
      val singleBigEvent = generateEvents(1, BytesUnit.MB(1))
      val buf = new EventsBuffer(BytesUnit(100), loggerFactory, sequencerMetrics)

      buf.bufferEvents(singleBigEvent)

      // With only one event, head and last timestamps should be the same
      val expectedTimestamp = CantonTimestamp.Epoch.plusSeconds(1L).toMicros
      sequencerMetricGaugeValue("head_timestamp") shouldBe expectedTimestamp
      sequencerMetricGaugeValue("last_timestamp") shouldBe expectedTimestamp
    }

    "update timestamps correctly when events are added incrementally" in { env =>
      import env.*
      val memoryNeededPerEvent = EventsBuffer.approximateEventSize(
        generateEvents(1, BytesUnit(100)).head1
      )
      val buf =
        new EventsBuffer(memoryNeededPerEvent * 3, loggerFactory, sequencerMetrics)

      // Add first batch of events
      val firstBatch = generateEvents(2, BytesUnit(100), startIndex = 1)
      buf.bufferEvents(firstBatch)

      sequencerMetricGaugeValue("head_timestamp") shouldBe CantonTimestamp.Epoch
        .plusSeconds(1L)
        .toMicros
      sequencerMetricGaugeValue("last_timestamp") shouldBe CantonTimestamp.Epoch
        .plusSeconds(2L)
        .toMicros

      // Add more events that will cause eviction
      val secondBatch = generateEvents(3, BytesUnit(100), startIndex = 3)
      buf.bufferEvents(secondBatch)

      // Buffer should keep only last 3 events (indices 3, 4, 5)
      sequencerMetricGaugeValue("head_timestamp") shouldBe CantonTimestamp.Epoch
        .plusSeconds(3L)
        .toMicros
      sequencerMetricGaugeValue("last_timestamp") shouldBe CantonTimestamp.Epoch
        .plusSeconds(5L)
        .toMicros
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
    private val metricsContext = MetricsContext("cache" -> "events-fan-out-buffer")
    val sequencerMetrics: SequencerMetrics = {
      val (_, metrics) = HistogramInventory.create(implicit inventory =>
        new SequencerMetrics(new SequencerHistograms(MetricName.Daml), factory)
      )
      metrics
    }
    def metricCounterValue(name: String): Long =
      factory.metrics
        .counters(MetricName.Daml :+ "cache" :+ name)(metricsContext)
        .markers(metricsContext)
        .get()

    def metricGaugeValue(name: String): Long =
      factory.metrics
        .asyncGauges(MetricName.Daml :+ "cache" :+ name)(metricsContext)()
        .asInstanceOf[Long]

    def sequencerMetricGaugeValue(name: String): Long =
      factory.metrics
        .gauges(MetricName.Daml :+ "sequencer" :+ name)(MetricsContext.Empty)
        .getValue
        .asInstanceOf[Long]
  }
  override protected def withFixture(test: OneArgTest): Outcome = {
    val fixture = new MetricsFixture()
    val result = test(fixture)
    fixture.sequencerMetrics.eventBuffer.closeAcquired()
    result
  }

  override protected type FixtureParam = MetricsFixture
}
