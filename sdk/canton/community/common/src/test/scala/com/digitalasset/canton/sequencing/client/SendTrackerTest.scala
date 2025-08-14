// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{
  CommonMockMetrics,
  MetricsUtils,
  SequencerClientMetrics,
  TrafficConsumptionMetrics,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.{
  EventCostCalculator,
  TrafficReceipt,
  TrafficStateController,
}
import com.digitalasset.canton.sequencing.{
  RawProtocolEvent,
  SequencedProtocolEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise

class SendTrackerTest extends AnyWordSpec with BaseTest with MetricsUtils with FailOnShutdown {
  private lazy val metrics = CommonMockMetrics.sequencerClient
  private lazy val msgId1 = MessageId.tryCreate("msgId1")
  private lazy val msgId2 = MessageId.tryCreate("msgId2")

  private def sign(event: RawProtocolEvent): SignedContent[RawProtocolEvent] =
    SignedContent(event, SymbolicCrypto.emptySignature, None, testedProtocolVersion)

  private def deliverDefault(timestamp: CantonTimestamp): SequencedProtocolEvent =
    SequencedEventWithTraceContext(
      sign(
        SequencerTestUtils.mockDeliver(
          timestamp = timestamp,
          synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
        )
      )
    )(
      traceContext
    )

  private def deliver(
      msgId: MessageId,
      timestamp: CantonTimestamp,
      trafficReceipt: Option[TrafficReceipt] = None,
  ): SequencedProtocolEvent =
    SequencedEventWithTraceContext(
      sign(
        Deliver.create(
          None,
          timestamp,
          DefaultTestIdentities.physicalSynchronizerId,
          Some(msgId),
          Batch.empty(testedProtocolVersion),
          None,
          trafficReceipt,
        )
      )
    )(
      traceContext
    )

  private def deliverError(
      msgId: MessageId,
      timestamp: CantonTimestamp,
      trafficReceipt: Option[TrafficReceipt] = None,
  ): SequencedProtocolEvent =
    SequencedEventWithTraceContext(
      sign(
        DeliverError.create(
          None,
          timestamp,
          DefaultTestIdentities.physicalSynchronizerId,
          msgId,
          SequencerErrors.SubmissionRequestRefused("test"),
          trafficReceipt,
        )
      )
    )(
      traceContext
    )

  private case class Env(tracker: MySendTracker, store: InMemorySendTrackerStore)

  private class MySendTracker(
      initialPendingSends: Map[MessageId, CantonTimestamp],
      store: SendTrackerStore,
      metrics: SequencerClientMetrics,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      timeoutHandler: MessageId => Unit,
      val trafficStateController: Option[TrafficStateController],
  ) extends SendTracker(
        initialPendingSends,
        store,
        metrics,
        loggerFactory,
        timeouts,
        trafficStateController,
      ) {

    private val calls = new AtomicInteger()

    def callCount = calls.get()

    def assertNotCalled = callCount shouldBe 0

    override def handleTimeout(
        timestamp: CantonTimestamp
    )(msgId: MessageId)(implicit traceContext: TraceContext): Unit = {
      calls.incrementAndGet()
      timeoutHandler(msgId)
      super.handleTimeout(timestamp)(msgId)
    }

  }

  private val initialTrafficState = TrafficState.empty
  private def mkSendTracker(
      timeoutHandler: MessageId => Unit = _ => ()
  ): Env = {
    val store = new InMemorySendTrackerStore()
    val topologyClient =
      TestingTopology(Set(DefaultTestIdentities.physicalSynchronizerId))
        .build(loggerFactory)
        .forOwnerAndSynchronizer(participant1, DefaultTestIdentities.physicalSynchronizerId)

    val histogramInventory = new HistogramInventory()
    val trafficStateController = new TrafficStateController(
      DefaultTestIdentities.participant1,
      loggerFactory,
      topologyClient,
      initialTrafficState,
      testedProtocolVersion,
      new EventCostCalculator(loggerFactory),
      new TrafficConsumptionMetrics(MetricName("test"), metricsFactory(histogramInventory)),
      DefaultTestIdentities.physicalSynchronizerId,
    )
    val tracker =
      new MySendTracker(
        Map.empty,
        store,
        metrics,
        loggerFactory,
        timeouts,
        timeoutHandler,
        Some(trafficStateController),
      )

    Env(tracker, store)
  }

  implicit private val eventSpecificMetricsContext: MetricsContext = MetricsContext(
    "test" -> "value"
  )

  "tracking sends" should {

    "error if there's a previously tracked send with the same message id" in {
      val Env(tracker, _) = mkSendTracker()

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track first")
      tracker
        .track(msgId1, CantonTimestamp.MinValue)
        .swap
        .valueOrFail("track second") shouldBe SavePendingSendError.MessageIdAlreadyTracked
    }

    "is able to track a send with a prior message id if a receipt is observed" in {
      val Env(tracker, _) = mkSendTracker()

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track first")
      tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))
      tracker
        .track(msgId1, CantonTimestamp.MinValue)
        .valueOrFail(
          "track same msgId after receipt"
        )
      tracker.assertNotCalled
    }

    "propagate metrics context" in {
      val Env(tracker, _) = mkSendTracker()

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track first")
      tracker.update(
        Seq(
          deliver(
            msgId1,
            initialTrafficState.timestamp.immediateSuccessor,
            trafficReceipt = Some(
              TrafficReceipt(
                consumedCost = NonNegativeLong.tryCreate(1),
                extraTrafficConsumed = NonNegativeLong.tryCreate(2),
                baseTrafficRemainder = NonNegativeLong.tryCreate(3),
              )
            ),
          )
        )
      )

      tracker.trafficStateController.value.updateBalance(
        NonNegativeLong.tryCreate(20),
        PositiveInt.one,
        CantonTimestamp.MaxValue,
      )

      assertLongValue("test.extra-traffic-purchased", 20L)
      assertInContext(
        "test.extra-traffic-purchased",
        "member",
        DefaultTestIdentities.participant1.toString,
      )
      assertLongValue("test.event-delivered-cost", 1L)
      assertInContext(
        "test.event-delivered-cost",
        "synchronizer",
        DefaultTestIdentities.physicalSynchronizerId.toString,
      )
      assertInContext(
        "test.event-delivered-cost",
        "member",
        DefaultTestIdentities.participant1.toString,
      )
      // Event specific metrics should contain the event specific metrics context
      assertInContext("test.event-delivered-cost", "test", "value")
      assertLongValue("test.extra-traffic-consumed", 2L)
      assertInContext(
        "test.extra-traffic-consumed",
        "member",
        DefaultTestIdentities.participant1.toString,
      )
      assertInContext(
        "test.extra-traffic-consumed",
        "synchronizer",
        DefaultTestIdentities.physicalSynchronizerId.toString,
      )
      // But not the event agnostic metrics
      assertNotInContext("test.extra-traffic-consumed", "test")
    }

    "not re-export metrics when replaying events older than current state" in {
      val Env(tracker, _) = mkSendTracker()
      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track first")
      tracker.update(
        Seq(
          deliver(
            msgId1,
            initialTrafficState.timestamp,
            trafficReceipt = Some(
              TrafficReceipt(
                consumedCost = NonNegativeLong.tryCreate(1),
                extraTrafficConsumed = NonNegativeLong.tryCreate(2),
                baseTrafficRemainder = NonNegativeLong.tryCreate(3),
              )
            ),
          )
        )
      )

      assertNoValue("event-delivered-cost")

    }

    "metrics should contain default labels for unknown sends" in {
      val Env(tracker, _) = mkSendTracker()

      tracker.update(
        Seq(
          deliver(
            msgId1,
            initialTrafficState.timestamp.immediateSuccessor,
            trafficReceipt = Some(
              TrafficReceipt(
                consumedCost = NonNegativeLong.tryCreate(1),
                extraTrafficConsumed = NonNegativeLong.tryCreate(2),
                baseTrafficRemainder = NonNegativeLong.tryCreate(3),
              )
            ),
          )
        )
      )

      assertLongValue("test.event-delivered-cost", 1L)
      assertInContext(
        "test.event-delivered-cost",
        "member",
        DefaultTestIdentities.participant1.toString,
      )
      // Check there are labels for application-id and type
      assertInContext("test.event-delivered-cost", "application-id", "unknown")
      assertInContext("test.event-delivered-cost", "type", "unknown")

    }
  }

  "updating" should {
    def verifyEventRemovesPendingSend(event: SequencedProtocolEvent) = {
      val Env(tracker, store) = mkSendTracker()

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track msgId1")
      tracker.track(msgId2, CantonTimestamp.MinValue).valueOrFail("track msgId2")

      val pendingSends1 = store.fetchPendingSends
      pendingSends1 shouldBe Map(
        msgId1 -> CantonTimestamp.MinValue,
        msgId2 -> CantonTimestamp.MinValue,
      )

      tracker.update(Seq(event))
      val pendingSends2 = store.fetchPendingSends

      pendingSends2 shouldBe Map(msgId2 -> CantonTimestamp.MinValue)
      tracker.assertNotCalled
    }

    "remove tracked send on deliver event" in verifyEventRemovesPendingSend(
      deliver(msgId1, CantonTimestamp.MinValue)
    )

    "removed tracked send on deliver error event" in verifyEventRemovesPendingSend(
      deliverError(msgId1, CantonTimestamp.MinValue)
    )

    "notify only timed out events" in {
      val Env(tracker, _) = mkSendTracker()

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track msgId1")
      tracker
        .track(msgId2, CantonTimestamp.MinValue.plusSeconds(2))
        .valueOrFail("track msgId2")

      tracker.update(
        Seq(
          deliverDefault(CantonTimestamp.MinValue.plusSeconds(1))
        )
      )

      tracker.callCount shouldBe 1
      tracker.update(
        Seq(
          deliverDefault(CantonTimestamp.MinValue.plusSeconds(3))
        )
      )
      tracker.callCount shouldBe 2
    }

    "not get upset if we see the same message id twice" in {
      // during reconnects we may replay the same deliver/deliverEvent
      val Env(tracker, _) = mkSendTracker()

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track msgId1")
      tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))
      tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))

      succeed
    }

    "call timeout handlers sequentially" in {
      val concurrentCalls = new AtomicInteger()
      val totalCalls = new AtomicInteger()

      val Env(tracker, _) = mkSendTracker { _ =>
        totalCalls.incrementAndGet()
        if (!concurrentCalls.compareAndSet(0, 1)) {
          fail("timeout handler was called concurrently")
        }

        if (!concurrentCalls.compareAndSet(1, 0)) {
          fail("timeout handler was called concurrently")
        }
      }

      tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFail("track msgId1")
      tracker.track(msgId2, CantonTimestamp.MinValue).valueOrFail("track msgId2")
      tracker.update(Seq(deliverDefault(CantonTimestamp.MinValue.plusSeconds(1))))
      totalCalls.get() shouldBe 2
    }

    "track callback" should {
      class CaptureSendResultHandler {
        private val calledWithP = Promise[UnlessShutdown[SendResult]]()
        val handler: SendCallback = result => {
          calledWithP.success(result)
        }

        val result: FutureUnlessShutdown[SendResult] = FutureUnlessShutdown(calledWithP.future)
      }

      "callback with successful send" in {
        val Env(tracker, _) = mkSendTracker()
        val sendResultHandler = new CaptureSendResultHandler

        tracker
          .track(msgId1, CantonTimestamp.MinValue, sendResultHandler.handler)
          .valueOrFail("track msgId1")
        tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))

        sendResultHandler.result.futureValueUS should matchPattern { case SendResult.Success(_) => }
      }

      "callback with deliver error" in {
        val Env(tracker, _) = mkSendTracker()
        val sendResultHandler = new CaptureSendResultHandler

        tracker
          .track(msgId1, CantonTimestamp.MinValue, sendResultHandler.handler)
          .valueOrFail("track msgId1")
        tracker.update(
          Seq(deliverError(msgId1, CantonTimestamp.MinValue))
        )

        sendResultHandler.result.futureValueUS should matchPattern { case SendResult.Error(_) => }
      }

      "callback with timeout" in {
        val Env(tracker, _) = mkSendTracker()
        val sendResultHandler = new CaptureSendResultHandler
        val sendMaxSequencingTime = CantonTimestamp.MinValue
        val deliverEventTime = sendMaxSequencingTime.plusSeconds(1)

        tracker
          .track(msgId1, sendMaxSequencingTime, sendResultHandler.handler)
          .valueOrFail("track msgId1")
        tracker.update(Seq(deliverDefault(deliverEventTime)))

        sendResultHandler.result.futureValueUS should matchPattern {
          case SendResult.Timeout(`deliverEventTime`) =>
        }
      }
    }
  }
}
