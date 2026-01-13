// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  TopologyTransactionsBroadcast,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClient,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  AllMembersOfSynchronizer,
  Batch,
  MessageId,
  Recipients,
}
import com.digitalasset.canton.synchronizer.sequencer.config.TimeAdvancingTopologyConfig
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshotLoader,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Namespace,
  SequencerGroup,
  SequencerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

class TimeAdvancingTopologySubscriberV2Test extends AnyWordSpec with BaseTest {

  import TimeAdvancingTopologySubscriberV2Test.*

  // To avoid context-switching and test entire code paths
  implicit private val executionContext: ExecutionContext = directExecutionContext

  private lazy val config = TimeAdvancingTopologyConfig()

  "TimeAdvancingTopologySubscriber" should {
    "not send a time-advancement message if the effective time is not in the future" in {
      // given
      val clock = new SimClock(loggerFactory = loggerFactory)
      val ts = CantonTimestamp.Epoch
      val tracker = new TestBroadcastTimeTrackerImpl

      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)

      val mockSequencerClient = mock[SequencerClient]

      val subscriber =
        new TimeAdvancingTopologySubscriberV2(
          clock,
          mockSequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          aSequencerId,
          tracker,
          config,
          timeouts,
          loggerFactory,
        )

      // when
      subscriber
        .observed(
          SequencedTime(ts),
          EffectiveTime(ts),
          SequencerCounter(0),
          transactions = Seq(mock[GenericSignedTopologyTransaction]),
        )
        .futureValueUS

      // then
      clock.advance(Duration.ofDays(1000))

      always() {
        verify(mockSequencerClient, never)
          .send(
            any[Batch[DefaultOpenEnvelope]],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[Option[AggregationRule]],
            any[SendCallback],
            any[Boolean],
          )(any[TraceContext], any[MetricsContext])
      }.discard

      subscriber.close()
    }

    "send a time-advancement message" in {
      // given
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)
      val clock = new SimClock(start = ts2, loggerFactory)
      val tracker = new TestBroadcastTimeTrackerImpl

      val sequencerClient = mock[SequencerClientSend]
      when(
        sequencerClient.send(
          batch = any[Batch[DefaultOpenEnvelope]],
          topologyTimestamp = any[Option[CantonTimestamp]],
          maxSequencingTime = any[CantonTimestamp],
          messageId = any[MessageId],
          aggregationRule = any[Option[AggregationRule]],
          callback = any[SendCallback],
          amplify = eqTo(false),
        )(any[TraceContext], any[MetricsContext])
      ).thenReturn(EitherTUtil.unitUS[SendAsyncClientError])

      val snapshot = mock[TopologySnapshotLoader]
      val sequencerGroup =
        SequencerGroup(
          active = Seq(aSequencerId),
          passive = Seq.empty,
          threshold = PositiveInt.one,
        )
      when(snapshot.sequencerGroup()(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(Some(sequencerGroup)))
      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(snapshot))
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)
      val topologyChangeDelay = staticSynchronizerParameters.topologyChangeDelay

      val subscriber = new TimeAdvancingTopologySubscriberV2(
        clock,
        sequencerClient,
        topologyClient,
        aPhysicalSynchronizerId,
        aSequencerId,
        tracker,
        config,
        timeouts,
        loggerFactory,
      )

      val expectedBatch = Batch.of(
        testedProtocolVersion,
        TopologyTransactionsBroadcast(aPhysicalSynchronizerId, Seq.empty) ->
          Recipients.cc(AllMembersOfSynchronizer),
      )
      val expectedAggregationRule = AggregationRule(
        NonEmptyUtil.fromUnsafe(sequencerGroup.active),
        threshold = PositiveInt.one,
        testedProtocolVersion,
      )

      // when
      val effectiveTime = EffectiveTime(ts1.plus(topologyChangeDelay.duration))
      subscriber
        .observed(
          SequencedTime(ts1),
          effectiveTime,
          SequencerCounter(0),
          Seq(mock[GenericSignedTopologyTransaction]),
        )
        .futureValueUS
      clock.advance(
        staticSynchronizerParameters.topologyChangeDelay.duration
          .plus(config.gracePeriod.asJava)
          .plus(config.pollBackoff.asJava)
      )

      // then
      verify(sequencerClient)
        .send(
          batch = eqTo(expectedBatch),
          topologyTimestamp = eqTo(None),
          maxSequencingTime = eqTo(effectiveTime.value.plus(config.maxSequencingTimeWindow.asJava)),
          messageId = any[MessageId],
          aggregationRule = eqTo(Some(expectedAggregationRule)),
          callback = any[SendCallback],
          amplify = eqTo(false),
        )(any[TraceContext], any[MetricsContext])

      subscriber.close()
    }

    "not send a time-advancing message if it's not an active sequencer" in {
      // given
      val clock = new SimClock(loggerFactory = loggerFactory)
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)
      val passiveSequencerId =
        SequencerId.tryCreate("passive", Namespace(Fingerprint.tryFromString("fingerprint")))
      val tracker = new TestBroadcastTimeTrackerImpl

      val snapshot = mock[TopologySnapshotLoader]
      when(snapshot.sequencerGroup()(any[TraceContext])).thenReturn(
        FutureUnlessShutdown.pure(
          Some(
            SequencerGroup(
              active = Seq(aSequencerId),
              passive = Seq(passiveSequencerId),
              threshold = PositiveInt.one,
            )
          )
        )
      )
      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(snapshot))
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)

      val mockSequencerClient = mock[SequencerClientSend]

      val subscriber =
        new TimeAdvancingTopologySubscriberV2(
          clock,
          mockSequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          passiveSequencerId, // boom!
          tracker,
          config,
          timeouts,
          loggerFactory,
        )

      // when
      subscriber
        .observed(
          SequencedTime(ts1),
          EffectiveTime(ts2),
          SequencerCounter(0),
          transactions = Seq(mock[GenericSignedTopologyTransaction]),
        )
        .futureValueUS

      // then
      clock.advance(Duration.ofDays(1000))

      always() {
        verify(mockSequencerClient, never).send(
          any[Batch[DefaultOpenEnvelope]],
          any[Option[CantonTimestamp]],
          any[CantonTimestamp],
          any[MessageId],
          any[Option[AggregationRule]],
          any[SendCallback],
          any[Boolean],
        )(any[TraceContext], any[MetricsContext])
      }.discard

      subscriber.close()
    }

    // A case caught by flakiness in the sequencer off-boarding integration test.
    "ask for a fresh sequencer group again just before sending a time-advancing message" in {
      // given
      val snapshot = mock[TopologySnapshotLoader]
      val sequencerGroup =
        SequencerGroup(
          active = Seq(aSequencerId),
          passive = Seq.empty,
          threshold = PositiveInt.one,
        )
      when(snapshot.sequencerGroup()).thenReturn(FutureUnlessShutdown.pure(Some(sequencerGroup)))
      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      when(topologyClient.currentSnapshotApproximation).thenReturn(
        FutureUnlessShutdown.pure(snapshot)
      )
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)

      val tracker = new TestBroadcastTimeTrackerImpl

      val mockSequencerClient = mock[SequencerClient]
      when(
        mockSequencerClient.send(
          any[Batch[DefaultOpenEnvelope]],
          any[Option[CantonTimestamp]],
          any[CantonTimestamp],
          any[MessageId],
          any[Option[AggregationRule]],
          any[SendCallback],
          any[Boolean],
        )(any[TraceContext], any[MetricsContext])
      ).thenReturn(EitherTUtil.unitUS)

      val clock = new SimClock(loggerFactory = loggerFactory)

      val subscriber =
        new TimeAdvancingTopologySubscriberV2(
          clock,
          mockSequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          aSequencerId,
          tracker,
          config,
          timeouts,
          loggerFactory,
        )

      // when
      subscriber.broadcastToAdvanceTime(clock.now, EffectiveTime(CantonTimestamp.Epoch), 0)

      // then
      verify(topologyClient).currentSnapshotApproximation
      verify(snapshot).sequencerGroup()

      subscriber.close()
    }

    // A case caught by flakiness in the sequencer node bootstrap test.
    "not send a time-advancing message if it's not an active sequencer after asking for a sequencer group again" in {
      // given
      val clock = new SimClock(loggerFactory = loggerFactory)
      val sequencerClient = mock[SequencerClient]

      val snapshot = mock[TopologySnapshotLoader]
      val sequencerGroup =
        SequencerGroup(
          active = Seq.empty,
          passive = Seq(aSequencerId),
          threshold = PositiveInt.one,
        )
      when(snapshot.sequencerGroup()).thenReturn(FutureUnlessShutdown.pure(Some(sequencerGroup)))
      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      when(topologyClient.currentSnapshotApproximation).thenReturn(
        FutureUnlessShutdown.pure(snapshot)
      )
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)

      val tracker = new TestBroadcastTimeTrackerImpl

      val subscriber =
        new TimeAdvancingTopologySubscriberV2(
          clock,
          sequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          aSequencerId, // boom!
          tracker,
          config,
          timeouts,
          loggerFactory,
        )

      // when
      subscriber.broadcastToAdvanceTime(clock.now, EffectiveTime(CantonTimestamp.Epoch), 0)

      // then
      verify(sequencerClient, never).send(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        any[SendCallback],
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])

      subscriber.close()
    }

    "do not send time advancement if we observed a broadcast messages within the grace period" in {
      // given
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)
      val clock = new SimClock(start = ts2, loggerFactory)
      val tracker = new TestBroadcastTimeTrackerImpl

      val sequencerClient = mock[SequencerClientSend]
      when(
        sequencerClient.send(
          batch = any[Batch[DefaultOpenEnvelope]],
          topologyTimestamp = any[Option[CantonTimestamp]],
          maxSequencingTime = any[CantonTimestamp],
          messageId = any[MessageId],
          aggregationRule = any[Option[AggregationRule]],
          callback = any[SendCallback],
          amplify = eqTo(false),
        )(any[TraceContext], any[MetricsContext])
      ).thenReturn(EitherTUtil.unitUS[SendAsyncClientError])

      val snapshot = mock[TopologySnapshotLoader]
      val sequencerGroup =
        SequencerGroup(
          active = Seq(aSequencerId),
          passive = Seq.empty,
          threshold = PositiveInt.one,
        )
      when(snapshot.sequencerGroup()(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(Some(sequencerGroup)))
      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(snapshot))
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)
      val topologyChangeDelay = staticSynchronizerParameters.topologyChangeDelay

      val subscriber = new TimeAdvancingTopologySubscriberV2(
        clock,
        sequencerClient,
        topologyClient,
        aPhysicalSynchronizerId,
        aSequencerId,
        tracker,
        config,
        timeouts,
        loggerFactory,
      )

      // when
      val effectiveTime = EffectiveTime(ts1.plus(topologyChangeDelay.duration))
      subscriber
        .observed(
          SequencedTime(ts1),
          effectiveTime,
          SequencerCounter(0),
          Seq(mock[GenericSignedTopologyTransaction]),
        )
        .futureValueUS

      val slack = java.time.Duration.ofMillis(10)

      require(
        config.gracePeriod.asJava.compareTo(
          config.pollBackoff.asJava.plus(slack)
        ) > 0,
        "This test required that the grace period is longer than the poll interval plus some slack",
      )

      clock.advance(
        staticSynchronizerParameters.topologyChangeDelay.duration
          .plus(config.pollBackoff.asJava)
          .plus(slack)
      )
      // Simulate the receiving of a broadcast message
      tracker.setLastBroadcastTimestamp(effectiveTime.value.plus(slack))

      // Since the grace period is longer than the poll interval, this should suffice to trigger another periodic check
      clock.advance(config.gracePeriod.asJava)

      // then
      always() {
        verify(sequencerClient, never)
          .send(
            batch = any[Batch[DefaultOpenEnvelope]],
            topologyTimestamp = any[Option[CantonTimestamp]],
            maxSequencingTime = any[CantonTimestamp],
            messageId = any[MessageId],
            aggregationRule = any[Option[AggregationRule]],
            callback = any[SendCallback],
            amplify = any[Boolean],
          )(any[TraceContext], any[MetricsContext])
      }.discard

      subscriber.close()
    }

    "conflate broadcasts for multiple topology transactions" in {
      // given
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)
      val clock = new SimClock(start = ts2, loggerFactory)
      val tracker = new TestBroadcastTimeTrackerImpl

      val sequencerClient = mock[SequencerClientSend]
      when(
        sequencerClient.send(
          batch = any[Batch[DefaultOpenEnvelope]],
          topologyTimestamp = any[Option[CantonTimestamp]],
          maxSequencingTime = any[CantonTimestamp],
          messageId = any[MessageId],
          aggregationRule = any[Option[AggregationRule]],
          callback = any[SendCallback],
          amplify = eqTo(false),
        )(any[TraceContext], any[MetricsContext])
      ).thenReturn(EitherTUtil.unitUS[SendAsyncClientError])

      val snapshot = mock[TopologySnapshotLoader]
      val sequencerGroup =
        SequencerGroup(
          active = Seq(aSequencerId),
          passive = Seq.empty,
          threshold = PositiveInt.one,
        )
      when(snapshot.sequencerGroup()(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(Some(sequencerGroup)))
      val topologyClient = mock[SynchronizerTopologyClientWithInit]
      when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(snapshot))
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)
      val topologyChangeDelay = staticSynchronizerParameters.topologyChangeDelay

      val subscriber = new TimeAdvancingTopologySubscriberV2(
        clock,
        sequencerClient,
        topologyClient,
        aPhysicalSynchronizerId,
        aSequencerId,
        tracker,
        config,
        timeouts,
        loggerFactory,
      )

      // when
      val effectiveTime1 = EffectiveTime(ts1.plus(topologyChangeDelay.duration))
      subscriber
        .observed(
          SequencedTime(ts1),
          effectiveTime1,
          SequencerCounter(0),
          Seq(mock[GenericSignedTopologyTransaction]),
        )
        .futureValueUS

      subscriber
        .observed(
          SequencedTime(ts1.immediateSuccessor),
          effectiveTime1.immediateSuccessor,
          SequencerCounter(1),
          Seq(mock[GenericSignedTopologyTransaction]),
        )
        .futureValueUS

      clock.advance(java.time.Duration.ofDays(100))

      always() {
        verify(sequencerClient, times(1))
          .send(
            batch = any[Batch[DefaultOpenEnvelope]],
            topologyTimestamp = any[Option[CantonTimestamp]],
            maxSequencingTime = any[CantonTimestamp],
            messageId = any[MessageId],
            aggregationRule = any[Option[AggregationRule]],
            callback = any[SendCallback],
            amplify = any[Boolean],
          )(any[TraceContext], any[MetricsContext])
      }.discard

      subscriber.close()
    }
  }
}

object TimeAdvancingTopologySubscriberV2Test {
  private val aPhysicalSynchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val aSequencerId = DefaultTestIdentities.sequencerId
}

class TestBroadcastTimeTrackerImpl extends BroadcastTimeTracker {
  private val lastBroadcastTimestampRef =
    new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)

  override def lastBroadcastTimestamp: CantonTimestamp = lastBroadcastTimestampRef.get()

  def setLastBroadcastTimestamp(timestamp: CantonTimestamp): Unit =
    lastBroadcastTimestampRef.set(timestamp)
}
