// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.testedProtocolVersion
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
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
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshotLoader,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.{
  Namespace,
  PhysicalSynchronizerId,
  SequencerGroup,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.concurrent.ExecutionContext

class TimeAdvancingTopologySubscriberV1Test extends AnyWordSpec with BaseTest {

  import TimeAdvancingTopologySubscriberV1Test.*

  // To avoid context-switching and test entire code paths
  implicit private val executionContext: ExecutionContext = directExecutionContext

  "TimeAdvancingTopologySubscriber" should {
    "not schedule a time-advancement message if the effective time is not in the future" in {
      // given
      val clock = mock[Clock]
      val ts = CantonTimestamp.Epoch

      val subscriber =
        new TimeAdvancingTopologySubscriberV1(
          clock,
          mock[SequencerClient],
          mock[SynchronizerTopologyClientWithInit],
          aPhysicalSynchronizerId,
          aSequencerId,
          loggerFactory,
        )

      // when
      subscriber
        .observed(
          SequencedTime(ts),
          EffectiveTime(ts),
          SequencerCounter(0),
          transactions = Seq.empty,
        )
        .discard

      // then
      verify(clock, never).scheduleAfter(any[CantonTimestamp => Any], any[Duration])
    }

    "schedule and send a time-advancement message" in {
      // given
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)
      val clock = new SimClock(start = ts2, loggerFactory)

      val sequencerClient = mock[SequencerClientSend]
      when(
        sequencerClient.send(
          batch = any[Batch[DefaultOpenEnvelope]],
          topologyTimestamp = any[Option[CantonTimestamp]],
          maxSequencingTime = any[CantonTimestamp],
          messageId = any[MessageId],
          aggregationRule = any[Option[AggregationRule]],
          callback = eqTo(SendCallback.empty),
          amplify = eqTo(false),
        )(any[TraceContext], any[MetricsContext])
      ).thenReturn(
        EitherT(FutureUnlessShutdown.pure(Right(()): Either[SendAsyncClientError, Unit]))
      )

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

      val subscriber =
        new TimeAdvancingTopologySubscriberV1(
          clock,
          sequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          aSequencerId,
          loggerFactory,
        )

      val expectedBatch =
        Batch.of(
          testedProtocolVersion,
          Seq(
            TopologyTransactionsBroadcast(aPhysicalSynchronizerId, Seq.empty) ->
              Recipients.cc(AllMembersOfSynchronizer)
          )*
        )
      val expectedAggregationRule =
        NonEmpty
          .from(sequencerGroup.active)
          .map(sequencerGroup =>
            AggregationRule(sequencerGroup, threshold = PositiveInt.one, testedProtocolVersion)
          )

      // when
      subscriber
        .observed(
          SequencedTime(ts1),
          EffectiveTime(ts2),
          SequencerCounter(0),
          transactions = Seq.empty,
        )
        .discard
      clock.advance(staticSynchronizerParameters.topologyChangeDelay.duration)

      // then
      verify(sequencerClient)
        .send(
          batch = eqTo(expectedBatch),
          topologyTimestamp = eqTo(None),
          maxSequencingTime =
            eqTo(ts2.plus(TimeAdvancingTopologyConfig.defaultMaxSequencingTimeWindow.asJava)),
          messageId = any[MessageId],
          aggregationRule = eqTo(expectedAggregationRule),
          callback = eqTo(SendCallback.empty),
          amplify = eqTo(false),
        )(any[TraceContext], any[MetricsContext])
    }

    "not schedule a time-advancing message if it's not an active sequencer" in {
      // given
      val clock = mock[Clock]
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)
      val passiveSequencerId =
        SequencerId.tryCreate("passive", Namespace(Fingerprint.tryFromString("fingerprint")))

      val snapshot = mock[TopologySnapshotLoader]
      when(snapshot.sequencerGroup()).thenReturn(
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
      when(topologyClient.currentSnapshotApproximation).thenReturn(
        FutureUnlessShutdown.pure(snapshot)
      )
      val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
      when(topologyClient.staticSynchronizerParameters).thenReturn(staticSynchronizerParameters)

      val subscriber =
        new TimeAdvancingTopologySubscriberV1(
          clock,
          mock[SequencerClient],
          topologyClient,
          aPhysicalSynchronizerId,
          passiveSequencerId, // boom!
          loggerFactory,
        )

      // when
      subscriber
        .observed(
          SequencedTime(ts1),
          EffectiveTime(ts2),
          SequencerCounter(0),
          transactions = Seq.empty,
        )
        .discard

      // then
      verify(clock, never).scheduleAfter(any[CantonTimestamp => Any], any[Duration])
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

      val subscriber =
        new TimeAdvancingTopologySubscriberV1(
          mock[Clock],
          mockSequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          aSequencerId,
          loggerFactory,
        )

      // when
      subscriber.broadcastToAdvanceTime(EffectiveTime(CantonTimestamp.Epoch))

      // then
      verify(topologyClient).currentSnapshotApproximation
      verify(snapshot).sequencerGroup()
    }

    // A case caught by flakiness in the sequencer node bootstrap test.
    "not send a time-advancing message if it's not an active sequencer after asking for a sequencer group again" in {
      // given
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

      val subscriber =
        new TimeAdvancingTopologySubscriberV1(
          mock[Clock],
          sequencerClient,
          topologyClient,
          aPhysicalSynchronizerId,
          aSequencerId, // boom!
          loggerFactory,
        )

      // when
      subscriber.broadcastToAdvanceTime(EffectiveTime(CantonTimestamp.Epoch))

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
    }
  }
}

object TimeAdvancingTopologySubscriberV1Test {
  private val aPhysicalSynchronizerId =
    PhysicalSynchronizerId(
      SynchronizerId.tryFromString("id::default"),
      NonNegativeInt.tryCreate(0),
      testedProtocolVersion,
    )

  private val aSequencerId =
    SequencerId.tryCreate("sequencer", Namespace(Fingerprint.tryFromString("fingerprint")))
}
