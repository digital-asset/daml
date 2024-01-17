// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError.AboveTrafficLimit
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficLimitsStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.{EventCostCalculator, TopUpEvent}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.FutureOutcome
import org.scalatest.flatspec.FixtureAsyncFlatSpec

import scala.concurrent.{ExecutionContext, Future}

class EnterpriseSequencerRateLimitManagerTest
    extends FixtureAsyncFlatSpec
    with BaseTest
    with HasExecutionContext {

  behavior of "EnterpriseSequencerRateLimiter"

  private val trafficConfig: TrafficControlParameters = TrafficControlParameters()
  private val sender: Member = mediatorIdX.member
  private val recipients: Recipients = Recipients.cc(participant1, participant2)
  private val envelope1: ClosedEnvelope = ClosedEnvelope.create(
    ByteString.copyFromUtf8("hello"),
    recipients,
    Seq.empty,
    testedProtocolVersion,
  )
  private val eventCost = 5L
  private val eventCostCalculator = mock[EventCostCalculator]
  private val batch: Batch[ClosedEnvelope] = Batch(List(envelope1), testedProtocolVersion)
  private val topUp: TopUpEvent = TopUpEvent(
    PositiveLong.tryCreate(10L),
    CantonTimestamp.Epoch,
    PositiveInt.one,
  )
  when(
    eventCostCalculator.computeEventCost(batch, trafficConfig.readVsWriteScalingFactor, Map.empty)
  )
    .thenReturn(NonNegativeLong.tryCreate(eventCost))
  private val sequencingTs = CantonTimestamp.Epoch.plusSeconds(1)
  private val someState = TrafficState
    .empty(sequencingTs)
    .copy(extraTrafficRemainder =
      NonNegativeLong.tryCreate(15L)
    ) // value is irrelevant, just different from inState
  private val inState = TrafficState.empty(CantonTimestamp.Epoch)
  private val sequencerMetrics = SequencerMetrics.noop("sequencer-rate-limit-manager-test")

  case class Env(
      trafficConfig: TrafficControlParameters,
      batch: Batch[ClosedEnvelope],
      rlm: SequencerRateLimitManager,
      srlm: SequencerMemberRateLimiter,
      trafficLimitsStore: TrafficLimitsStore,
      srlmFact: SequencerMemberRateLimiterFactory,
  )

  override type FixtureParam = Env

  it should "return new state if consume is successful" in { implicit f =>
    val outState = Right(someState)
    mockConsumeResponse(outState, None)
    mockGetLimitsStoreResponse()

    mockGetLimitsStoreResponse()

    f.rlm
      .consume(
        sender,
        f.batch,
        sequencingTs,
        inState,
        trafficConfig,
        Map.empty,
      )
      .value
      .map { state =>
        verifyNoPruning(f)
        state shouldBe outState
      }
  }

  it should "initialize new members with the top ups from the store" in { implicit f =>
    val outState = Right(someState)
    mockConsumeResponse(outState, None)
    mockGetLimitsStoreResponse()

    mockGetLimitsStoreResponse()

    f.rlm
      .consume(
        sender,
        f.batch,
        sequencingTs,
        inState,
        trafficConfig,
        Map.empty,
      )
      .value
      .map { state =>
        verifyNoPruning(f)
        verify(f.srlmFact, times(1)).create(
          sender,
          Seq(topUp),
          loggerFactory,
          sequencerMetrics,
          eventCostCalculator,
        )
        state shouldBe outState
      }
  }

  it should "return AboveTrafficLimit with new state" in { implicit f =>
    val outState =
      Left(
        AboveTrafficLimit(
          sender,
          NonNegativeLong.tryCreate(eventCost),
          Some(someState),
        )
      )

    mockConsumeResponse(outState, None)
    mockGetLimitsStoreResponse()

    f.rlm
      .consume(
        sender,
        f.batch,
        sequencingTs,
        inState,
        trafficConfig,
        Map.empty,
      )
      .value
      .map { state =>
        verifyNoPruning(f)
        state shouldBe outState
      }
  }

  it should "return traffic status for a member" in { implicit f =>
    mockGetLimitsStoreResponse(ts = inState.timestamp)
    when(f.srlm.getTrafficLimit(inState.timestamp)).thenReturn(NonNegativeLong.tryCreate(50L))
    val topUps = List(topUp, topUp.copy(limit = PositiveLong.tryCreate(56L)))
    when(f.srlm.pruneUntilAndGetAllTopUpsFor(inState.timestamp)).thenReturn(topUps)

    f.rlm
      .getTrafficStatusFor(Map(sender -> inState))
      .map { status =>
        status should have size 1
        status.head.timestamp shouldBe inState.timestamp
        status.head.member shouldBe sender
        status.head.trafficState.extraTrafficRemainder.value shouldBe 50L
        status.head.trafficState.extraTrafficLimit.value.value shouldBe 50L
        status.head.trafficState.extraTrafficConsumed.value shouldBe 0
        status.head.currentAndFutureTopUps should contain theSameElementsInOrderAs topUps
      }
  }

  it should "create new traffic state" in { implicit f =>
    mockGetLimitsStoreResponse()
    when(f.srlm.getTrafficLimit(sequencingTs)).thenReturn(NonNegativeLong.tryCreate(54L))

    f.rlm
      .createNewTrafficStateAt(sender, sequencingTs, trafficConfig)
      .map { state =>
        state.timestamp shouldBe sequencingTs
        state.extraTrafficConsumed.value shouldBe 0L
        state.extraTrafficRemainder.value shouldBe 54L
        state.baseTrafficRemainder.value shouldBe trafficConfig.maxBaseTrafficAmount.value
      }
  }

  it should "top up a member" in { implicit f =>
    val ts = sequencingTs.plusSeconds(1)
    val newTopUp =
      TopUpEvent(PositiveLong.tryCreate(152L), ts, PositiveInt.tryCreate(6))
    mockGetLimitsStoreResponse(ts = ts)
    doNothing.when(f.srlm).topUp(newTopUp)
    when(f.trafficLimitsStore.updateTotalExtraTrafficLimit(sender, newTopUp))
      .thenReturn(Future.unit)

    f.rlm
      .topUp(sender, newTopUp)
      .map { _ =>
        assert(true)
      }
  }

  it should "update traffic states" in { implicit f =>
    val updateTimestamp = sequencingTs.plusSeconds(1)

    // Create a new rate limiter mock for P1
    val srlmP1 = mock[SequencerMemberRateLimiter]
    when(
      f.srlmFact.create(
        argThat({ (member: Member) => member == participant1 }),
        any[Seq[TopUpEvent]],
        any[NamedLoggerFactory],
        any[SequencerMetrics],
        any[EventCostCalculator],
      )
    )
      .thenReturn(srlmP1)

    // Will return top up for sender
    mockGetLimitsStoreResponse(ts = updateTimestamp)
    mockGetLimitsStoreResponse(member = participant1, ts = updateTimestamp)

    when(f.srlm.getTrafficLimit(updateTimestamp))
      .thenReturn(NonNegativeLong.tryCreate(83L))

    when(srlmP1.getTrafficLimit(updateTimestamp))
      .thenReturn(NonNegativeLong.tryCreate(49L))

    val ts1 = TrafficState(
      NonNegativeLong.tryCreate(10L),
      NonNegativeLong.tryCreate(11L),
      NonNegativeLong.tryCreate(12L),
      CantonTimestamp.now(),
    )
    val ts2 = TrafficState(
      NonNegativeLong.tryCreate(20L),
      NonNegativeLong.tryCreate(21L),
      NonNegativeLong.tryCreate(22L),
      CantonTimestamp.now(),
    )
    val ts1Updated = TrafficState(
      NonNegativeLong.tryCreate(30L),
      NonNegativeLong.tryCreate(31L),
      NonNegativeLong.tryCreate(32L),
      CantonTimestamp.now(),
    )
    val ts2Updated = TrafficState(
      NonNegativeLong.tryCreate(40L),
      NonNegativeLong.tryCreate(41L),
      NonNegativeLong.tryCreate(42L),
      CantonTimestamp.now(),
    )

    when(f.srlm.updateTrafficState(updateTimestamp, trafficConfig, NonNegativeLong.zero, ts1))
      .thenReturn((ts1Updated, true, None))

    when(srlmP1.updateTrafficState(updateTimestamp, trafficConfig, NonNegativeLong.zero, ts2))
      .thenReturn((ts2Updated, true, None))

    f.rlm
      .updateTrafficStates(
        Map(
          sender -> ts1,
          participant1 -> ts2,
        ),
        updateTimestamp,
        trafficConfig,
      )
      .map { newStates =>
        val newSenderState = newStates.get(sender).value
        val newParticipantState = newStates.get(participant1).value

        newSenderState shouldBe ts1Updated
        newParticipantState shouldBe ts2Updated
      }
  }

  it should "cache member top up state in memory" in { implicit f =>
    val outState = Right(someState)
    mockConsumeResponse(outState, None)
    mockGetLimitsStoreResponse()

    mockGetLimitsStoreResponse()

    for {
      _ <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs,
          inState,
          trafficConfig,
          Map.empty,
        )
        .value
      _ = verify(f.trafficLimitsStore, times(1))
        .getExtraTrafficLimits(sender)(
          implicitly[ExecutionContext],
          implicitly[TraceContext],
        )
      _ <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs,
          inState,
          trafficConfig,
          Map.empty,
        )
        .value
      _ = verifyNoMoreInteractions(f.trafficLimitsStore)
    } yield assert(true)
  }

  it should "prune traffic limits store when a new top up becomes active" in { implicit f =>
    val sequencingTs = CantonTimestamp.Epoch.plusSeconds(1)
    val inState = TrafficState.empty(CantonTimestamp.Epoch)
    val outState = Right(someState)
    // Only the sequencer counter matters
    val prunableTopUpThreshold = TopUpEvent(
      PositiveLong.tryCreate(1),
      CantonTimestamp.Epoch,
      PositiveInt.tryCreate(5),
    )
    when(
      f.trafficLimitsStore.pruneBelowSerial(sender, PositiveInt.tryCreate(5))(
        implicitly[ExecutionContext],
        implicitly[TraceContext],
      )
    ).thenReturn(Future.unit)

    mockConsumeResponse(outState, Some(prunableTopUpThreshold))
    mockGetLimitsStoreResponse()

    f.rlm
      .consume(
        sender,
        f.batch,
        sequencingTs,
        inState,
        trafficConfig,
        Map.empty,
      )
      .value
      .map { state =>
        eventually() {
          verify(f.trafficLimitsStore, times(1)).pruneBelowSerial(sender, PositiveInt.tryCreate(5))(
            implicitly[ExecutionContext],
            implicitly[TraceContext],
          )
          ()
        }
        state shouldBe outState
      }
  }

  private def mockConsumeResponse(
      response: Either[AboveTrafficLimit, TrafficState],
      newTopUp: Option[TopUpEvent],
  )(implicit f: Env) = {
    when(f.srlm.tryConsume(f.batch, sequencingTs, f.trafficConfig, inState, Map.empty, sender))
      .thenReturn(response -> newTopUp)
  }

  private def mockGetLimitsStoreResponse(
      topUps: Seq[TopUpEvent] = Seq(topUp),
      ts: CantonTimestamp = sequencingTs,
      member: Member = sender,
  )(implicit f: Env): Unit = {
    when(
      f.trafficLimitsStore
        .getExtraTrafficLimits(member)(
          implicitly[ExecutionContext],
          implicitly[TraceContext],
        )
    ).thenReturn(Future.successful(topUps))
    ()
  }

  private def verifyNoPruning(implicit f: Env): Unit = {
    verify(f.trafficLimitsStore, times(0)).pruneBelowSerial(
      any[Member],
      any[PositiveInt],
    )(any[ExecutionContext], any[TraceContext])
    ()
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val trafficLimitsStore: TrafficLimitsStore = mock[TrafficLimitsStore]
    val srlm: SequencerMemberRateLimiter = mock[SequencerMemberRateLimiter]
    val srlmFact = mock[SequencerMemberRateLimiterFactory]
    when(
      srlmFact.create(
        argThat({ (member: Member) => member == sender }),
        any[Seq[TopUpEvent]],
        any[NamedLoggerFactory],
        any[SequencerMetrics],
        any[EventCostCalculator],
      )
    ).thenReturn(srlm)

    val rlm: EnterpriseSequencerRateLimitManager = new EnterpriseSequencerRateLimitManager(
      trafficLimitsStore,
      loggerFactory,
      futureSupervisor,
      timeouts,
      sequencerMetrics,
      srlmFact,
      eventCostCalculator,
    )

    val env = Env(
      trafficConfig,
      batch,
      rlm,
      srlm,
      trafficLimitsStore,
      srlmFact,
    )

    withFixture(test.toNoArgAsyncTest(env))
  }
}
