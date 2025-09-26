// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.StoreBasedSynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreTestData,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  SynchronizerParametersState,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  SynchronizerId,
  SynchronizerTopologyManager,
  TopologyManager,
  TopologyManagerError,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.chaining.scalaUtilChainingOps

class PartyReplicationTopologyWorkflowTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {
  private val requestId = Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()
  private val partyId = PartyId.tryFromProtoPrimitive("onboarding::namespace")
  private val synchronizerId = SynchronizerId.tryFromString("synchronizer::namespace")
  private val physicalSynchronizerId = synchronizerId.toPhysical
  private val sp = ParticipantId("source-participant")
  private val tp = ParticipantId("target-participant")
  private val serial = PositiveInt.tryCreate(17)
  private val serialBefore = PositiveInt.tryCreate(serial.unwrap - 1)
  private val serialBefore2 = PositiveInt.tryCreate(serial.unwrap - 2)
  private val participantPermission = ParticipantPermission.Confirmation
  private val params = PartyReplicationStatus.ReplicationParams(
    requestId,
    partyId,
    synchronizerId,
    sp,
    tp,
    serial,
    participantPermission,
  )

  private val tsSerialMinusTwo = CantonTimestamp.ofEpochSecond(-1L)
  private val tsSerialMinusOne = CantonTimestamp.Epoch
  private val tsSerial = CantonTimestamp.ofEpochSecond(1L)

  private val ptpPartyMissingFromSP = PartyToParticipant.tryCreate(
    partyId = partyId,
    threshold = PositiveInt.one,
    participants =
      Seq(HostingParticipant(ParticipantId("other-participant"), ParticipantPermission.Submission)),
  )
  private val ptpBefore = PartyToParticipant.tryCreate(
    partyId = partyId,
    threshold = PositiveInt.one,
    participants = Seq(HostingParticipant(sp, ParticipantPermission.Submission)),
  )

  private val ptpProposal = PartyToParticipant.tryCreate(
    partyId = partyId,
    threshold = PositiveInt.one,
    participants = Seq(
      HostingParticipant(sp, ParticipantPermission.Submission),
      HostingParticipant(
        params.targetParticipantId,
        params.participantPermission,
        onboarding = true,
      ),
    ),
  )

  private val ptpProposalMissingOnboardingFlag = PartyToParticipant.tryCreate(
    partyId = partyId,
    threshold = PositiveInt.one,
    participants = Seq(
      HostingParticipant(sp, ParticipantPermission.Submission),
      HostingParticipant(
        params.targetParticipantId,
        params.participantPermission,
        onboarding = false,
      ),
    ),
  )

  private val topologyStoreTestData =
    new TopologyStoreTestData(testedProtocolVersion, loggerFactory, executionContext)

  private def topologyWorkflow(p: ParticipantId = tp): PartyReplicationTopologyWorkflow =
    new PartyReplicationTopologyWorkflow(
      participantId = p,
      timeouts = DefaultProcessingTimeouts.testing,
      loggerFactory = loggerFactory,
    )

  private def mockTopologyManager() =
    mock[SynchronizerTopologyManager].tap { tm =>
      when(tm.psid).thenReturn(physicalSynchronizerId)
      when(tm.managerVersion).thenReturn(TopologyManager.PV(testedProtocolVersion))
    }

  private def newTopologyStore() =
    new InMemoryTopologyStore(
      SynchronizerStore(physicalSynchronizerId),
      testedProtocolVersion,
      loggerFactory,
      DefaultProcessingTimeouts.testing,
    )

  private def mockSynchronizerTimeTracker(tsToReturnO: Option[CantonTimestamp]) =
    mock[SynchronizerTimeTracker].tap { timeTracker =>
      when(timeTracker.requestTick(any[CantonTimestamp], any[Boolean])(anyTraceContext))
        .thenReturn(SynchronizerTimeTracker.DummyTickRequest)
      when(timeTracker.latestTime).thenReturn(tsToReturnO)
    }

  private def add(topologyStore: TopologyStore[SynchronizerStore])(
      ts: CantonTimestamp,
      serial: PositiveInt,
      mapping: TopologyMapping,
      proposal: Boolean = false,
  ) = {
    val signedTx =
      topologyStoreTestData.makeSignedTx(mapping, serial = serial, isProposal = proposal)(
        topologyStoreTestData.p1Key
      )
    topologyStore
      .update(
        SequencedTime(ts),
        EffectiveTime(ts),
        removeMapping = if (proposal) Map.empty else Map(mapping.uniqueKey -> serial),
        removeTxs = Set.empty,
        additions = Seq(ValidatedTopologyTransaction(signedTx)),
      )
      .map(_ => signedTx)
  }

  "PartyReplicationTopologyWorkflow" when {
    "onboarding" should {
      "complete authorization when prerequisites are met" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()

        when(
          topologyManager.proposeAndAuthorize(
            op = TopologyChangeOp.Replace,
            mapping = ptpProposal,
            serial = Some(serial),
            signingKeys = Seq.empty,
            protocolVersion = testedProtocolVersion,
            expectFullAuthorization = false,
            forceChanges = ForceFlags.none,
            waitToBecomeEffective = None,
          )
        ).thenReturn(
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](
            topologyStoreTestData.makeSignedTx(ptpProposal, serial = serial, isProposal = true)(
              topologyStoreTestData.p1Key
            )
          )
        )

        for {
          _ <- add(topologyStore)(tsSerialMinusOne, serialBefore, ptpBefore)
          effectiveTsBeforeO <- tw
            .authorizeOnboardingTopology(params, topologyManager, topologyStore)
            .valueOrFail("expect authorization to succeed")
          _ <- add(topologyStore)(tsSerial, serial, ptpProposal).map(tx =>
            Right(tx): Either[TopologyManagerError, GenericSignedTopologyTransaction]
          )
          effectiveTsAfterO <- tw
            .authorizeOnboardingTopology(params, topologyManager, topologyStore)
            .valueOrFail("expect authorization to succeed")
        } yield {
          effectiveTsBeforeO shouldBe None
          effectiveTsAfterO shouldBe Some(tsSerial)
        }
      }.failOnShutdown

      "back off and wait when existing proposal already signed by TP" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()

        when(
          topologyManager.extendSignature(
            any[SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]],
            signingKeys = eqTo(Seq.empty),
            eqTo(ForceFlags.none),
          )(anyTraceContext)
        ).thenReturn(
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](
            topologyStoreTestData.makeSignedTx(ptpProposal, serial = serial, isProposal = true)(
              // returning the same transaction and number of keys indicates that that TP has already signed
              // because signing again does not add a new signature
              topologyStoreTestData.p1Key
            )
          )
        )

        for {
          _ <- add(topologyStore)(tsSerialMinusOne, serialBefore, ptpBefore)
          _ <- add(topologyStore)(tsSerial, serial, ptpProposal, proposal = true)
          effectiveTsBeforeO <- tw
            .authorizeOnboardingTopology(params, topologyManager, topologyStore)
            .valueOrFail("expect authorization to succeed")
          _ <- add(topologyStore)(tsSerial, serial, ptpProposal).map(tx =>
            Right(tx): Either[TopologyManagerError, GenericSignedTopologyTransaction]
          )
          effectiveTsAfterO <- tw
            .authorizeOnboardingTopology(params, topologyManager, topologyStore)
            .valueOrFail("expect authorization to succeed")
        } yield {
          effectiveTsBeforeO shouldBe None
          effectiveTsAfterO shouldBe Some(tsSerial)
        }
      }.failOnShutdown

      "detect party not hosted on synchronizer" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()
        tw
          .authorizeOnboardingTopology(params, topologyManager, topologyStore)
          .leftOrFail("expect failure")
          .map(_ should include regex "Party .* not hosted by source participant")
      }.failOnShutdown

      "detect party not hosted on source participant" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()

        for {
          _ <- add(topologyStore)(tsSerialMinusOne, serialBefore, ptpPartyMissingFromSP)
          err <- tw
            .authorizeOnboardingTopology(params, topologyManager, topologyStore)
            .leftOrFail("expect failure")
        } yield {
          err should include regex "Party .* not hosted by source participant"
        }
      }.failOnShutdown

      "detect party not hosted on target participant as onboarding after authorization at serial" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()

        for {
          _ <- add(topologyStore)(tsSerial, serial, ptpProposalMissingOnboardingFlag)
          err <- tw
            .authorizeOnboardingTopology(params, topologyManager, topologyStore)
            .leftOrFail("expect failure")
        } yield {
          err should include regex "Target participant .* not authorized to onboard party .* even though just added"
        }
      }.failOnShutdown
    }

    "onboarded" should {
      "complete authorization only when prerequisites are met" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()
        val clock = new SimClock(loggerFactory = loggerFactory)
        clock.advanceTo(tsSerial)
        val topologyClient = new StoreBasedSynchronizerTopologyClient(
          clock,
          store = topologyStore,
          packageDependenciesResolver = StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
          staticSynchronizerParameters = defaultStaticSynchronizerParameters,
        )
        val onboardingTs = tsSerialMinusOne
        // unsafe time means less than the default one minute decision time
        val synchronizerLatestTimeObservedUnsafe = Some(CantonTimestamp.ofEpochSecond(20L))
        val synchronizerLatestTimeObservedSafe = Some(CantonTimestamp.ofEpochSecond(3600L))

        when(
          topologyManager.proposeAndAuthorize(
            op = TopologyChangeOp.Replace,
            mapping = ptpProposalMissingOnboardingFlag,
            serial = Some(serial),
            signingKeys = Seq.empty,
            protocolVersion = testedProtocolVersion,
            expectFullAuthorization = true,
            forceChanges = ForceFlags.none,
            waitToBecomeEffective = None,
          )
        ).thenAnswer[TopologyChangeOp, TopologyMapping, Option[PositiveInt], Seq[
          Fingerprint
        ], ProtocolVersion, Boolean, ForceFlags, Option[NonNegativeFiniteDuration]] {
          case (_, mapping, _, _, _, _, _, _) =>
            // Have the topology manager mock store the transaction in test topology store.
            EitherT.right[TopologyManagerError](
              add(topologyStore)(tsSerial, serial, mapping)
            )
        }

        for {
          _ <- topologyClient.observed(
            SequencedTime(tsSerial),
            EffectiveTime(tsSerial),
            SequencerCounter.Genesis,
            Seq.empty,
          )
          _ <- add(topologyStore)(tsSerialMinusTwo, serialBefore2, ptpBefore)
          _ <- add(topologyStore)(
            tsSerialMinusTwo,
            serialBefore2,
            SynchronizerParametersState(
              synchronizerId,
              TestSynchronizerParameters.defaultDynamic,
            ),
          )
          errTooEarly <- tw
            .authorizeOnboardedTopology(
              params,
              tsSerialMinusTwo,
              mockSynchronizerTimeTracker(synchronizerLatestTimeObservedSafe),
              topologyManager,
              topologyStore,
              topologyClient,
            )
            .leftOrFail("expect premature authorization to fail")
          _ <- add(topologyStore)(onboardingTs, serialBefore, ptpProposal)
          isOnboardedAfterUnsafeCall <- tw
            .authorizeOnboardedTopology(
              params,
              onboardingTs,
              mockSynchronizerTimeTracker(synchronizerLatestTimeObservedUnsafe),
              topologyManager,
              topologyStore,
              topologyClient,
            )
            .valueOrFail("expect authorization to not happen due to unsafe time")
          isOnboardedAfterFirstSafeCall <- tw
            .authorizeOnboardedTopology(
              params,
              onboardingTs,
              mockSynchronizerTimeTracker(synchronizerLatestTimeObservedSafe),
              topologyManager,
              topologyStore,
              topologyClient,
            )
            .valueOrFail("expect authorization to succeed")
          isOnboardedAfterSecondSafeCall <- tw
            .authorizeOnboardedTopology(
              params,
              onboardingTs,
              mockSynchronizerTimeTracker(synchronizerLatestTimeObservedSafe),
              topologyManager,
              topologyStore,
              topologyClient,
            )
            .valueOrFail("expect second call observe party onboarded")
        } yield {
          errTooEarly should include regex "Party .* is not hosted by target participant"
          isOnboardedAfterUnsafeCall shouldBe false
          isOnboardedAfterFirstSafeCall shouldBe false
          isOnboardedAfterSecondSafeCall shouldBe true
        }
      }.failOnShutdown
    }
  }
}
