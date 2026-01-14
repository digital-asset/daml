// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, TopologyConfig}
import com.digitalasset.canton.crypto.{Fingerprint, Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.sync.{ConnectedSynchronizer, SyncEphemeralState}
import com.digitalasset.canton.participant.synchronizer.SynchronizerHandle
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.{
  StoreBasedSynchronizerTopologyClient,
  SynchronizerTopologyClientWithInit,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  NoPackageDependencies,
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
  private val requestId =
    Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).addInt(0).finish()
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

  private def mockConnectedSynchronizer(
      topologyStore: TopologyStore[SynchronizerStore],
      topologyManager: SynchronizerTopologyManager,
  ) =
    mock[ConnectedSynchronizer].tap { cs =>
      val syncPersistentState = mock[SyncPersistentState].tap { ps =>
        when(ps.topologyStore).thenReturn(topologyStore)
        when(ps.topologyManager).thenReturn(topologyManager)
      }
      val synchronizerHandle = mock[SynchronizerHandle].tap { sh =>
        when(sh.syncPersistentState).thenReturn(syncPersistentState)
      }
      when(cs.psid).thenReturn(physicalSynchronizerId)
      when(cs.synchronizerHandle).thenReturn(synchronizerHandle)
    }

  private def mockConnectedSynchronizer(
      topologyStore: TopologyStore[SynchronizerStore],
      topologyClient: SynchronizerTopologyClientWithInit,
      topologyManager: SynchronizerTopologyManager,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  ) =
    mock[ConnectedSynchronizer].tap { cs =>
      val ephemeralState = mock[SyncEphemeralState].tap { es =>
        when(es.timeTracker).thenReturn(synchronizerTimeTracker)
      }
      val syncPersistentState = mock[SyncPersistentState].tap { ps =>
        when(ps.topologyStore).thenReturn(topologyStore)
        when(ps.topologyManager).thenReturn(topologyManager)
      }
      val synchronizerHandle = mock[SynchronizerHandle].tap { sh =>
        when(sh.syncPersistentState).thenReturn(syncPersistentState)
        when(sh.topologyClient).thenReturn(topologyClient)
      }
      when(cs.psid).thenReturn(physicalSynchronizerId)
      when(cs.ephemeral).thenReturn(ephemeralState)
      when(cs.synchronizerHandle).thenReturn(synchronizerHandle)
    }

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
        removals = if (proposal) Map.empty else Map(mapping.uniqueKey -> (serial.some, Set.empty)),
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
        val connectedSynchronizer =
          mockConnectedSynchronizer(topologyStore, topologyManager)

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
            .authorizeOnboardingTopology(params, connectedSynchronizer)
            .valueOrFail("expect authorization to succeed")
          _ <- add(topologyStore)(tsSerial, serial, ptpProposal).map(tx =>
            Right(tx): Either[TopologyManagerError, GenericSignedTopologyTransaction]
          )
          effectiveTsAfterO <- tw
            .authorizeOnboardingTopology(params, connectedSynchronizer)
            .valueOrFail("expect authorization to succeed")
        } yield {
          effectiveTsBeforeO shouldBe None
          effectiveTsAfterO shouldBe Some(EffectiveTime(tsSerial))
        }
      }.failOnShutdown

      "back off and wait when existing proposal already signed by TP" in {
        val tw = topologyWorkflow()
        val topologyManager = mockTopologyManager()
        val topologyStore = newTopologyStore()
        val connectedSynchronizer =
          mockConnectedSynchronizer(topologyStore, topologyManager)

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
          _ <- add(topologyStore)(tsSerialMinusTwo, serialBefore, ptpBefore)
          _ <- add(topologyStore)(tsSerialMinusOne, serial, ptpProposal, proposal = true)
          effectiveTsBeforeO <- tw
            .authorizeOnboardingTopology(params, connectedSynchronizer)
            .valueOrFail("expect authorization to succeed")
          _ <- add(topologyStore)(tsSerial, serial, ptpProposal).map(tx =>
            Right(tx): Either[TopologyManagerError, GenericSignedTopologyTransaction]
          )
          effectiveTsAfterO <- tw
            .authorizeOnboardingTopology(params, connectedSynchronizer)
            .valueOrFail("expect authorization to succeed")
        } yield {
          effectiveTsBeforeO shouldBe None
          effectiveTsAfterO shouldBe Some(EffectiveTime(tsSerial))
        }
      }.failOnShutdown

      "detect party not hosted on synchronizer" in {
        val tw = topologyWorkflow()
        val connectedSynchronizer =
          mockConnectedSynchronizer(newTopologyStore(), mockTopologyManager())
        tw
          .authorizeOnboardingTopology(params, connectedSynchronizer)
          .leftOrFail("expect failure")
          .map(_ should include regex "Party .* not hosted by source participant")
      }.failOnShutdown

      "detect party not hosted on source participant" in {
        val tw = topologyWorkflow()
        val topologyStore = newTopologyStore()
        val connectedSynchronizer =
          mockConnectedSynchronizer(topologyStore, mockTopologyManager())

        for {
          _ <- add(topologyStore)(tsSerialMinusOne, serialBefore, ptpPartyMissingFromSP)
          err <- tw
            .authorizeOnboardingTopology(params, connectedSynchronizer)
            .leftOrFail("expect failure")
        } yield {
          err should include regex "Party .* not hosted by source participant"
        }
      }.failOnShutdown

      "detect party not hosted on target participant as onboarding after authorization at serial" in {
        val tw = topologyWorkflow()
        val topologyStore = newTopologyStore()
        val connectedSynchronizer =
          mockConnectedSynchronizer(topologyStore, mockTopologyManager())

        for {
          _ <- add(topologyStore)(tsSerial, serial, ptpProposalMissingOnboardingFlag)
          err <- tw
            .authorizeOnboardingTopology(params, connectedSynchronizer)
            .leftOrFail("expect failure")
        } yield {
          err should include regex "Target participant .* not authorized to onboard party .* even though just added"
        }
      }.failOnShutdown
    }

    "clear onboarding" should {
      "complete authorization only when prerequisites are met" in {
        val tw = topologyWorkflow()
        val topologyStore = newTopologyStore()
        val topologyManager = mockTopologyManager()
        val clock = new SimClock(loggerFactory = loggerFactory)
        clock.advanceTo(tsSerial)
        val topologyClient = new StoreBasedSynchronizerTopologyClient(
          clock,
          store = topologyStore,
          packageDependencyResolver = NoPackageDependencies,
          topologyConfig = TopologyConfig(),
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
          staticSynchronizerParameters = defaultStaticSynchronizerParameters,
        )
        val synchronizerLatestTimeObservedUnsafe = Some(CantonTimestamp.ofEpochSecond(20L))
        val synchronizerLatestTimeObservedSafe = Some(CantonTimestamp.ofEpochSecond(3600L))
        val connectedSynchronizerSafe =
          mockConnectedSynchronizer(
            topologyStore,
            topologyClient,
            topologyManager,
            mockSynchronizerTimeTracker(synchronizerLatestTimeObservedSafe),
          )

        val connectedSynchronizerUnsafe =
          mockConnectedSynchronizer(
            topologyStore,
            topologyClient,
            topologyManager,
            mockSynchronizerTimeTracker(synchronizerLatestTimeObservedUnsafe),
          )

        val onboardingTs = tsSerialMinusOne
        // unsafe time means less than the default one minute decision time

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
          _ <- add(topologyStore)(tsSerialMinusTwo.minusSeconds(1), serialBefore2, ptpBefore)
          _ <- add(topologyStore)(
            tsSerialMinusTwo,
            serialBefore2,
            SynchronizerParametersState(
              synchronizerId,
              TestSynchronizerParameters.defaultDynamic,
            ),
          )
          errTooEarly <- tw
            .authorizeClearingOnboardingFlag(
              params,
              EffectiveTime(tsSerialMinusTwo),
              connectedSynchronizerSafe,
            )
            .leftOrFail("expect premature authorization to fail")
          _ <- add(topologyStore)(onboardingTs, serialBefore, ptpProposal)
          isOnboardedAfterUnsafeCall <- tw
            .authorizeClearingOnboardingFlag(
              params,
              EffectiveTime(onboardingTs),
              connectedSynchronizerUnsafe,
            )
            .valueOrFail("expect authorization to not happen due to unsafe time")
          isOnboardedAfterFirstSafeCall <- tw
            .authorizeClearingOnboardingFlag(
              params,
              EffectiveTime(onboardingTs),
              connectedSynchronizerSafe,
            )
            .valueOrFail("expect authorization to succeed")
          isOnboardedAfterSecondSafeCall <- tw
            .authorizeClearingOnboardingFlag(
              params,
              EffectiveTime(onboardingTs),
              connectedSynchronizerSafe,
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
