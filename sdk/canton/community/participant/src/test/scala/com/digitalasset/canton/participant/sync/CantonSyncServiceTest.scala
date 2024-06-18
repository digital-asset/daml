// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.implicits.*
import cats.{Eval, Id}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder, TreeTransactionBuilder}
import com.daml.lf.transaction.{CommittedTransaction, VersionedTransaction}
import com.daml.lf.value.Value.ValueRecord
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.TestingConfigInternal
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.admin.workflows.java.PackageID
import com.digitalasset.canton.participant.admin.{PackageService, ResourceManagementService}
import com.digitalasset.canton.participant.domain.{DomainAliasManager, DomainRegistry}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.pruning.NoOpPruningProcessor
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.ParticipantEventLog.ProductionParticipantEventLogId
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.{
  InMemoryParticipantEventLog,
  InMemoryParticipantSettingsStore,
}
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.TransactionAccepted
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  ParticipantTopologyManager,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.{
  DefaultParticipantStateValues,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  DefaultDamlValues,
  HasExecutionContext,
  LedgerSubmissionId,
  LfPartyId,
}
import org.apache.pekko.stream.Materializer
import org.mockito.ArgumentMatchers
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.Future
import scala.jdk.FutureConverters.*

class CantonSyncServiceTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {

  private val LocalNodeParameters = ParticipantNodeParameters.forTestingOnly(testedProtocolVersion)

  case class Fixture() {

    val participantId: ParticipantId = ParticipantId("CantonSyncServiceTest")
    private val domainRegistry = mock[DomainRegistry]
    private val aliasManager = mock[DomainAliasManager]
    private val syncDomainPersistentStateManager = mock[SyncDomainPersistentStateManager]
    private val domainConnectionConfigStore = mock[DomainConnectionConfigStore]
    private val packageService = mock[PackageService]
    val topologyManager: ParticipantTopologyManager = mock[ParticipantTopologyManager]

    private val identityPusher = mock[ParticipantTopologyDispatcher]
    val partyNotifier = mock[LedgerServerPartyNotifier]
    private val syncCrypto = mock[SyncCryptoApiProvider]
    private val multiDomainEventLog = mock[MultiDomainEventLog]
    val participantNodePersistentState = mock[ParticipantNodePersistentState]
    private val participantSettingsStore = new InMemoryParticipantSettingsStore(loggerFactory)
    val participantEventPublisher = mock[ParticipantEventPublisher]
    private val participantEventLog =
      new InMemoryParticipantEventLog(ProductionParticipantEventLogId, loggerFactory)
    private val indexedStringStore = InMemoryIndexedStringStore()
    private val participantNodeEphemeralState = mock[ParticipantNodeEphemeralState]
    private val pruningProcessor = NoOpPruningProcessor
    private val commandDeduplicationStore = mock[CommandDeduplicationStore]
    private val inFlightSubmissionStore = mock[InFlightSubmissionStore]
    private val sequencerInfoLoader = mock[SequencerInfoLoader]

    private implicit val mat: Materializer = mock[Materializer] // not used
    private val syncDomainStateFactory: SyncDomainEphemeralStateFactory =
      mock[SyncDomainEphemeralStateFactoryImpl]

    participantSettingsStore
      .insertMaxDeduplicationDuration(NonNegativeFiniteDuration.Zero)
      .failOnShutdown
      .futureValue

    when(participantNodePersistentState.participantEventLog).thenReturn(participantEventLog)
    when(participantNodePersistentState.multiDomainEventLog).thenReturn(multiDomainEventLog)
    when(participantNodePersistentState.settingsStore).thenReturn(participantSettingsStore)
    when(participantNodePersistentState.commandDeduplicationStore).thenReturn(
      commandDeduplicationStore
    )
    when(participantNodePersistentState.inFlightSubmissionStore).thenReturn(inFlightSubmissionStore)
    when(partyNotifier.resumePending()).thenReturn(Future.unit)

    when(
      multiDomainEventLog.fetchUnpublished(
        ArgumentMatchers.eq(ProductionParticipantEventLogId),
        ArgumentMatchers.eq(None),
      )(anyTraceContext)
    )
      .thenReturn(Future.successful(List.empty))
    when(multiDomainEventLog.lookupByEventIds(any[Seq[EventId]])(anyTraceContext))
      .thenReturn(Future.successful(Map.empty))

    when(participantNodeEphemeralState.participantEventPublisher).thenReturn(
      participantEventPublisher
    )
    when(participantEventPublisher.publishTimeModelConfigNeededUpstreamOnlyIfFirst(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.unit)
    when(domainConnectionConfigStore.getAll()).thenReturn(Seq.empty)
    when(aliasManager.ids).thenReturn(Set.empty)

    when(
      commandDeduplicationStore.storeDefiniteAnswers(
        any[Seq[(ChangeId, DefiniteAnswerEvent, Boolean)]]
      )(anyTraceContext)
    ).thenReturn(Future.unit)
    when(inFlightSubmissionStore.delete(any[Seq[InFlightReference]])(anyTraceContext))
      .thenReturn(Future.unit)

    val sync = new CantonSyncService(
      participantId,
      domainRegistry,
      domainConnectionConfigStore,
      aliasManager,
      Eval.now(participantNodePersistentState),
      participantNodeEphemeralState,
      syncDomainPersistentStateManager,
      packageService,
      topologyManager,
      identityPusher,
      partyNotifier,
      syncCrypto,
      pruningProcessor,
      DAMLe.newEngine(
        uniqueContractKeys = false,
        enableLfDev = false,
        enableLfPreview = false,
        enableStackTraces = false,
      ),
      syncDomainStateFactory,
      new SimClock(loggerFactory = loggerFactory),
      new ResourceManagementService.CommunityResourceManagementService(
        None,
        ParticipantTestMetrics,
      ),
      LocalNodeParameters,
      SyncDomain.DefaultFactory,
      indexedStringStore,
      new MemoryStorage(loggerFactory, timeouts),
      ParticipantTestMetrics,
      sequencerInfoLoader,
      () => true,
      FutureSupervisor.Noop,
      SuppressingLogger(getClass),
      skipRecipientsCheck = false,
      multiDomainLedgerAPIEnabled = false,
      TestingConfigInternal(),
    )
  }

  override type FixtureParam = Fixture

  override def withFixture(test: OneArgTest): Outcome =
    test(Fixture())

  "Canton sync service" should {
    "emit add party event" in { f =>
      when(
        f.topologyManager.allocateParty(
          any[String255],
          any[PartyId],
          any[ParticipantId],
          any[ProtocolVersion],
        )(any[TraceContext])
      ).thenReturn(EitherT.rightT(()))

      when(f.participantEventPublisher.publish(any[LedgerSyncEvent])(anyTraceContext))
        .thenReturn(FutureUnlessShutdown.unit)

      when(
        f.partyNotifier.expectPartyAllocationForXNodes(
          any[PartyId],
          any[ParticipantId],
          any[String255],
        )
      ).thenReturn(Right(()))

      val lfInputPartyId = LfPartyId.assertFromString("desiredPartyName")
      val partyId =
        PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"$lfInputPartyId::default"))
      when(
        f.partyNotifier.setDisplayName(
          ArgumentMatchers.eq(partyId),
          ArgumentMatchers.eq(String255.tryCreate("displayName")),
        )(anyTraceContext)
      )
        .thenReturn(Future.successful(()))

      val submissionId = LedgerSubmissionId.assertFromString("CantonSyncServiceTest submission")

      val fut = f.sync
        .allocateParty(Some(lfInputPartyId), Some("displayName"), submissionId)(
          TraceContext.empty
        )
        .asScala

      val result = fut.map(_ => {
        verify(f.topologyManager).allocateParty(
          eqTo(String255.tryCreate(submissionId)),
          eqTo(partyId),
          eqTo(f.participantId),
          eqTo(testedProtocolVersion),
        )(anyTraceContext)
        succeed
      })

      result.futureValue
    }

    def stats(sync: CantonSyncService, packageId: String): Option[Int] = {

      import TransactionBuilder.Implicits.*

      val createNode = TestNodeBuilder.create(
        id = TransactionBuilder.newCid,
        templateId = Ref.Identifier(packageId, Ref.QualifiedName("M", "D")),
        argument = ValueRecord(None, ImmArray.Empty),
        signatories = Seq("Alice"),
        observers = Seq.empty,
      )

      val tx: VersionedTransaction = TreeTransactionBuilder.toVersionedTransaction(createNode)

      lazy val event = TransactionAccepted(
        completionInfoO = DefaultParticipantStateValues.completionInfo(List.empty).some,
        transactionMeta = DefaultParticipantStateValues.transactionMeta(),
        transaction = CommittedTransaction.subst[Id](tx),
        transactionId = DefaultDamlValues.lfTransactionId(1),
        recordTime = CantonTimestamp.Epoch.toLf,
        divulgedContracts = List.empty,
        blindingInfoO = None,
        hostedWitnesses = Nil,
        contractMetadata = Map(),
      )

      Option(sync.eventTranslationStrategy.augmentTransactionStatistics(event))
        .collect({ case ta: TransactionAccepted => ta })
        .flatMap(_.completionInfoO)
        .flatMap(_.statistics)
        .map(_.committed.actions)

    }

    "populate metering" in { f =>
      stats(f.sync, "packageX") shouldBe Some(1)
    }

    "not include ping-pong packages in metering" in { f =>
      stats(f.sync, PackageID.PingPong) shouldBe Some(0)
    }

    "not include dar-distribution packages in metering" in { f =>
      stats(f.sync, PackageID.DarDistribution) shouldBe Some(0)
    }

  }
}
