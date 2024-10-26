// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.TestingConfigInternal
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{ChangeId, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.admin.{PackageService, ResourceManagementService}
import com.digitalasset.canton.participant.domain.{DomainAliasManager, DomainRegistry}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.submission.{
  CommandDeduplicatorImpl,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.memory.InMemoryParticipantSettingsStore
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  PartyOps,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.{LifeCycleContainer, ParticipantNodeParameters}
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LedgerSubmissionId, LfPartyId}
import org.apache.pekko.stream.Materializer
import org.mockito.ArgumentMatchers
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
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
    val partyOps: PartyOps = mock[PartyOps]

    private val identityPusher = mock[ParticipantTopologyDispatcher]
    val partyNotifier = mock[LedgerServerPartyNotifier]
    private val syncCrypto = mock[SyncCryptoApiProvider]
    private val ledgerApiIndexer = mock[LedgerApiIndexer]
    val participantNodePersistentState = mock[ParticipantNodePersistentState]
    private val participantSettingsStore = new InMemoryParticipantSettingsStore(loggerFactory)
    val participantEventPublisher = mock[ParticipantEventPublisher]
    private val indexedStringStore = InMemoryIndexedStringStore()
    private val participantNodeEphemeralState = mock[ParticipantNodeEphemeralState]
    private val pruningProcessor = mock[PruningProcessor]
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

    when(participantNodePersistentState.settingsStore).thenReturn(participantSettingsStore)
    when(participantNodePersistentState.commandDeduplicationStore).thenReturn(
      commandDeduplicationStore
    )
    when(participantNodePersistentState.inFlightSubmissionStore).thenReturn(inFlightSubmissionStore)
    when(partyNotifier.resumePending()).thenReturn(Future.unit)

    when(participantNodeEphemeralState.participantEventPublisher).thenReturn(
      participantEventPublisher
    )
    when(participantEventPublisher.publishInitNeededUpstreamOnlyIfFirst(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.unit)
    when(domainConnectionConfigStore.getAll()).thenReturn(Seq.empty)
    when(aliasManager.ids).thenReturn(Set.empty)

    when(
      commandDeduplicationStore.storeDefiniteAnswers(
        any[Seq[(ChangeId, DefiniteAnswerEvent, Boolean)]]
      )(anyTraceContext)
    ).thenReturn(FutureUnlessShutdown.unit)
    when(inFlightSubmissionStore.delete(any[Seq[InFlightReference]])(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.unit)

    val clock = new SimClock(loggerFactory = loggerFactory)

    val commandDeduplicator = new CommandDeduplicatorImpl(
      store = Eval.now(commandDeduplicationStore),
      clock = clock,
      publicationTimeLowerBound = Eval.now(CantonTimestamp.MinValue),
      loggerFactory = loggerFactory,
    )

    val inFlightSubmissionTracker = new InFlightSubmissionTracker(
      store = Eval.now(inFlightSubmissionStore),
      deduplicator = commandDeduplicator,
      timeouts = LocalNodeParameters.processingTimeouts,
      loggerFactory = loggerFactory,
    )

    when(participantNodeEphemeralState.inFlightSubmissionTracker).thenReturn(
      inFlightSubmissionTracker
    )

    private val commandProgressTracker = mock[CommandProgressTracker]
    private val ledgerApiLifeCycleContainer = new LifeCycleContainer[LedgerApiIndexer](
      stateName = "mock-lapi-indexer",
      create = () => FutureUnlessShutdown.pure(ledgerApiIndexer),
      loggerFactory = loggerFactory,
    )

    val sync = new CantonSyncService(
      participantId,
      domainRegistry,
      domainConnectionConfigStore,
      aliasManager,
      Eval.now(participantNodePersistentState),
      participantNodeEphemeralState,
      syncDomainPersistentStateManager,
      Eval.now(packageService),
      partyOps,
      identityPusher,
      partyNotifier,
      syncCrypto,
      pruningProcessor,
      DAMLe.newEngine(enableLfDev = false, enableLfBeta = false, enableStackTraces = false),
      commandProgressTracker,
      syncDomainStateFactory,
      clock,
      new ResourceManagementService(
        Eval.now(participantSettingsStore),
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
      TestingConfigInternal(),
      ledgerApiLifeCycleContainer,
    )
  }

  override type FixtureParam = Fixture

  override def withFixture(test: OneArgTest): Outcome =
    test(Fixture())

  "Canton sync service" should {
    "emit add party event" in { f =>
      when(
        f.partyOps.allocateParty(
          any[PartyId],
          any[ParticipantId],
          any[ProtocolVersion],
        )(any[TraceContext], any[ExecutionContext])
      ).thenReturn(EitherT.rightT(()))

      when(
        f.participantEventPublisher.publishEventDelayableByRepairOperation(any[Update])(
          anyTraceContext
        )
      )
        .thenReturn(FutureUnlessShutdown.unit)

      when(
        f.partyNotifier.expectPartyAllocationForNodes(
          any[PartyId],
          any[ParticipantId],
          any[String255],
          any[Option[String255]],
        )
      ).thenReturn(Either.unit)

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

      val result = fut.map { _ =>
        verify(f.partyOps).allocateParty(
          eqTo(partyId),
          eqTo(f.participantId),
          eqTo(ProtocolVersion.latest),
        )(anyTraceContext, any[ExecutionContext])
        succeed
      }

      result.futureValue
    }
  }
}
