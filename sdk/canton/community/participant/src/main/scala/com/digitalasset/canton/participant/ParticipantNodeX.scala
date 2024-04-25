// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.health.{ComponentStatus, GrpcHealthReporter, HealthService}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.CommunityParticipantFactoryCommon
import com.digitalasset.canton.participant.admin.{
  PackageDependencyResolver,
  PackageOps,
  PackageOpsX,
  ResourceManagementService,
}
import com.digitalasset.canton.participant.config.{LocalParticipantConfig, PartyNotificationConfig}
import com.digitalasset.canton.participant.domain.DomainAliasResolution
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.scheduler.{
  ParticipantSchedulersParameters,
  SchedulersWithParticipantPruning,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  CantonSyncService,
  ParticipantEventPublisher,
  SyncDomainPersistentStateManager,
  SyncDomainPersistentStateManagerX,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  IdentityProvidingServiceClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{PartyMetadataStore, TopologyStore}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class ParticipantNodeBootstrapX(
    arguments: CantonNodeBootstrapCommonArguments[
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ],
    engine: Engine,
    cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
    setStartableStoppableLedgerApiAndCantonServices: (
        StartableStoppableLedgerApiServer,
        StartableStoppableLedgerApiDependentServices,
    ) => Unit,
    resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    createSchedulers: ParticipantSchedulersParameters => Future[SchedulersWithParticipantPruning] =
      _ => Future.successful(SchedulersWithParticipantPruning.noop),
    private[canton] val persistentStateFactory: ParticipantNodePersistentStateFactory,
    ledgerApiServerFactory: CantonLedgerApiServerFactory,
    setInitialized: () => Unit,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends CantonNodeBootstrapX[
      ParticipantNodeX,
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ](arguments)
    with ParticipantNodeBootstrapCommon {

  // TODO(#12946) clean up to remove SingleUseCell
  private val cantonSyncService = new SingleUseCell[CantonSyncService]

  override protected def sequencedTopologyStores: Seq[TopologyStore[DomainStore]] =
    cantonSyncService.get.toList.flatMap(_.syncDomainPersistentStateManager.getAll.values).collect {
      case s: SyncDomainPersistentState => s.topologyStore
    }

  override protected def sequencedTopologyManagers: Seq[DomainTopologyManager] =
    cantonSyncService.get.toList.flatMap(_.syncDomainPersistentStateManager.getAll.values).collect {
      case s: SyncDomainPersistentState => s.topologyManager
    }

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ): BootstrapStageOrLeaf[ParticipantNodeX] =
    new StartupNode(storage, crypto, nodeId, manager, healthReporter, healthService)

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      topologyManager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ) extends BootstrapStage[ParticipantNodeX, RunningNode[ParticipantNodeX]](
        description = "Startup participant node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val participantId = ParticipantId(nodeId)

    private val packageDependencyResolver = new PackageDependencyResolver(
      DamlPackageStore(
        storage,
        arguments.futureSupervisor,
        arguments.parameterConfig,
        loggerFactory,
      ),
      arguments.parameterConfig.processingTimeouts,
      loggerFactory,
    )

    private val componentFactory = new ParticipantComponentBootstrapFactory {

      override def createSyncDomainAndTopologyDispatcher(
          aliasResolution: DomainAliasResolution,
          indexedStringStore: IndexedStringStore,
      ): (SyncDomainPersistentStateManager, ParticipantTopologyDispatcher) = {
        val manager = new SyncDomainPersistentStateManagerX(
          aliasResolution,
          storage,
          indexedStringStore,
          parameters,
          config.topology,
          crypto,
          clock,
          futureSupervisor,
          loggerFactory,
        )

        val topologyDispatcher =
          new ParticipantTopologyDispatcher(
            topologyManager,
            participantId,
            manager,
            crypto,
            clock,
            config,
            parameterConfig.processingTimeouts,
            futureSupervisor,
            loggerFactory,
          )

        (manager, topologyDispatcher)
      }

      override def createPackageOps(
          manager: SyncDomainPersistentStateManager,
          crypto: SyncCryptoApiProvider,
      ): PackageOps = {
        val authorizedTopologyStoreClient = new StoreBasedTopologySnapshot(
          CantonTimestamp.MaxValue,
          topologyManager.store,
          StoreBasedDomainTopologyClient.NoPackageDependencies,
          loggerFactory,
        )
        val packageOpsX = new PackageOpsX(
          participantId = participantId,
          headAuthorizedTopologySnapshot = authorizedTopologyStoreClient,
          manager = manager,
          topologyManager = topologyManager,
          nodeId = nodeId,
          initialProtocolVersion = ProtocolVersion.latest,
          loggerFactory = StartupNode.this.loggerFactory,
          timeouts = timeouts,
        )

        addCloseable(packageOpsX)
        packageOpsX
      }
    }

    private val participantOps = new ParticipantTopologyManagerOps {
      override def allocateParty(
          validatedSubmissionId: CantonRequireTypes.String255,
          partyId: PartyId,
          participantId: ParticipantId,
          protocolVersion: ProtocolVersion,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
        // TODO(#14069) make this "extend" / not replace
        //    this will also be potentially racy!
        performUnlessClosingEitherUSF(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              TopologyChangeOp.Replace,
              PartyToParticipant(
                partyId,
                None,
                threshold = PositiveInt.one,
                participants =
                  Seq(HostingParticipant(participantId, ParticipantPermission.Submission)),
                groupAddressing = false,
              ),
              serial = None,
              // TODO(#12390) auto-determine signing keys
              signingKeys = Seq(partyId.uid.namespace.fingerprint),
              protocolVersion,
              expectFullAuthorization = true,
            )
        )
          .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
          .map(_ => ())
      }

    }

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[ParticipantNodeX]]] = {
      val indexedStringStore =
        IndexedStringStore.create(
          storage,
          parameterConfig.cachingConfigs.indexedStrings,
          timeouts,
          loggerFactory,
        )
      addCloseable(indexedStringStore)
      val partyMetadataStore =
        PartyMetadataStore(storage, parameterConfig.processingTimeouts, loggerFactory)
      addCloseable(partyMetadataStore)
      val adminToken = CantonAdminToken.create(crypto.pureCrypto)
      // upstream party information update generator

      val partyNotifierFactory = (eventPublisher: ParticipantEventPublisher) => {
        val partyNotifier = new LedgerServerPartyNotifier(
          participantId,
          eventPublisher,
          partyMetadataStore,
          clock,
          arguments.futureSupervisor,
          mustTrackSubmissionIds = true,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )
        // Notify at participant level if eager notification is configured, else rely on notification via domain.
        if (parameterConfig.partyChangeNotification == PartyNotificationConfig.Eager) {
          topologyManager.addObserver(partyNotifier.attachToIdentityManager())
        }
        partyNotifier
      }

      createParticipantServices(
        participantId,
        crypto,
        storage,
        persistentStateFactory,
        engine,
        ledgerApiServerFactory,
        indexedStringStore,
        cantonSyncServiceFactory,
        setStartableStoppableLedgerApiAndCantonServices,
        resourceManagementServiceFactory,
        replicationServiceFactory,
        createSchedulers,
        partyNotifierFactory,
        adminToken,
        participantOps,
        packageDependencyResolver,
        componentFactory,
      ).map {
        case (
              partyNotifier,
              sync,
              ephemeralState,
              ledgerApiServer,
              ledgerApiDependentServices,
              schedulers,
              topologyDispatcher,
            ) =>
          if (cantonSyncService.putIfAbsent(sync).nonEmpty) {
            sys.error("should not happen")
          }
          addCloseable(partyNotifier)
          addCloseable(ephemeralState.participantEventPublisher)
          addCloseable(topologyDispatcher)
          addCloseable(schedulers)
          addCloseable(sync)
          addCloseable(ledgerApiServer)
          addCloseable(ledgerApiDependentServices)
          addCloseable(packageDependencyResolver)
          val node = new ParticipantNodeX(
            participantId,
            arguments.metrics,
            config,
            parameterConfig,
            storage,
            clock,
            crypto.pureCrypto,
            topologyDispatcher,
            ips,
            sync,
            adminToken,
            recordSequencerInteractions,
            replaySequencerConfig,
            schedulers,
            loggerFactory,
            healthService.dependencies.map(_.toComponentStatus),
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
      }.map { node =>
        setInitialized()
        node
      }
    }
  }

  override protected def member(uid: UniqueIdentifier): Member = ParticipantId(uid)

  override protected def mkNodeHealthService(storage: Storage): HealthService =
    HealthService(
      "participant",
      logger,
      timeouts,
      criticalDependencies = Seq(storage),
      // The sync service won't be reporting Ok until the node is initialized, but that shouldn't prevent traffic from
      // reaching the node
      Seq(
        syncDomainHealth,
        syncDomainEphemeralHealth,
        syncDomainSequencerClientHealth,
        syncDomainAcsCommitmentProcessorHealth,
      ),
    )

  override protected def setPostInitCallbacks(
      sync: CantonSyncService
  ): Unit = {
    // TODO(#14048) implement me

  }
}

object ParticipantNodeBootstrapX {

  object CommunityParticipantFactory
      extends CommunityParticipantFactoryCommon[ParticipantNodeBootstrapX] {

    override protected def createEngine(arguments: Arguments): Engine =
      super.createEngine(arguments)

    override protected def createNode(
        arguments: Arguments,
        engine: Engine,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): ParticipantNodeBootstrapX = {
      new ParticipantNodeBootstrapX(
        arguments,
        createEngine(arguments),
        CantonSyncService.DefaultFactory,
        (_ledgerApi, _ledgerApiDependentServices) => (),
        createResourceService(arguments),
        createReplicationServiceFactory(arguments),
        persistentStateFactory = ParticipantNodePersistentStateFactory,
        ledgerApiServerFactory = ledgerApiServerFactory,
        setInitialized = () => (),
      )
    }
  }
}

class ParticipantNodeX(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: LocalParticipantConfig,
    val nodeParameters: ParticipantNodeParameters,
    storage: Storage,
    override protected val clock: Clock,
    override val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcher,
    private[canton] val ips: IdentityProvidingServiceClient,
    override private[canton] val sync: CantonSyncService,
    val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val schedulers: SchedulersWithParticipantPruning,
    val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends ParticipantNodeCommon(sync) {

  override def close(): Unit = () // closing is done in the bootstrap class

  def readyDomains: Map[DomainId, Boolean] =
    sync.readyDomains.values.toMap

  override def status: Future[ParticipantStatus] = {
    val ports = Map("ledger" -> config.ledgerApi.port, "admin" -> config.adminApi.port)
    val domains = readyDomains
    val topologyQueues = identityPusher.queueStatus
    Future.successful(
      ParticipantStatus(
        id.uid,
        uptime(),
        ports,
        domains,
        sync.isActive(),
        topologyQueues,
        healthData,
      )
    )
  }

  override def isActive: Boolean = storage.isActive
}

object ParticipantNodeX {}
