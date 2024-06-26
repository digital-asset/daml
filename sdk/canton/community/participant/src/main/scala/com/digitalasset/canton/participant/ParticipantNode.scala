// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.CommunityGrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.{
  CommunityCryptoFactory,
  Crypto,
  CryptoPureApi,
  SyncCryptoApiProvider,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, StaticGrpcServices}
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.grpc.*
import com.digitalasset.canton.participant.admin.workflows.java.canton
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.domain.grpc.GrpcDomainRegistry
import com.digitalasset.canton.participant.domain.{DomainAliasManager, DomainAliasResolution}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.{
  IndexerLockIds,
  LedgerApiServerState,
}
import com.digitalasset.canton.participant.ledger.api.{
  CantonAdminToken,
  StartableStoppableLedgerApiDependentServices,
  StartableStoppableLedgerApiServer,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.scheduler.{
  ParticipantSchedulersParameters,
  SchedulersWithParticipantPruning,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  ParticipantTopologyManagerError,
  ParticipantTopologyManagerOps,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import com.digitalasset.canton.time.admin.v30.DomainTimeServiceGrpc
import com.digitalasset.canton.topology.TopologyManagerError.InvalidTopologyMapping
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{PartyMetadataStore, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseProtocolVersion,
}
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class ParticipantNodeBootstrap(
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
    packageServiceFactory: PackageServiceFactory,
    ledgerApiServerFactory: CantonLedgerApiServerFactory,
    setInitialized: () => Unit,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends CantonNodeBootstrapImpl[
      ParticipantNode,
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ](arguments) {

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

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[DomainTopologyClient] =
    storeId match {
      case DomainStore(domainId, _) =>
        cantonSyncService.get.flatMap(_.lookupTopologyClient(domainId))
      case _ => None
    }

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[ParticipantNode] =
    new StartupNode(storage, crypto, nodeId, manager, healthReporter, healthService)

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      topologyManager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[ParticipantNode, RunningNode[ParticipantNode]](
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

    private def createSyncDomainAndTopologyDispatcher(
        aliasResolution: DomainAliasResolution,
        indexedStringStore: IndexedStringStore,
    ): (SyncDomainPersistentStateManager, ParticipantTopologyDispatcher) = {
      val manager = new SyncDomainPersistentStateManager(
        participantId,
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
          config.topology,
          crypto,
          clock,
          config,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )

      (manager, topologyDispatcher)
    }

    private def createPackageOps(
        manager: SyncDomainPersistentStateManager
    ): PackageOps = {
      val authorizedTopologyStoreClient = new StoreBasedTopologySnapshot(
        CantonTimestamp.MaxValue,
        topologyManager.store,
        StoreBasedDomainTopologyClient.NoPackageDependencies,
        loggerFactory,
      )
      val packageOps = new PackageOpsImpl(
        participantId = participantId,
        headAuthorizedTopologySnapshot = authorizedTopologyStoreClient,
        stateManager = manager,
        topologyManager = topologyManager,
        nodeId = nodeId,
        initialProtocolVersion = ProtocolVersion.latest,
        loggerFactory = ParticipantNodeBootstrap.this.loggerFactory,
        timeouts = timeouts,
      )

      addCloseable(packageOps)
      packageOps
    }

    private val participantOps = new ParticipantTopologyManagerOps {
      override def allocateParty(
          validatedSubmissionId: CantonRequireTypes.String255,
          partyId: PartyId,
          participantId: ParticipantId,
          protocolVersion: ProtocolVersion,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = for {
        ptp <- EitherT.fromEither[FutureUnlessShutdown](
          PartyToParticipant
            .create(
              partyId,
              None,
              threshold = PositiveInt.one,
              participants =
                Seq(HostingParticipant(participantId, ParticipantPermission.Submission)),
              groupAddressing = false,
            )
            .leftMap(err =>
              ParticipantTopologyManagerError.IdentityManagerParentError(
                InvalidTopologyMapping.Reject(err)
              )
            )
        )
        // TODO(#14069) make this "extend" / not replace
        //    this will also be potentially racy!
        _ <- performUnlessClosingEitherUSF(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              TopologyChangeOp.Replace,
              ptp,
              serial = None,
              // TODO(#12390) auto-determine signing keys
              signingKeys = Seq(partyId.uid.namespace.fingerprint),
              protocolVersion,
              expectFullAuthorization = true,
            )
        )
          .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
          .map(_ => ())
      } yield ()

    }

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[ParticipantNode]]] = {
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

      // admin token is taken from the config or created per session
      val adminToken: CantonAdminToken = config.ledgerApi.adminToken.fold(
        CantonAdminToken.create(crypto.pureCrypto)
      )(token => CantonAdminToken(secret = token))

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
        packageServiceFactory,
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
        createSyncDomainAndTopologyDispatcher,
        createPackageOps,
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
          val node = new ParticipantNode(
            participantId,
            arguments.metrics,
            config,
            storage,
            clock,
            crypto.pureCrypto,
            topologyDispatcher,
            ips,
            sync,
            adminToken,
            recordSequencerInteractions,
            replaySequencerConfig,
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

  override protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService) = {
    val readiness = DependenciesHealthService(
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
    val liveness = LivenessHealthService.alwaysAlive(logger, timeouts)
    (readiness, liveness)
  }

  private def setPostInitCallbacks(
      sync: CantonSyncService
  ): Unit = {
    // TODO(#14048) implement me

  }

  override def config: LocalParticipantConfig = arguments.config

  /** If set to `Some(path)`, every sequencer client will record all received events to the directory `path`.
    */
  protected val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]] =
    new AtomicReference(None)
  protected val replaySequencerConfig: AtomicReference[Option[ReplayConfig]] = new AtomicReference(
    None
  )

  lazy val syncDomainHealth: MutableHealthComponent = MutableHealthComponent(
    loggerFactory,
    SyncDomain.healthName,
    timeouts,
  )
  lazy val syncDomainEphemeralHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      SyncDomainEphemeralState.healthName,
      timeouts,
    )
  lazy val syncDomainSequencerClientHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      SequencerClient.healthName,
      timeouts,
    )

  lazy val syncDomainAcsCommitmentProcessorHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      AcsCommitmentProcessor.healthName,
      timeouts,
    )

  protected def createParticipantServices(
      participantId: ParticipantId,
      crypto: Crypto,
      storage: Storage,
      persistentStateFactory: ParticipantNodePersistentStateFactory,
      packageServiceFactory: PackageServiceFactory,
      engine: Engine,
      ledgerApiServerFactory: CantonLedgerApiServerFactory,
      indexedStringStore: IndexedStringStore,
      cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
      setStartableStoppableLedgerApiAndCantonServices: (
          StartableStoppableLedgerApiServer,
          StartableStoppableLedgerApiDependentServices,
      ) => Unit,
      resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
      replicationServiceFactory: Storage => ServerServiceDefinition,
      createSchedulers: ParticipantSchedulersParameters => Future[SchedulersWithParticipantPruning],
      createPartyNotifierAndSubscribe: ParticipantEventPublisher => LedgerServerPartyNotifier,
      adminToken: CantonAdminToken,
      topologyManager: ParticipantTopologyManagerOps,
      packageDependencyResolver: PackageDependencyResolver,
      createSyncDomainAndTopologyDispatcher: (
          DomainAliasResolution,
          IndexedStringStore,
      ) => (SyncDomainPersistentStateManager, ParticipantTopologyDispatcher),
      createPackageOps: SyncDomainPersistentStateManager => PackageOps,
  )(implicit executionSequencerFactory: ExecutionSequencerFactory): EitherT[
    FutureUnlessShutdown,
    String,
    (
        LedgerServerPartyNotifier,
        CantonSyncService,
        ParticipantNodeEphemeralState,
        LedgerApiServerState,
        StartableStoppableLedgerApiDependentServices,
        SchedulersWithParticipantPruning,
        ParticipantTopologyDispatcher,
    ),
  ] = {
    val syncCrypto = new SyncCryptoApiProvider(
      participantId,
      ips,
      crypto,
      config.parameters.caching,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    // closed in DomainAliasManager
    val registeredDomainsStore = RegisteredDomainsStore(storage, timeouts, loggerFactory)

    for {
      domainConnectionConfigStore <- EitherT
        .right(
          DomainConnectionConfigStore.create(
            storage,
            ReleaseProtocolVersion.latest,
            timeouts,
            loggerFactory,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      domainAliasManager <- EitherT
        .right[String](
          DomainAliasManager
            .create(domainConnectionConfigStore, registeredDomainsStore, loggerFactory)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      (syncDomainPersistentStateManager, topologyDispatcher) =
        createSyncDomainAndTopologyDispatcher(
          domainAliasManager,
          indexedStringStore,
        )

      persistentState <- EitherT.right(
        persistentStateFactory.create(
          syncDomainPersistentStateManager,
          storage,
          clock,
          config.init.ledgerApi.maxDeduplicationDuration.toInternal.some,
          parameterConfig.batchingConfig,
          ReleaseProtocolVersion.latest,
          arguments.metrics,
          indexedStringStore,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
      )

      excludedPackageIds =
        if (parameters.excludeInfrastructureTransactions) {
          Set(
            canton.internal.ping.Ping.TEMPLATE_ID,
            canton.internal.bong.BongProposal.TEMPLATE_ID,
            canton.internal.bong.Bong.TEMPLATE_ID,
            canton.internal.bong.Merge.TEMPLATE_ID,
            canton.internal.bong.Explode.TEMPLATE_ID,
            canton.internal.bong.Collapse.TEMPLATE_ID,
          ).map(x => LfPackageId.assertFromString(x.getPackageId))
        } else {
          Set.empty[LfPackageId]
        }

      ephemeralState = ParticipantNodeEphemeralState(
        participantId,
        persistentState,
        clock,
        timeouts = parameterConfig.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

      packageService <- EitherT.right(
        packageServiceFactory.create(
          createAndInitialize = () =>
            PackageService.createAndInitialize(
              clock = clock,
              engine = engine,
              packageDependencyResolver = packageDependencyResolver,
              enableUpgradeValidation = !parameterConfig.disableUpgradeValidation,
              futureSupervisor = futureSupervisor,
              hashOps = syncCrypto.pureCrypto,
              loggerFactory = loggerFactory,
              metrics = arguments.metrics,
              packageMetadataViewConfig = config.parameters.packageMetadataView,
              packageOps = createPackageOps(syncDomainPersistentStateManager),
              timeouts = parameterConfig.processingTimeouts,
            )
        )
      )
      sequencerInfoLoader = new SequencerInfoLoader(
        parameterConfig.processingTimeouts,
        parameterConfig.tracing.propagation,
        ProtocolVersionCompatibility.supportedProtocolsParticipant(parameterConfig),
        parameterConfig.protocolConfig.minimumProtocolVersion,
        parameterConfig.protocolConfig.dontWarnOnDeprecatedPV,
        loggerFactory,
      )

      partyNotifier <- EitherT
        .rightT[Future, String](
          createPartyNotifierAndSubscribe(ephemeralState.participantEventPublisher)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      domainRegistry = new GrpcDomainRegistry(
        participantId,
        syncDomainPersistentStateManager,
        persistentState.map(_.settingsStore),
        topologyDispatcher,
        syncCrypto,
        config.crypto,
        clock,
        parameterConfig,
        domainAliasManager,
        arguments.testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        packageDependencyResolver,
        arguments.metrics.domainMetrics,
        sequencerInfoLoader,
        partyNotifier,
        futureSupervisor,
        loggerFactory,
      )

      syncDomainEphemeralStateFactory = new SyncDomainEphemeralStateFactoryImpl(
        parameterConfig.processingTimeouts,
        loggerFactory,
        futureSupervisor,
      )

      // Initialize the SyncDomain persistent states before participant recovery so that pruning recovery can re-invoke
      // an interrupted prune after a shutdown or crash, which touches the domain stores.
      _ <- EitherT
        .right[String](
          syncDomainPersistentStateManager.initializePersistentStates()
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      resourceManagementService = resourceManagementServiceFactory(
        persistentState.map(_.settingsStore)
      )

      schedulers <-
        EitherT
          .liftF(
            createSchedulers(
              ParticipantSchedulersParameters(
                isActive,
                persistentState.map(_.multiDomainEventLog),
                storage,
                adminToken,
                parameterConfig.stores,
                parameterConfig.batchingConfig,
              )
            )
          )
          .mapK(FutureUnlessShutdown.outcomeK)

      // Sync Service
      sync = cantonSyncServiceFactory.create(
        participantId,
        domainRegistry,
        domainConnectionConfigStore,
        domainAliasManager,
        persistentState,
        ephemeralState,
        syncDomainPersistentStateManager,
        packageService,
        topologyManager,
        topologyDispatcher,
        partyNotifier,
        syncCrypto,
        engine,
        syncDomainEphemeralStateFactory,
        storage,
        clock,
        resourceManagementService,
        parameterConfig,
        indexedStringStore,
        schedulers,
        arguments.metrics,
        sequencerInfoLoader,
        arguments.futureSupervisor,
        loggerFactory,
        arguments.testingConfig,
      )

      _ = {
        schedulers.setPruningProcessor(sync.pruningProcessor)
        setPostInitCallbacks(sync)
        syncDomainHealth.set(sync.syncDomainHealth)
        syncDomainEphemeralHealth.set(sync.ephemeralHealth)
        syncDomainSequencerClientHealth.set(sync.sequencerClientHealth)
        syncDomainAcsCommitmentProcessorHealth.set(sync.acsCommitmentProcessorHealth)
      }

      ledgerApiServer <- ledgerApiServerFactory
        .create(
          name,
          participantId = participantId.toLf,
          sync = sync,
          participantNodePersistentState = persistentState,
          arguments.config,
          arguments.parameterConfig,
          arguments.metrics.ledgerApiServer,
          arguments.metrics.httpApiServer,
          tracerProvider,
          adminToken,
          excludedPackageIds,
        )

    } yield {
      val ledgerApiDependentServices =
        new StartableStoppableLedgerApiDependentServices(
          config,
          parameterConfig,
          packageService,
          sync,
          participantId,
          clock,
          adminServerRegistry,
          adminToken,
          futureSupervisor,
          loggerFactory,
          tracerProvider,
        )

      setStartableStoppableLedgerApiAndCantonServices(
        ledgerApiServer.startableStoppableLedgerApi,
        ledgerApiDependentServices,
      )

      adminServerRegistry
        .addServiceU(
          TrafficControlServiceGrpc.bindService(
            new GrpcTrafficControlService(sync, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          PartyNameManagementServiceGrpc.bindService(
            new GrpcPartyNameManagementService(partyNotifier),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          DomainConnectivityServiceGrpc
            .bindService(
              new GrpcDomainConnectivityService(
                sync,
                domainAliasManager,
                parameterConfig.processingTimeouts,
                sequencerInfoLoader,
                loggerFactory,
              ),
              executionContext,
            )
        )
      adminServerRegistry
        .addServiceU(
          TransferServiceGrpc.bindService(
            new GrpcTransferService(sync.transferService, participantId, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          InspectionServiceGrpc.bindService(
            new GrpcInspectionService(sync.stateInspection),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          ResourceManagementServiceGrpc.bindService(
            new GrpcResourceManagementService(resourceManagementService, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          DomainTimeServiceGrpc.bindService(
            GrpcDomainTimeService.forParticipant(sync.lookupDomainTimeTracker, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry.addServiceU(replicationServiceFactory(storage))
      adminServerRegistry
        .addServiceU(
          PruningServiceGrpc.bindService(
            new GrpcPruningService(
              sync,
              () => schedulers.getPruningScheduler(loggerFactory),
              loggerFactory,
            ),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          ParticipantRepairServiceGrpc.bindService(
            new GrpcParticipantRepairService(
              sync,
              parameterConfig.processingTimeouts,
              loggerFactory,
            ),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          ApiInfoServiceGrpc.bindService(
            new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
            executionContext,
          )
        )
      // return values
      (
        partyNotifier,
        sync,
        ephemeralState,
        ledgerApiServer,
        ledgerApiDependentServices,
        schedulers,
        topologyDispatcher,
      )
    }
  }
}

object ParticipantNodeBootstrap {
  val LoggerFactoryKeyName: String = "participant"

  trait Factory[PC <: LocalParticipantConfig, B <: CantonNodeBootstrap[_]] {

    type Arguments =
      CantonNodeBootstrapCommonArguments[PC, ParticipantNodeParameters, ParticipantMetrics]

    protected def createEngine(arguments: Arguments): Engine

    protected def createResourceService(
        arguments: Arguments
    )(store: Eval[ParticipantSettingsStore]): ResourceManagementService

    protected def createLedgerApiServerFactory(
        arguments: Arguments,
        engine: Engine,
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        actorSystem: ActorSystem,
    ): CantonLedgerApiServerFactory

    def create(
        arguments: NodeFactoryArguments[PC, ParticipantNodeParameters, ParticipantMetrics],
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, B]
  }

  object CommunityParticipantFactory
      extends Factory[CommunityParticipantConfig, ParticipantNodeBootstrap] {

    override protected def createEngine(arguments: Arguments): Engine =
      DAMLe.newEngine(
        enableLfDev = arguments.parameterConfig.devVersionSupport,
        enableLfBeta = arguments.parameterConfig.betaVersionSupport,
        enableStackTraces = arguments.parameterConfig.engine.enableEngineStackTraces,
        iterationsBetweenInterruptions =
          arguments.parameterConfig.engine.iterationsBetweenInterruptions,
      )

    override protected def createResourceService(
        arguments: Arguments
    )(store: Eval[ParticipantSettingsStore]): ResourceManagementService =
      new ResourceManagementService.CommunityResourceManagementService(
        arguments.config.parameters.warnIfOverloadedFor.map(_.toInternal),
        arguments.metrics,
      )

    private def createReplicationServiceFactory(
        arguments: Arguments
    )(storage: Storage): ServerServiceDefinition =
      StaticGrpcServices
        .notSupportedByCommunity(
          EnterpriseParticipantReplicationServiceGrpc.SERVICE,
          arguments.loggerFactory,
        )

    override protected def createLedgerApiServerFactory(
        arguments: Arguments,
        engine: Engine,
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        actorSystem: ActorSystem,
    ): CantonLedgerApiServerFactory =
      new CantonLedgerApiServerFactory(
        engine = engine,
        clock = arguments.clock,
        testingTimeService = testingTimeService,
        allocateIndexerLockIds = _dbConfig => Option.empty[IndexerLockIds].asRight,
        meteringReportKey = CommunityKey,
        futureSupervisor = arguments.futureSupervisor,
        loggerFactory = arguments.loggerFactory,
      )

    override def create(
        arguments: NodeFactoryArguments[
          CommunityParticipantConfig,
          ParticipantNodeParameters,
          ParticipantMetrics,
        ],
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, ParticipantNodeBootstrap] =
      arguments
        .toCantonNodeBootstrapCommonArguments(
          new CommunityStorageFactory(arguments.config.storage),
          new CommunityCryptoFactory,
          new CommunityCryptoPrivateStoreFactory,
          new CommunityGrpcVaultServiceFactory,
        )
        .map { arguments =>
          val engine = createEngine(arguments)
          createNode(
            arguments,
            engine,
            createLedgerApiServerFactory(
              arguments,
              engine,
              testingTimeService,
            ),
          )
        }

    private def createNode(
        arguments: Arguments,
        engine: Engine,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): ParticipantNodeBootstrap = {
      new ParticipantNodeBootstrap(
        arguments,
        createEngine(arguments),
        CantonSyncService.DefaultFactory,
        (_ledgerApi, _ledgerApiDependentServices) => (),
        createResourceService(arguments),
        createReplicationServiceFactory(arguments),
        persistentStateFactory = ParticipantNodePersistentStateFactory,
        packageServiceFactory = PackageServiceFactory,
        ledgerApiServerFactory = ledgerApiServerFactory,
        setInitialized = () => (),
      )
    }
  }
}

class ParticipantNode(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: LocalParticipantConfig,
    storage: Storage,
    override protected val clock: Clock,
    val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcher,
    private[canton] val ips: IdentityProvidingServiceClient,
    private[canton] val sync: CantonSyncService,
    val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

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

  def reconnectDomainsIgnoreFailures()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    if (sync.isActive())
      sync.reconnectDomains(ignoreFailures = true).map(_ => ())
    else {
      logger.info("Not reconnecting to domains as instance is passive")
      EitherTUtil.unitUS
    }
  }
}
