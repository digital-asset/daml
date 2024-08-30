// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
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
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  CantonMutableHandlerRegistry,
  StaticGrpcServices,
}
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.grpc.*
import com.digitalasset.canton.participant.admin.workflows.java.canton
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.domain.grpc.GrpcDomainRegistry
import com.digitalasset.canton.participant.health.admin.ParticipantStatus
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.ledger.api.{
  StartableStoppableLedgerApiDependentServices,
  StartableStoppableLedgerApiServer,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.submission.{
  CommandDeduplicatorImpl,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.scheduler.{
  ParticipantSchedulersParameters,
  SchedulersWithParticipantPruning,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncDomain.SubmissionReady
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  PackageOps,
  PackageOpsImpl,
  ParticipantTopologyDispatcher,
  ParticipantTopologyValidation,
  PartyOps,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import com.digitalasset.canton.time.admin.v30.DomainTimeServiceGrpc
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.{PartyMetadataStore, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseProtocolVersion,
  ReleaseVersion,
}
import com.digitalasset.daml.lf.engine.Engine
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

  private val cantonSyncService = new SingleUseCell[CantonSyncService]
  private val packageDependencyResolver = new SingleUseCell[PackageDependencyResolver]

  override protected val adminTokenConfig: Option[String] =
    config.ledgerApi.adminToken.orElse(config.adminApi.adminToken)

  private def tryGetPackageDependencyResolver(): PackageDependencyResolver =
    packageDependencyResolver.getOrElse(
      sys.error("packageDependencyResolver should be defined")
    )

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
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[ParticipantNode] =
    new StartupNode(
      storage,
      crypto,
      adminServerRegistry,
      adminToken,
      nodeId,
      manager,
      healthService,
    )

  override protected def createAuthorizedTopologyManager(
      nodeId: UniqueIdentifier,
      crypto: Crypto,
      authorizedStore: TopologyStore[AuthorizedStore],
      storage: Storage,
  ): AuthorizedTopologyManager = {
    val resolver = new PackageDependencyResolver(
      DamlPackageStore(
        storage,
        arguments.futureSupervisor,
        arguments.parameterConfig,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        loggerFactory,
      ),
      arguments.parameterConfig.processingTimeouts,
      loggerFactory,
    )

    val _ = packageDependencyResolver.putIfAbsent(resolver)

    def acsInspectionPerDomain(): Map[DomainId, AcsInspection] =
      cantonSyncService.get
        .map(_.syncDomainPersistentStateManager.getAll.map { case (domainId, state) =>
          domainId -> state.acsInspection
        })
        .getOrElse(Map.empty)

    val topologyManager = new AuthorizedTopologyManager(
      nodeId,
      clock,
      crypto,
      authorizedStore,
      exitOnFatalFailures = parameters.exitOnFatalFailures,
      bootstrapStageCallback.timeouts,
      futureSupervisor,
      bootstrapStageCallback.loggerFactory,
    ) with ParticipantTopologyValidation {

      override def validatePackageVetting(
          currentlyVettedPackages: Set[LfPackageId],
          nextPackageIds: Set[LfPackageId],
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        validatePackageVetting(
          currentlyVettedPackages,
          nextPackageIds,
          resolver,
          acsInspections = () => acsInspectionPerDomain(),
          forceFlags,
        )

      override def checkCannotDisablePartyWithActiveContracts(
          partyId: PartyId,
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        checkCannotDisablePartyWithActiveContracts(
          partyId,
          forceFlags,
          () => acsInspectionPerDomain(),
        )

    }
    topologyManager
  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      nodeId: UniqueIdentifier,
      topologyManager: AuthorizedTopologyManager,
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[ParticipantNode, RunningNode[ParticipantNode]](
        description = "Startup participant node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    override def getAdminToken: Option[String] = Some(adminToken.secret)
    private val participantId = ParticipantId(nodeId)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[ParticipantNode]]] = {

      val partyOps = new PartyOps(topologyManager, loggerFactory)
      createParticipantServices(
        participantId,
        crypto,
        adminServerRegistry,
        storage,
        persistentStateFactory,
        packageServiceFactory,
        engine,
        partyOps,
        topologyManager,
        tryGetPackageDependencyResolver(),
      ).map { case (sync, topologyDispatcher) =>
        if (cantonSyncService.putIfAbsent(sync).nonEmpty) {
          sys.error("should not happen")
        }
        val node = new ParticipantNode(
          participantId,
          arguments.metrics,
          config,
          parameters,
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

    private def createPackageOps(
        manager: SyncDomainPersistentStateManager
    ): PackageOps = {
      val authorizedTopologyStoreClient = new StoreBasedTopologySnapshot(
        CantonTimestamp.MaxValue,
        topologyManager.store,
        tryGetPackageDependencyResolver(),
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
        futureSupervisor = futureSupervisor,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
      )

      addCloseable(packageOps)
      packageOps
    }

    private def createParticipantServices(
        participantId: ParticipantId,
        crypto: Crypto,
        adminServerRegistry: CantonMutableHandlerRegistry,
        storage: Storage,
        persistentStateFactory: ParticipantNodePersistentStateFactory,
        packageServiceFactory: PackageServiceFactory,
        engine: Engine,
        partyOps: PartyOps,
        authorizedTopologyManager: AuthorizedTopologyManager,
        packageDependencyResolver: PackageDependencyResolver,
    )(implicit executionSequencerFactory: ExecutionSequencerFactory): EitherT[
      FutureUnlessShutdown,
      String,
      (
          CantonSyncService,
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
      val indexedStringStore = IndexedStringStore.create(
        storage,
        parameterConfig.cachingConfigs.indexedStrings,
        timeouts,
        loggerFactory,
      )

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

        syncDomainPersistentStateManager = new SyncDomainPersistentStateManager(
          participantId,
          domainAliasManager,
          storage,
          indexedStringStore,
          parameters,
          crypto,
          clock,
          tryGetPackageDependencyResolver(),
          futureSupervisor,
          loggerFactory,
        )

        topologyDispatcher = new ParticipantTopologyDispatcher(
          authorizedTopologyManager,
          participantId,
          syncDomainPersistentStateManager,
          config.topology,
          crypto,
          clock,
          config,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )

        persistentState <- EitherT.right(
          persistentStateFactory.create(
            syncDomainPersistentStateManager,
            storage,
            config.storage,
            exitOnFatalFailures = parameters.exitOnFatalFailures,
            clock,
            config.init.ledgerApi.maxDeduplicationDuration.toInternal.some,
            parameterConfig.batchingConfig,
            ReleaseProtocolVersion.latest,
            arguments.metrics,
            participantId.toLf,
            config.ledgerApi,
            indexedStringStore,
            parameterConfig.processingTimeouts,
            futureSupervisor,
            loggerFactory,
          )
        )

        commandDeduplicator = new CommandDeduplicatorImpl(
          persistentState.map(_.commandDeduplicationStore),
          clock,
          persistentState.flatMap(mdel =>
            Eval.always(mdel.multiDomainEventLog.publicationTimeLowerBound)
          ),
          loggerFactory,
        )

        inFlightSubmissionTracker = new InFlightSubmissionTracker(
          persistentState.map(_.inFlightSubmissionStore),
          commandDeduplicator,
          persistentState.map(_.multiDomainEventLog),
          timeouts,
          loggerFactory,
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
          inFlightSubmissionTracker,
          clock,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
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
                exitOnFatalFailures = parameters.exitOnFatalFailures,
                packageMetadataViewConfig = config.parameters.packageMetadataView,
                packageOps = createPackageOps(syncDomainPersistentStateManager),
                timeouts = parameterConfig.processingTimeouts,
              )
          )
        )

        sequencerInfoLoader = new SequencerInfoLoader(
          parameterConfig.processingTimeouts,
          parameterConfig.tracing.propagation,
          ProtocolVersionCompatibility.supportedProtocols(parameterConfig),
          parameterConfig.protocolConfig.minimumProtocolVersion,
          parameterConfig.protocolConfig.dontWarnOnDeprecatedPV,
          loggerFactory,
        )

        partyMetadataStore =
          PartyMetadataStore(storage, parameterConfig.processingTimeouts, loggerFactory)

        partyNotifier = new LedgerServerPartyNotifier(
          participantId,
          ephemeralState.participantEventPublisher,
          partyMetadataStore,
          clock,
          arguments.futureSupervisor,
          mustTrackSubmissionIds = true,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )

        // Notify at participant level if eager notification is configured, else rely on notification via domain.
        _ = if (parameterConfig.partyChangeNotification == PartyNotificationConfig.Eager) {
          authorizedTopologyManager.addObserver(partyNotifier.attachToIdentityManager())
        }

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
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          parameterConfig.processingTimeouts,
          loggerFactory,
          futureSupervisor,
          clock,
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
          partyOps,
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
          exitOnFatalFailures = arguments.parameterConfig.exitOnFatalFailures,
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
            TransferStore.reassignmentOffsetPersistenceFor(syncDomainPersistentStateManager),
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
            InspectionServiceGrpc.bindService(
              new GrpcInspectionService(
                sync.stateInspection,
                indexedStringStore,
                domainAliasManager,
                loggerFactory,
              ),
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

        addCloseable(sync)
        addCloseable(domainConnectionConfigStore)
        addCloseable(domainAliasManager)
        addCloseable(syncDomainPersistentStateManager)
        addCloseable(domainRegistry)
        addCloseable(inFlightSubmissionTracker)
        addCloseable(partyMetadataStore)
        persistentState.map(addCloseable).discard
        packageService.map(addCloseable).discard
        addCloseable(indexedStringStore)
        addCloseable(partyNotifier)
        addCloseable(ephemeralState.participantEventPublisher)
        addCloseable(ephemeralState.inFlightSubmissionTracker)
        addCloseable(topologyDispatcher)
        addCloseable(schedulers)
        addCloseable(ledgerApiServer)
        addCloseable(ledgerApiDependentServices)
        addCloseable(packageDependencyResolver)

        // return values
        (sync, topologyDispatcher)
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

  override protected def bindNodeStatusService(): ServerServiceDefinition =
    ParticipantStatusServiceGrpc.bindService(
      new GrpcParticipantStatusService(getNodeStatus, loggerFactory),
      executionContext,
    )

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
        enableLfDev = arguments.parameterConfig.alphaVersionSupport,
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
            createLedgerApiServerFactory(
              arguments,
              engine,
              testingTimeService,
            ),
          )
        }

    private def createNode(
        arguments: Arguments,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): ParticipantNodeBootstrap =
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

class ParticipantNode(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: LocalParticipantConfig,
    nodeParameters: ParticipantNodeParameters,
    val storage: Storage,
    override protected val clock: Clock,
    val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcher,
    private[canton] val ips: IdentityProvidingServiceClient,
    private[canton] val sync: CantonSyncService,
    override val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  override type Status = ParticipantStatus

  override def close(): Unit = () // closing is done in the bootstrap class

  def readyDomains: Map[DomainId, SubmissionReady] =
    sync.readyDomains.values.toMap

  private def supportedProtocolVersions: Seq[ProtocolVersion] = {
    val supportedPvs = ProtocolVersionCompatibility.supportedProtocols(nodeParameters)
    nodeParameters.protocolConfig.minimumProtocolVersion match {
      case Some(pv) => supportedPvs.filter(p => p >= pv)
      case None => supportedPvs
    }
  }

  override def status: ParticipantStatus = {
    val ports = Map("ledger" -> config.ledgerApi.port, "admin" -> config.adminApi.port)
    val domains = readyDomains
    val topologyQueues = identityPusher.queueStatus

    ParticipantStatus(
      id.uid,
      uptime(),
      ports,
      domains,
      sync.isActive(),
      topologyQueues,
      healthData,
      version = ReleaseVersion.current,
      supportedProtocolVersions = supportedProtocolVersions,
    )

  }

  override def isActive: Boolean = storage.isActive

  def reconnectDomainsIgnoreFailures()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    if (sync.isActive())
      sync.reconnectDomains(ignoreFailures = true).map(_ => ())
    else {
      logger.info("Not reconnecting to domains as instance is passive")
      EitherTUtil.unitUS
    }
}
