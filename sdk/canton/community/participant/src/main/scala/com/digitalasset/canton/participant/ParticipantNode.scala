// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
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
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.ParticipantServices
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.grpc.*
import com.digitalasset.canton.participant.admin.workflows.java.canton
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.health.admin.ParticipantStatus
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.ledger.api.{
  AcsCommitmentPublicationPostProcessor,
  LedgerApiIndexer,
  LedgerApiIndexerConfig,
  StartableStoppableLedgerApiDependentServices,
  StartableStoppableLedgerApiServer,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.submission.{
  CommandDeduplicatorImpl,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  PruningProcessor,
  SortedReconciliationIntervalsProviderFactory,
}
import com.digitalasset.canton.participant.scheduler.ParticipantPruningScheduler
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.sync.SyncDomain.SubmissionReady
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.participant.synchronizer.grpc.GrpcDomainRegistry
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.scheduler.{Schedulers, SchedulersImpl}
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.*
import com.digitalasset.canton.time.admin.v30.DomainTimeServiceGrpc
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  StoreBasedTopologySnapshot,
  SynchronizerTopologyClient,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, SynchronizerStore}
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
    resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    ledgerApiServerFactory: CantonLedgerApiServerFactory,
    setInitialized: ParticipantServices => Unit,
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

  override protected def sequencedTopologyStores: Seq[TopologyStore[SynchronizerStore]] =
    cantonSyncService.get.toList.flatMap(_.syncDomainPersistentStateManager.getAll.values).collect {
      case s: SyncDomainPersistentState => s.topologyStore
    }

  override protected def sequencedTopologyManagers: Seq[SynchronizerTopologyManager] =
    cantonSyncService.get.toList.flatMap(_.syncDomainPersistentStateManager.getAll.values).collect {
      case s: SyncDomainPersistentState => s.topologyManager
    }

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[SynchronizerTopologyClient] =
    storeId match {
      case SynchronizerStore(synchronizerId, _) =>
        cantonSyncService.get.flatMap(_.lookupTopologyClient(synchronizerId))
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

    def acsInspectionPerDomain(): Map[SynchronizerId, AcsInspection] =
      cantonSyncService.get
        .map(_.syncDomainPersistentStateManager.getAll.map { case (synchronizerId, state) =>
          synchronizerId -> state.acsInspection
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
        engine,
        partyOps,
        topologyManager,
        tryGetPackageDependencyResolver(),
      ).map { participantServices =>
        if (cantonSyncService.putIfAbsent(participantServices.cantonSyncService).nonEmpty) {
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
          participantServices.participantTopologyDispatcher,
          participantServices.cantonSyncService,
          adminToken,
          recordSequencerInteractions,
          replaySequencerConfig,
          loggerFactory,
          healthService.dependencies.map(_.toComponentStatus),
        )
        addCloseable(node)
        setInitialized(participantServices)
        Some(new RunningNode(bootstrapStageCallback, node))
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
      )

      addCloseable(packageOps)
      packageOps
    }

    private def createParticipantServices(
        participantId: ParticipantId,
        crypto: Crypto,
        adminServerRegistry: CantonMutableHandlerRegistry,
        storage: Storage,
        engine: Engine,
        partyOps: PartyOps,
        authorizedTopologyManager: AuthorizedTopologyManager,
        packageDependencyResolver: PackageDependencyResolver,
    )(implicit executionSequencerFactory: ExecutionSequencerFactory): EitherT[
      FutureUnlessShutdown,
      String,
      ParticipantServices,
    ] = {

      val syncCrypto = new SyncCryptoApiProvider(
        participantId,
        ips,
        crypto,
        arguments.parameterConfig.sessionSigningKeys,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
      // closed in SynchronizerAliasManager
      val registeredDomainsStore = RegisteredDomainsStore(storage, timeouts, loggerFactory)
      val indexedStringStore = IndexedStringStore.create(
        storage,
        parameterConfig.cachingConfigs.indexedStrings,
        timeouts,
        loggerFactory,
      )

      for {
        synchronizerConnectionConfigStore <- EitherT
          .right(
            SynchronizerConnectionConfigStore.create(
              storage,
              ReleaseProtocolVersion.latest,
              timeouts,
              loggerFactory,
            )
          )

        synchronizerAliasManager <- EitherT
          .right[String](
            SynchronizerAliasManager
              .create(synchronizerConnectionConfigStore, registeredDomainsStore, loggerFactory)
          )

        persistentStateContainer = new LifeCycleContainer[ParticipantNodePersistentState](
          stateName = "persistent-state",
          create = () =>
            ParticipantNodePersistentState.create(
              storage,
              config.storage,
              config.init.ledgerApi.maxDeduplicationDuration.toInternal.some,
              parameters,
              ReleaseProtocolVersion.latest,
              arguments.metrics,
              participantId.toLf,
              config.ledgerApi,
              futureSupervisor,
              loggerFactory,
            ),
          loggerFactory = loggerFactory,
        )
        _ <- EitherT.right(persistentStateContainer.initializeNext())
        persistentState = persistentStateContainer.asEval

        syncDomainPersistentStateManager = new SyncDomainPersistentStateManager(
          participantId,
          synchronizerAliasManager,
          storage,
          indexedStringStore,
          persistentState.map(_.acsCounterParticipantConfigStore).value,
          parameters,
          crypto,
          clock,
          tryGetPackageDependencyResolver(),
          persistentState.map(_.ledgerApiStore),
          persistentState.map(_.contractStore),
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

        commandDeduplicator = new CommandDeduplicatorImpl(
          persistentState.map(_.commandDeduplicationStore),
          clock,
          persistentState.map(
            _.ledgerApiStore
              .ledgerEndCache()
              .map(_.lastPublicationTime)
              .getOrElse(CantonTimestamp.MinValue)
          ),
          loggerFactory,
        )

        inFlightSubmissionTracker = new InFlightSubmissionTracker(
          persistentState.map(_.inFlightSubmissionStore),
          commandDeduplicator,
          loggerFactory,
        )

        excludedPackageIds =
          if (parameters.excludeInfrastructureTransactions) {
            Set(
              canton.internal.ping.Ping.PACKAGE_ID,
              canton.internal.bong.BongProposal.PACKAGE_ID,
              canton.internal.bong.Bong.PACKAGE_ID,
              canton.internal.bong.Merge.PACKAGE_ID,
              canton.internal.bong.Explode.PACKAGE_ID,
              canton.internal.bong.Collapse.PACKAGE_ID,
            ).map(LfPackageId.assertFromString)
          } else {
            Set.empty[LfPackageId]
          }

        commandProgressTracker =
          if (parameters.commandProgressTracking.enabled)
            new CommandProgressTrackerImpl(parameters.commandProgressTracking, clock, loggerFactory)
          else CommandProgressTracker.NoOp

        connectedDomainsLookupContainer = new ConnectedDomainsLookupContainer
        sequentialPostProcessor = new AcsCommitmentPublicationPostProcessor(
          connectedDomainsLookupContainer,
          loggerFactory,
        )

        ledgerApiIndexerContainer = new LifeCycleContainer[LedgerApiIndexer](
          stateName = "indexer",
          create = () =>
            FutureUnlessShutdown.outcomeF(
              LedgerApiIndexer.initialize(
                metrics = arguments.metrics.ledgerApiServer,
                clock = clock,
                commandProgressTracker = commandProgressTracker,
                ledgerApiStore = persistentState.map(_.ledgerApiStore),
                ledgerApiIndexerConfig = LedgerApiIndexerConfig(
                  storageConfig = config.storage,
                  processingTimeout = parameterConfig.processingTimeouts,
                  serverConfig = config.ledgerApi,
                  indexerConfig = config.parameters.ledgerApiServer.indexer,
                  indexerHaConfig = ledgerApiServerFactory.createHaConfig(config),
                  ledgerParticipantId = participantId.toLf,
                  excludedPackageIds = excludedPackageIds,
                  onlyForTestingEnableInMemoryTransactionStore =
                    arguments.testingConfig.enableInMemoryTransactionStoreForParticipants,
                ),
                reassignmentOffsetPersistence = ReassignmentStore.reassignmentOffsetPersistenceFor(
                  syncDomainPersistentStateManager
                ),
                postProcessor = inFlightSubmissionTracker
                  .processPublications(_)(_)
                  .failOnShutdownTo(
                    // This will be throw in the Indexer pekko-stream pipeline, and handled gracefully there
                    new RuntimeException("Post processing aborted due to shutdown")
                  ),
                sequentialPostProcessor = sequentialPostProcessor,
                loggerFactory = loggerFactory,
              )
            ),
          loggerFactory = loggerFactory,
        )
        _ <- EitherT.right {
          // only initialize indexer if storage is available
          if (storage.isActive) {
            ledgerApiIndexerContainer.initializeNext()
          } else {
            logger.info("Ledger API Indexer is not initialized due to inactive storage")
            FutureUnlessShutdown.unit
          }
        }

        ephemeralState = ParticipantNodeEphemeralState(
          ledgerApiIndexerContainer.asEval,
          inFlightSubmissionTracker,
          clock,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          timeouts = parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )

        packageServiceContainer = new LifeCycleContainer[PackageService](
          stateName = "package-service",
          create = () =>
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
            ),
          loggerFactory = loggerFactory,
        )
        _ <- EitherT.right(packageServiceContainer.initializeNext())

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
          parameterConfig.batchingConfig.maxItemsInBatch,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )

        domainRegistry = new GrpcDomainRegistry(
          participantId,
          syncDomainPersistentStateManager,
          topologyDispatcher,
          syncCrypto,
          config.crypto,
          clock,
          parameterConfig,
          synchronizerAliasManager,
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

        resourceManagementService = resourceManagementServiceFactory(
          persistentState.map(_.settingsStore)
        )

        pruningProcessor = new PruningProcessor(
          persistentState,
          syncDomainPersistentStateManager,
          new SortedReconciliationIntervalsProviderFactory(
            syncDomainPersistentStateManager,
            futureSupervisor,
            loggerFactory,
          ),
          parameterConfig.batchingConfig.maxPruningBatchSize,
          arguments.metrics.pruning,
          exitOnFatalFailures = arguments.parameterConfig.exitOnFatalFailures,
          synchronizerId =>
            synchronizerAliasManager
              .aliasForSynchronizerId(synchronizerId)
              .flatMap(synchronizerAlias =>
                synchronizerConnectionConfigStore.get(synchronizerAlias).toOption.map(_.status)
              ),
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
        pruningScheduler = new ParticipantPruningScheduler(
          pruningProcessor,
          arguments.clock,
          arguments.metrics,
          arguments.config.ledgerApi.clientConfig,
          persistentState,
          storage,
          adminToken,
          parameterConfig.stores,
          arguments.parameterConfig.processingTimeouts,
          arguments.loggerFactory,
        )

        schedulers <-
          EitherT
            .liftF(
              {
                val schedulers =
                  new SchedulersImpl(
                    Map("pruning" -> pruningScheduler),
                    arguments.loggerFactory,
                  )
                if (isActive) {
                  schedulers.start().map(_ => schedulers)
                } else {
                  Future.successful(schedulers)
                }
              }
            )
            .mapK(FutureUnlessShutdown.outcomeK)

        // Sync Service
        sync = cantonSyncServiceFactory.create(
          participantId,
          domainRegistry,
          synchronizerConnectionConfigStore,
          synchronizerAliasManager,
          persistentState,
          ephemeralState,
          syncDomainPersistentStateManager,
          packageServiceContainer.asEval,
          partyOps,
          topologyDispatcher,
          partyNotifier,
          syncCrypto,
          engine,
          commandProgressTracker,
          syncDomainEphemeralStateFactory,
          storage,
          clock,
          resourceManagementService,
          parameterConfig,
          pruningProcessor,
          schedulers,
          arguments.metrics,
          exitOnFatalFailures = arguments.parameterConfig.exitOnFatalFailures,
          sequencerInfoLoader,
          arguments.futureSupervisor,
          loggerFactory,
          arguments.testingConfig,
          ledgerApiIndexerContainer,
          connectedDomainsLookupContainer,
        )

        _ = {
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
            ledgerApiIndexer = ledgerApiIndexerContainer.asEval,
            arguments.config,
            arguments.parameterConfig,
            arguments.metrics.ledgerApiServer,
            arguments.metrics.httpApiServer,
            tracerProvider,
            adminToken,
          )

      } yield {
        val ledgerApiDependentServices =
          new StartableStoppableLedgerApiDependentServices(
            config,
            parameterConfig,
            packageServiceContainer.asEval,
            sync,
            participantId,
            clock,
            adminServerRegistry,
            adminToken,
            futureSupervisor,
            loggerFactory,
            tracerProvider,
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
            SynchronizerConnectivityServiceGrpc
              .bindService(
                new GrpcSynchronizerConnectivityService(
                  sync,
                  synchronizerAliasManager,
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
                ips,
                indexedStringStore,
                synchronizerAliasManager,
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
              GrpcDomainTimeService
                .forParticipant(sync.lookupSynchronizerTimeTracker, loggerFactory),
              executionContext,
            )
          )
        adminServerRegistry.addServiceU(replicationServiceFactory(storage))
        adminServerRegistry
          .addServiceU(
            PruningServiceGrpc.bindService(
              new GrpcPruningService(
                participantId,
                sync,
                pruningScheduler,
                syncDomainPersistentStateManager,
                ips,
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
                parameterConfig,
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
        addCloseable(synchronizerConnectionConfigStore)
        addCloseable(synchronizerAliasManager)
        addCloseable(syncDomainPersistentStateManager)
        addCloseable(domainRegistry)
        addCloseable(partyMetadataStore)
        persistentState.map(addCloseable).discard
        addCloseable((() => packageServiceContainer.closeCurrent()): AutoCloseable)
        addCloseable(indexedStringStore)
        addCloseable(partyNotifier)
        addCloseable(ephemeralState.participantEventPublisher)
        addCloseable(topologyDispatcher)
        addCloseable(schedulers)
        addCloseable(ledgerApiServer)
        addCloseable(ledgerApiDependentServices)
        addCloseable(packageDependencyResolver)

        // return values
        ParticipantServices(
          persistentStateContainer = persistentStateContainer,
          packageServiceContainer = packageServiceContainer,
          ledgerApiIndexerContainer = ledgerApiIndexerContainer,
          cantonSyncService = sync,
          schedulers = schedulers,
          startableStoppableLedgerApiServer = ledgerApiServer.startableStoppableLedgerApi,
          startableStoppableLedgerApiDependentServices = ledgerApiDependentServices,
          participantTopologyDispatcher = topologyDispatcher,
        )
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
      criticalDependencies = storage +: crypto.toList,
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

  final case class ParticipantServices(
      persistentStateContainer: LifeCycleContainer[ParticipantNodePersistentState],
      packageServiceContainer: LifeCycleContainer[PackageService],
      ledgerApiIndexerContainer: LifeCycleContainer[LedgerApiIndexer],
      cantonSyncService: CantonSyncService,
      schedulers: Schedulers,
      startableStoppableLedgerApiServer: StartableStoppableLedgerApiServer,
      startableStoppableLedgerApiDependentServices: StartableStoppableLedgerApiDependentServices,
      participantTopologyDispatcher: ParticipantTopologyDispatcher,
  )

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
      new ResourceManagementService(
        store,
        arguments.config.parameters.warnIfOverloadedFor.map(_.toInternal),
        arguments.metrics,
      )

    private def createReplicationServiceFactory(
        arguments: Arguments
    ): ServerServiceDefinition =
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
        allocateIndexerLockIds = _ => Option.empty[IndexerLockIds].asRight,
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
        createResourceService(arguments),
        _ => createReplicationServiceFactory(arguments),
        ledgerApiServerFactory = ledgerApiServerFactory,
        setInitialized = _ => (),
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

  def readyDomains: Map[SynchronizerId, SubmissionReady] =
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
