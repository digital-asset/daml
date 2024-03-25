// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DbConfig, H2DbConfig}
import com.digitalasset.canton.crypto.{Crypto, SyncCryptoApiProvider}
import com.digitalasset.canton.domain.api.v0.DomainTimeServiceGrpc
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrapCommon}
import com.digitalasset.canton.health.MutableHealthComponent
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics as LedgerApiServerMetrics
import com.digitalasset.canton.participant.admin.grpc.*
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.admin.{
  DomainConnectivityService,
  PackageDependencyResolver,
  PackageOps,
  PackageService,
  MutablePackageNameMapResolver,
  ResourceManagementService,
}
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.domain.grpc.GrpcDomainRegistry
import com.digitalasset.canton.participant.domain.{
  AgreementService,
  DomainAliasManager,
  DomainAliasResolution,
}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.{
  IndexerLockIds,
  LedgerApiServerState,
}
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.scheduler.{
  ParticipantSchedulersParameters,
  SchedulersWithParticipantPruning,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcherCommon,
  ParticipantTopologyManagerOps,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseProtocolVersion}
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class CantonLedgerApiServerFactory(
    engine: Engine,
    clock: Clock,
    testingTimeService: TestingTimeService,
    allocateIndexerLockIds: DbConfig => Either[String, Option[IndexerLockIds]],
    meteringReportKey: MeteringReportKey,
    val multiDomainEnabled: Boolean,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  def create(
      name: InstanceName,
      ledgerId: String,
      participantId: LedgerParticipantId,
      sync: CantonSyncService,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      config: LocalParticipantConfig,
      parameters: ParticipantNodeParameters,
      metrics: LedgerApiServerMetrics,
      httpApiMetrics: HttpApiMetrics,
      tracerProvider: TracerProvider,
      adminToken: CantonAdminToken,
      packageNameMapResolver: MutablePackageNameMapResolver,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, String, CantonLedgerApiServerWrapper.LedgerApiServerState] = {

    val ledgerTestingTimeService = (config.testingTime, clock) match {
      case (Some(TestingTimeServiceConfig.MonotonicTime), clock) =>
        Some(new CantonTimeServiceBackend(clock, testingTimeService, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, simClock: SimClock) =>
        Some(new CantonExternalClockBackend(simClock, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, remoteClock: RemoteClock) =>
        Some(new CantonExternalClockBackend(remoteClock, loggerFactory))
      case _ => None
    }

    for {
      // For participants with append-only schema enabled, we allocate lock IDs for the indexer
      indexerLockIds <-
        config.storage match {
          case _: H2DbConfig =>
            // For H2 the non-unique indexer lock ids are sufficient.
            logger.debug("Not allocating indexer lock IDs on H2 config")
            EitherT.rightT[FutureUnlessShutdown, String](None)
          case dbConfig: DbConfig =>
            allocateIndexerLockIds(dbConfig)
              .leftMap { err =>
                s"Failed to allocated lock IDs for indexer: $err"
              }
              .toEitherT[FutureUnlessShutdown]
          case _ =>
            logger.debug("Not allocating indexer lock IDs on non-DB config")
            EitherT.rightT[FutureUnlessShutdown, String](None)
        }

      indexerHaConfig = indexerLockIds.fold(HaConfig()) {
        case IndexerLockIds(mainLockId, workerLockId) =>
          HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
      }

      ledgerApiServer <- CantonLedgerApiServerWrapper
        .initialize(
          CantonLedgerApiServerWrapper.Config(
            serverConfig = config.ledgerApi,
            jsonApiConfig = config.httpLedgerApiExperimental.map(_.toConfig),
            indexerConfig = parameters.ledgerApiServerParameters.indexer,
            indexerHaConfig = indexerHaConfig,
            ledgerId = ledgerId,
            participantId = participantId,
            engine = engine,
            syncService = sync,
            storageConfig = config.storage,
            cantonParameterConfig = parameters,
            testingTimeService = ledgerTestingTimeService,
            adminToken = adminToken,
            loggerFactory = loggerFactory,
            tracerProvider = tracerProvider,
            metrics = metrics,
            jsonApiMetrics = httpApiMetrics,
            meteringReportKey = meteringReportKey,
          ),
          // start ledger API server iff participant replica is active
          startLedgerApiServer = sync.isActive(),
          futureSupervisor = futureSupervisor,
          multiDomainEnabled = multiDomainEnabled,
          packageNameMapResolver = packageNameMapResolver,
        )(executionContext, actorSystem)
        .leftMap { err =>
          // The MigrateOnEmptySchema exception is private, thus match on the expected message
          val errMsg =
            if (
              Option(err.cause).nonEmpty && err.cause.getMessage.contains("migrate-on-empty-schema")
            )
              s"${err.cause.getMessage} Please run `$name.db.migrate` to apply pending migrations"
            else s"$err"
          s"Ledger API server failed to start: $errMsg"
        }
    } yield ledgerApiServer
  }
}

/** custom components that differ between x-nodes and 2.x nodes */
private[this] trait ParticipantComponentBootstrapFactory {

  def createSyncDomainAndTopologyDispatcher(
      aliasResolution: DomainAliasResolution,
      indexedStringStore: IndexedStringStore,
  ): (SyncDomainPersistentStateManager, ParticipantTopologyDispatcherCommon)

  def createPackageOps(
      manager: SyncDomainPersistentStateManager,
      crypto: SyncCryptoApiProvider,
  ): PackageOps

}

trait ParticipantNodeBootstrapCommon {
  this: CantonNodeBootstrapCommon[
    _,
    LocalParticipantConfig,
    ParticipantNodeParameters,
    ParticipantMetrics,
  ] =>

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

  lazy val syncAcsCommitmentProcessorHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      AcsCommitmentProcessor.healthName,
      timeouts,
    )

  protected def setPostInitCallbacks(sync: CantonSyncService): Unit

  protected def createParticipantServices(
      participantId: ParticipantId,
      crypto: Crypto,
      storage: Storage,
      persistentStateFactory: ParticipantNodePersistentStateFactory,
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
      componentFactory: ParticipantComponentBootstrapFactory,
      skipRecipientsCheck: Boolean,
      overrideKeyUniqueness: Option[Boolean] = None, // TODO(i13235) remove when UCK is gone
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
        ParticipantTopologyDispatcherCommon,
    ),
  ] = {
    val syncCrypto = new SyncCryptoApiProvider(
      participantId,
      ips,
      crypto,
      config.caching,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    // closed in DomainAliasManager
    val registeredDomainsStore = RegisteredDomainsStore(storage, timeouts, loggerFactory)

    // closed in grpc domain registry
    val agreementService = {
      // store is cleaned up as part of the agreement service
      val acceptedAgreements = ServiceAgreementStore(storage, timeouts, loggerFactory)
      new AgreementService(acceptedAgreements, parameterConfig, loggerFactory)
    }

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
        componentFactory.createSyncDomainAndTopologyDispatcher(
          domainAliasManager,
          indexedStringStore,
        )

      persistentState <- EitherT.right(
        persistentStateFactory.create(
          syncDomainPersistentStateManager,
          storage,
          clock,
          config.init.ledgerApi.maxDeduplicationDuration.toInternal.some,
          overrideKeyUniqueness.orElse(config.init.parameters.uniqueContractKeys.some),
          parameterConfig.batchingConfig,
          parameterConfig.stores,
          ReleaseProtocolVersion.latest,
          arguments.metrics,
          indexedStringStore,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
      )

      ephemeralState = ParticipantNodeEphemeralState(
        participantId,
        persistentState,
        clock,
        maxDeduplicationDuration = persistentState.map(
          _.settingsStore.settings.maxDeduplicationDuration
            .getOrElse(
              ErrorUtil.internalError(
                new RuntimeException("Max deduplication duration is not available")
              )
            )
        ),
        timeouts = parameterConfig.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

      // TODO(#17635): Remove this inverse dependency between the PackageService and the LedgerAPI IndexService
      //               with the unification of the Ledger API and Admin API package services.
      //               This is a temporary solution for allowing exposure of the package-map contained in the [[InMemoryState]]
      //               to the Admin API PackageService for package upload validation.
      packageNameMapResolver = new MutablePackageNameMapResolver()
      // Package Store and Management
      packageService =
        new PackageService(
          engine,
          packageDependencyResolver,
          ephemeralState.participantEventPublisher,
          syncCrypto.pureCrypto,
          componentFactory.createPackageOps(syncDomainPersistentStateManager, syncCrypto),
          arguments.metrics,
          parameterConfig.disableUpgradeValidation,
          packageNameMapResolver,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )

      sequencerInfoLoader = new SequencerInfoLoader(
        parameterConfig.processingTimeouts,
        parameterConfig.tracing.propagation,
        ProtocolVersionCompatibility.supportedProtocolsParticipant(parameterConfig),
        parameterConfig.protocolConfig.minimumProtocolVersion,
        parameterConfig.protocolConfig.dontWarnOnDeprecatedPV,
        loggerFactory,
      )

      domainRegistry = new GrpcDomainRegistry(
        participantId,
        syncDomainPersistentStateManager,
        persistentState.map(_.settingsStore),
        agreementService,
        topologyDispatcher,
        syncCrypto,
        config.crypto,
        clock,
        parameterConfig,
        domainAliasManager,
        arguments.testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        packageId => packageDependencyResolver.packageDependencies(List(packageId)),
        arguments.metrics.domainMetrics,
        sequencerInfoLoader,
        futureSupervisor,
        loggerFactory,
      )

      syncDomainEphemeralStateFactory = new SyncDomainEphemeralStateFactoryImpl(
        parameterConfig.processingTimeouts,
        loggerFactory,
        futureSupervisor,
      )

      partyNotifier <- EitherT
        .rightT[Future, String](
          createPartyNotifierAndSubscribe(ephemeralState.participantEventPublisher)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      // Initialize the SyncDomain persistent states before participant recovery so that pruning recovery can re-invoke
      // an interrupted prune after a shutdown or crash, which touches the domain stores.
      _ <- EitherT
        .right[String](
          syncDomainPersistentStateManager.initializePersistentStates()
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      ledgerId = participantId.uid.id.unwrap

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
        skipRecipientsCheck,
        multiDomainLedgerAPIEnabled = ledgerApiServerFactory.multiDomainEnabled,
      )

      _ = {
        schedulers.setPruningProcessor(sync.pruningProcessor)
        setPostInitCallbacks(sync)
        syncDomainHealth.set(sync.syncDomainHealth)
        syncDomainEphemeralHealth.set(sync.ephemeralHealth)
        syncDomainSequencerClientHealth.set(sync.sequencerClientHealth)
        syncAcsCommitmentProcessorHealth.set(sync.acsCommitmentProcessorHealth)
      }

      ledgerApiServer <- ledgerApiServerFactory
        .create(
          name,
          ledgerId = ledgerId,
          participantId = participantId.toLf,
          sync = sync,
          participantNodePersistentState = persistentState,
          arguments.config,
          arguments.parameterConfig,
          arguments.metrics.ledgerApiServer,
          arguments.metrics.httpApiServer,
          tracerProvider,
          adminToken,
          packageNameMapResolver,
        )

    } yield {
      val ledgerApiDependentServices =
        new StartableStoppableLedgerApiDependentServices(
          config,
          parameterConfig,
          packageService,
          sync,
          participantId,
          syncCrypto.pureCrypto,
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

      val stateService = new DomainConnectivityService(
        sync,
        domainAliasManager,
        agreementService,
        parameterConfig.processingTimeouts,
        sequencerInfoLoader,
        loggerFactory,
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
            .bindService(new GrpcDomainConnectivityService(stateService), executionContext)
        )
      adminServerRegistry
        .addServiceU(
          TransferServiceGrpc.bindService(
            new GrpcTransferService(sync.transferService, participantId),
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

abstract class ParticipantNodeCommon(
    private[canton] val sync: CantonSyncService
) extends CantonNode
    with NamedLogging
    with HasUptime {
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
