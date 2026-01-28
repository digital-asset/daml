// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import cats.Eval
import com.daml.executors.InstrumentedExecutors
import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.ledger.api.v2.experimental_features.ExperimentalCommandInspectionService
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.version_service.OffsetCheckpointFeature
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.entries.LoggingEntries
import com.daml.tracing.{DefaultOpenTelemetry, Telemetry}
import com.digitalasset.canton.auth.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.NonNegativeDurationConverter.NonNegativeDurationToMillisConverter
import com.digitalasset.canton.config.{AdminTokenConfig, ApiLoggingConfig, ProcessingTimeout}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.{HttpApiServer, JsonApiConfig}
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  IdentityProviderId,
  ParticipantAuthorizationFormat,
  TopologyFormat,
  UpdateFormat,
  User,
  UserRight,
}
import com.digitalasset.canton.ledger.localstore.*
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.ledger.participant.state.metrics.TimedSyncService
import com.digitalasset.canton.ledger.participant.state.{InternalIndexService, PackageSyncService}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.LifeCycle.FastCloseableChannel
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.ratelimiting.ActiveRequestCounterInterceptor
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcRequestLoggingInterceptor}
import com.digitalasset.canton.participant.config.{
  LedgerApiServerConfig,
  ParticipantNodeConfig,
  TestingTimeServiceConfig,
}
import com.digitalasset.canton.participant.store.{
  ParticipantNodePersistentState,
  ParticipantPruningStore,
  PruningOffsetServiceImpl,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.{
  LedgerApiServerBootstrapUtils,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.ratelimiting.{
  RateLimitingInterceptorFactory,
  ThreadpoolCheck,
}
import com.digitalasset.canton.platform.apiserver.services.ApiContractService
import com.digitalasset.canton.platform.apiserver.services.admin.Utils
import com.digitalasset.canton.platform.apiserver.{
  ApiServiceOwner,
  InProcessGrpcName,
  LedgerFeatures,
  TimeServiceBackend,
}
import com.digitalasset.canton.platform.config.{
  IdentityProviderManagementConfig,
  IndexServiceConfig,
}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.platform.store.dao.events.{ContractLoader, LfValueTranslation}
import com.digitalasset.canton.platform.store.{
  DbSupport,
  LedgerApiContractStore,
  LedgerApiContractStoreImpl,
}
import com.digitalasset.canton.platform.{
  PackagePreferenceBackend,
  ResourceCloseable,
  ResourceOwnerFlagCloseableOps,
  ResourceOwnerOps,
}
import com.digitalasset.canton.time.{Clock, RemoteClock, SimClock}
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.{LedgerParticipantId, LfPackageId, LfPartyId, config}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.{BindableService, ServerInterceptor, ServerServiceDefinition}
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

class LedgerApiServer(
    serverConfig: LedgerApiServerConfig,
    jsonApiConfig: JsonApiConfig,
    participantId: LedgerParticipantId,
    adminParty: Party,
    adminTokenConfig: AdminTokenConfig,
    engine: Engine,
    syncService: CantonSyncService,
    cantonParameterConfig: ParticipantNodeParameters,
    testingTimeService: Option[TimeServiceBackend],
    adminTokenDispenser: CantonAdminTokenDispenser,
    participantContractStore: Eval[LedgerApiContractStore],
    participantPruningStore: Eval[ParticipantPruningStore],
    enableCommandInspection: Boolean,
    tracerProvider: TracerProvider,
    grpcApiMetrics: LedgerApiServerMetrics,
    jsonApiMetrics: HttpApiMetrics,
    maxDeduplicationDuration: config.NonNegativeFiniteDuration,
    clock: Clock,
    telemetry: Telemetry,
    commandProgressTracker: CommandProgressTracker,
    ledgerApiStore: Eval[LedgerApiStore],
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    actorSystem: ActorSystem,
    tracer: Tracer,
) extends ResourceCloseable
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = cantonParameterConfig.processingTimeouts

  /** Constructs the Ledger API server resource owner. On acquisition of this resource, the server
    * is started. Assumes that no other instance is currently running.
    */
  private def owner()(implicit traceContext: TraceContext): ResourceOwner[LedgerApiServer] = {
    logger.info("Starting ledger API server.")
    for {
      _ <- buildLedgerApiServerOwner()
      _ <- ResourceOwner.forReleasable(() => syncService) { syncService =>
        syncService.unregisterInternalIndexService()
        Future.unit
      }
    } yield this
  }

  private def buildLedgerApiServerOwner()(implicit
      traceContext: TraceContext
  ): ResourceOwner[LedgerApiServer] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    val indexServiceConfig = serverConfig.indexService
    val authServices =
      if (serverConfig.authServices.isEmpty)
        List(AuthServiceWildcard)
      else
        Seq[AuthService](
          new CantonAdminTokenAuthService(
            adminTokenDispenser,
            Some(adminParty),
            adminTokenConfig,
          )
        ) ++
          serverConfig.authServices.map(
            _.create(
              serverConfig.jwksCacheConfig,
              serverConfig.jwtTimestampLeeway,
              loggerFactory,
              serverConfig.maxTokenLifetime,
            )
          )

    val jwtVerifierLoader =
      new CachedJwtVerifierLoader(
        cacheMaxSize = serverConfig.jwksCacheConfig.cacheMaxSize,
        cacheExpiration = serverConfig.jwksCacheConfig.cacheExpiration.underlying,
        connectionTimeout = serverConfig.jwksCacheConfig.connectionTimeout.underlying,
        readTimeout = serverConfig.jwksCacheConfig.readTimeout.underlying,
        jwtTimestampLeeway = serverConfig.jwtTimestampLeeway,
        maxTokenLife = serverConfig.maxTokenLifetime.toMillisOrNone(),
        metrics = Some(grpcApiMetrics.identityProviderConfigStore.verifierCache),
        loggerFactory = loggerFactory,
      )

    val apiInfoService = new GrpcApiInfoService(CantonGrpcUtil.ApiName.LedgerApi)
      with BindableService {
      override def bindService(): ServerServiceDefinition =
        ApiInfoServiceGrpc.bindService(this, executionContext)
    }
    val dbSupport = ledgerApiStore.value.ledgerApiDbSupport
    val inMemoryState = ledgerApiIndexer.value.inMemoryState
    val timedSyncService = new TimedSyncService(syncService, grpcApiMetrics)
    logger.debug(
      s"Ledger API Server is initializing with ledgerApiStore=$ledgerApiStore, ledgerApiIndexer=$ledgerApiIndexer, dbSupport=$dbSupport, inMemoryState=$inMemoryState"
    )
    for {
      contractLoader <- {
        import cantonParameterConfig.ledgerApiServerParameters.contractLoader.*
        ContractLoader
          .create(
            participantContractStore = participantContractStore.value,
            contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
              inMemoryState.stringInterningView,
              inMemoryState.ledgerEndCache,
            ),
            dbDispatcher = dbSupport.dbDispatcher,
            metrics = grpcApiMetrics,
            maxQueueSize = maxQueueSize.value,
            maxBatchSize = maxBatchSize.value,
            parallelism = parallelism.value,
            loggerFactory = loggerFactory,
          )
          .afterReleased(logger.info("ContractLoader released"))
      }
      queryExecutionContext <- ResourceOwner
        .forExecutorService(() =>
          InstrumentedExecutors.newWorkStealingExecutor(
            grpcApiMetrics.lapi.threadpool.apiQueryServices.toString,
            indexServiceConfig.apiQueryServicesThreadPoolSize.getOrElse(
              IndexServiceConfig.DefaultQueryServicesThreadPoolSize(noTracingLogger)
            ),
          )
        )
        .afterReleased(logger.info("ReadApiServiceExecutionContext released"))
      packagePreferenceBackend = new PackagePreferenceBackend(
        clock = clock,
        adminParty = LfPartyId.assertFromString(participantId),
        syncService = timedSyncService,
        loggerFactory = loggerFactory,
      )
      lfValueTranslation = new LfValueTranslation(
        metrics = grpcApiMetrics,
        engineO = Some(engine),
        loadPackage = (packageId, loggingContext) =>
          timedSyncService.getLfArchive(packageId)(loggingContext.traceContext),
        loggerFactory = loggerFactory,
      )
      indexService <- new IndexServiceOwner(
        dbSupport = dbSupport,
        config = indexServiceConfig,
        participantId = participantId,
        metrics = grpcApiMetrics,
        inMemoryState = inMemoryState,
        tracer = tracerProvider.tracer,
        loggerFactory = loggerFactory,
        incompleteOffsets = (off, ps, tc) =>
          timedSyncService.incompleteReassignmentOffsets(off, ps.getOrElse(Set.empty))(tc),
        contractLoader = contractLoader,
        getPackageMetadataSnapshot = timedSyncService.getPackageMetadataSnapshot(_),
        lfValueTranslation = lfValueTranslation,
        queryExecutionContext = queryExecutionContext,
        commandExecutionContext = executionContext,
        getPackagePreference = (
            packageName,
            candidatePackageIds,
            candidatePackageIdsRestrictionDescription,
            loggingContext,
        ) =>
          packagePreferenceBackend
            .getPreferredPackageVersionForParticipant(
              packageName,
              candidatePackageIds,
              candidatePackageIdsRestrictionDescription,
            )(
              loggingContext
            ),
        participantContractStore = participantContractStore.value,
        pruningOffsetService =
          PruningOffsetServiceImpl(participantPruningStore.value, loggerFactory),
      )
      _ = timedSyncService.registerInternalIndexService(new InternalIndexService {
        override def activeContracts(
            partyIds: Set[LfPartyId],
            validAt: Option[Offset],
        )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
          indexService.getActiveContracts(
            eventFormat = EventFormat(
              filtersByParty =
                partyIds.view.map(_ -> CumulativeFilter.templateWildcardFilter(true)).toMap,
              filtersForAnyParty =
                Option.when(partyIds.isEmpty)(CumulativeFilter.templateWildcardFilter(true)),
              verbose = false,
            ),
            activeAt = validAt,
          )(new LoggingContextWithTrace(LoggingEntries.empty, traceContext))

        override def topologyTransactions(
            partyId: LfPartyId,
            fromExclusive: Offset,
        )(implicit traceContext: TraceContext): Source[TopologyTransaction, NotUsed] =
          indexService
            .updates(
              begin = Some(fromExclusive),
              endAt = None,
              updateFormat = UpdateFormat(
                includeTransactions = None,
                includeReassignments = None,
                includeTopologyEvents = Some(
                  TopologyFormat(
                    participantAuthorizationFormat = Some(
                      ParticipantAuthorizationFormat(
                        parties = Some(Set(partyId))
                      )
                    )
                  )
                ),
              ),
            )
            .mapConcat(_.update.topologyTransaction)
      })
      userManagementStore = getUserManagementStore(dbSupport, loggerFactory)
      partyRecordStore = new PersistentPartyRecordStore(
        dbSupport = dbSupport,
        metrics = grpcApiMetrics,
        timeProvider = TimeProvider.UTC,
        executionContext = executionContext,
        loggerFactory = loggerFactory,
      )

      packageLoader = new DeduplicatingPackageLoader()
      packageResolver: PackageResolver = (packageId: LfPackageId) =>
        (traceContext: TraceContext) =>
          FutureUnlessShutdown.outcomeF(
            packageLoader.loadPackage(
              packageId = packageId,
              delegate = packageId => timedSyncService.getLfArchive(packageId)(traceContext),
              metric = grpcApiMetrics.index.db.translation.getLfPackage,
            )
          )

      contractValidator = ContractValidator(syncService.pureCryptoApi, engine, packageResolver)

      // TODO(i21582) The prepare endpoint of the interactive submission service does not suffix
      // contract IDs of the transaction yet. This means enrichment of the transaction may fail
      // when processing unsuffixed contract IDs. For that reason we disable this requirement via the flag below.
      // When CIDs are suffixed, we can re-use the LfValueTranslation from the index service created above
      interactiveSubmissionEnricher = new InteractiveSubmissionEnricher(
        new Engine(engine.config.copy(forbidLocalContractIds = false)),
        packageResolver = packageResolver,
      )
      apiContractService = new ApiContractService(
        ledgerApiContractStore = participantContractStore.value,
        lfValueTranslation = lfValueTranslation,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
      )
      (_, authInterceptor) <- ApiServiceOwner(
        indexService = indexService,
        transactionSubmissionTracker = inMemoryState.transactionSubmissionTracker,
        reassignmentSubmissionTracker = inMemoryState.reassignmentSubmissionTracker,
        partyAllocationTracker = inMemoryState.partyAllocationTracker,
        commandProgressTracker = commandProgressTracker,
        userManagementStore = userManagementStore,
        identityProviderConfigStore = getIdentityProviderConfigStore(
          dbSupport,
          serverConfig.identityProviderManagement,
          loggerFactory,
        ),
        partyRecordStore = partyRecordStore,
        participantId = participantId,
        command = serverConfig.commandService,
        managementServiceTimeout = serverConfig.managementServiceTimeout,
        userManagement = serverConfig.userManagementService,
        partyManagementServiceConfig = serverConfig.partyManagementService,
        packageServiceConfig = serverConfig.packageService,
        tls = serverConfig.tls,
        address = Some(serverConfig.address),
        maxInboundMessageSize = serverConfig.maxInboundMessageSize.unwrap,
        maxInboundMetadataSize = serverConfig.maxInboundMetadataSize.unwrap,
        port = serverConfig.port,
        seeding = cantonParameterConfig.ledgerApiServerParameters.contractIdSeeding,
        syncService = timedSyncService,
        healthChecks = new HealthChecks(
          // TODO(i21015): Possible issues with health check reporting: disconnected sequencer can be reported as healthy; possibly reporting protocol processing/CantonSyncService general health needed
          "write" -> (() => syncService.currentWriteHealth()),
          "indexer" -> ledgerApiIndexer.value.indexerHealth,
        ),
        metrics = grpcApiMetrics,
        timeServiceBackend = testingTimeService,
        otherServices = Seq(apiInfoService),
        otherInterceptors = getInterceptors(dbSupport.dbDispatcher.executor),
        engine = engine,
        queryExecutionContext = queryExecutionContext,
        commandExecutionContext = executionContext,
        checkOverloaded = syncService.checkOverloaded,
        ledgerFeatures = getLedgerFeatures,
        maxDeduplicationDuration = maxDeduplicationDuration,
        authServices = authServices,
        jwtVerifierLoader = jwtVerifierLoader,
        jwtTimestampLeeway = serverConfig.jwtTimestampLeeway,
        tokenExpiryGracePeriodForStreams =
          cantonParameterConfig.ledgerApiServerParameters.tokenExpiryGracePeriodForStreams,
        engineLoggingConfig = cantonParameterConfig.engine.submissionPhaseLogging,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        contractAuthenticator = contractValidator.authenticateHash,
        dynParamGetter = syncService.dynamicSynchronizerParameterGetter,
        interactiveSubmissionServiceConfig = serverConfig.interactiveSubmissionService,
        interactiveSubmissionEnricher = interactiveSubmissionEnricher,
        keepAlive = serverConfig.keepAliveServer,
        packagePreferenceBackend = packagePreferenceBackend,
        apiLoggingConfig = cantonParameterConfig.loggingConfig.api,
        apiContractService = apiContractService,
      )
      _ <- startHttpApiIfEnabled(
        timedSyncService,
        authInterceptor,
        packagePreferenceBackend,
        cantonParameterConfig.loggingConfig.api,
      )
      _ <- serverConfig.userManagementService.additionalAdminUserId
        .fold(ResourceOwner.unit) { rawUserId =>
          ResourceOwner.forFuture { () =>
            createExtraAdminUser(rawUserId, userManagementStore)
          }
        }
    } yield this
  }

  private def getIdentityProviderConfigStore(
      dbSupport: DbSupport,
      identityProviderManagement: IdentityProviderManagementConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): CachedIdentityProviderConfigStore =
    PersistentIdentityProviderConfigStore.cached(
      dbSupport = dbSupport,
      metrics = grpcApiMetrics,
      cacheExpiryAfterWrite = identityProviderManagement.cacheExpiryAfterWrite.underlying,
      maxIdentityProviders = IdentityProviderManagementConfig.MaxIdentityProviders,
      loggerFactory = loggerFactory,
    )

  private def getUserManagementStore(dbSupport: DbSupport, loggerFactory: NamedLoggerFactory)(
      implicit traceContext: TraceContext
  ): UserManagementStore =
    PersistentUserManagementStore.cached(
      dbSupport = dbSupport,
      metrics = grpcApiMetrics,
      timeProvider = TimeProvider.UTC,
      cacheExpiryAfterWriteInSeconds =
        serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
      maxCacheSize = serverConfig.userManagementService.maxCacheSize,
      maxRightsPerUser = serverConfig.userManagementService.maxRightsPerUser,
      loggerFactory = loggerFactory,
      flagCloseable = this,
    )(executionContext, traceContext)

  private def createExtraAdminUser(rawUserId: String, userManagementStore: UserManagementStore)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
    val userId = Ref.UserId.assertFromString(rawUserId)
    userManagementStore
      .createUser(
        user = User(
          id = userId,
          primaryParty = None,
          identityProviderId = IdentityProviderId.Default,
        ),
        rights = Set(UserRight.ParticipantAdmin),
      )
      .flatMap {
        case Left(UserManagementStore.UserExists(_)) =>
          logger.info(
            s"Creating admin user with id $userId failed. User with this id already exists"
          )
          Future.unit
        case other =>
          Utils.handleResult("creating extra admin user")(other).map(_ => ())
      }
  }

  private def getInterceptors(
      indexDbExecutor: Option[QueueAwareExecutor & NamedExecutor]
  ): List[ServerInterceptor] = List(
    new GrpcRequestLoggingInterceptor(
      loggerFactory,
      cantonParameterConfig.loggingConfig.api,
    ),
    GrpcTelemetry
      .builder(tracerProvider.openTelemetry)
      .build()
      .newServerInterceptor(),
  ) ::: (serverConfig.rateLimit
    .map(rateLimit =>
      RateLimitingInterceptorFactory.create(
        loggerFactory = loggerFactory,
        config = rateLimit,
        additionalChecks = List(
          ThreadpoolCheck(
            name = "Environment Execution Threadpool",
            limit = rateLimit.maxApiServicesQueueSize,
            queue = executionContext,
            loggerFactory = loggerFactory,
          )
        ) ++ indexDbExecutor.map(executor =>
          ThreadpoolCheck(
            name = "Index DB Threadpool",
            limit = rateLimit.maxApiServicesIndexDbQueueSize,
            queue = executor,
            loggerFactory = loggerFactory,
          )
        ),
      )
    )
    .toList) ::: (serverConfig.limits
    .map(cfg =>
      new ActiveRequestCounterInterceptor(
        "ledger-api",
        cfg.active,
        cfg.warnOnUndefinedLimits,
        cfg.throttleLoggingRatePerSecond,
        grpcApiMetrics.requests,
        loggerFactory,
      )
    )
    .toList)

  private def getLedgerFeatures: LedgerFeatures = LedgerFeatures(
    staticTime = testingTimeService.isDefined,
    commandInspectionService =
      ExperimentalCommandInspectionService.of(supported = enableCommandInspection),
    offsetCheckpointFeature = OffsetCheckpointFeature.of(
      maxOffsetCheckpointEmissionDelay = Some(
        (serverConfig.indexService.offsetCheckpointCacheUpdateInterval + serverConfig.indexService.idleStreamOffsetCheckpointTimeout).toProtoPrimitive
      )
    ),
    topologyAwarePackageSelection = serverConfig.topologyAwarePackageSelection.enabled,
  )

  private def startHttpApiIfEnabled(
      packageSyncService: PackageSyncService,
      authInterceptor: AuthInterceptor,
      packagePreferenceBackend: PackagePreferenceBackend,
      apiLoggingConfig: ApiLoggingConfig,
  ): ResourceOwner[Unit] =
    if (!jsonApiConfig.enabled)
      ResourceOwner.unit
    else
      for {
        channel <- ResourceOwner
          .forReleasable(() =>
            InProcessChannelBuilder
              .forName(InProcessGrpcName.forPort(serverConfig.clientConfig.port))
              .executor(executionContext.execute(_))
              .build()
          )(channel =>
            Future(
              new FastCloseableChannel(channel, logger, "JSON-API").close()
            )
          )
          .afterReleased(noTracingLogger.info("JSON-API gRPC channel is released"))
        _ <- HttpApiServer(
          jsonApiConfig,
          serverConfig.tls,
          channel,
          packageSyncService,
          loggerFactory,
          authInterceptor,
          packagePreferenceBackend = packagePreferenceBackend,
          apiLoggingConfig,
        )(
          jsonApiMetrics
        ).afterReleased(noTracingLogger.info("JSON-API HTTP Server is released"))
      } yield ()
}

object LedgerApiServer {
  def initialize(
      adminParty: Party,
      adminTokenDispenser: CantonAdminTokenDispenser,
      commandProgressTracker: CommandProgressTracker,
      config: ParticipantNodeConfig,
      httpApiMetrics: HttpApiMetrics,
      ledgerApiServerBootstrapUtils: LedgerApiServerBootstrapUtils,
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
      name: InstanceName,
      parameters: ParticipantNodeParameters,
      participantId: LedgerParticipantId,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      sync: CantonSyncService,
      tracerProvider: TracerProvider,
  )(implicit
      actorSystem: ActorSystem,
      executionContext: ExecutionContextIdlenessExecutorService,
      tracer: Tracer,
      traceContext: TraceContext,
  ): Future[LedgerApiServer] = {
    val telemetry = new DefaultOpenTelemetry(tracerProvider.openTelemetry)

    val ledgerTestingTimeService = (config.testingTime, ledgerApiServerBootstrapUtils.clock) match {
      case (Some(TestingTimeServiceConfig.MonotonicTime), clock) =>
        Some(
          new CantonTimeServiceBackend(
            clock,
            ledgerApiServerBootstrapUtils.testingTimeService,
            loggerFactory,
          )
        )
      case (_clockNotAdvanceableThroughLedgerApi, simClock: SimClock) =>
        Some(new CantonExternalClockBackend(simClock, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, remoteClock: RemoteClock) =>
        Some(new CantonExternalClockBackend(remoteClock, loggerFactory))
      case _ => None
    }
    val ledgerApiServerOwner = new LedgerApiServer(
      serverConfig = config.ledgerApi,
      jsonApiConfig = config.httpLedgerApi,
      participantId = participantId,
      adminParty = adminParty,
      adminTokenConfig = config.ledgerApi.adminTokenConfig.merge(config.adminApi.adminTokenConfig),
      engine = ledgerApiServerBootstrapUtils.engine,
      syncService = sync,
      cantonParameterConfig = parameters,
      testingTimeService = ledgerTestingTimeService,
      adminTokenDispenser = adminTokenDispenser,
      participantContractStore = participantNodePersistentState.map(state =>
        LedgerApiContractStoreImpl(state.contractStore, loggerFactory, metrics)
      ),
      participantPruningStore = participantNodePersistentState.map(_.pruningStore),
      enableCommandInspection = config.ledgerApi.enableCommandInspection,
      tracerProvider = tracerProvider,
      grpcApiMetrics = metrics,
      jsonApiMetrics = httpApiMetrics,
      maxDeduplicationDuration = participantNodePersistentState
        .map(_.settingsStore.settings.maxDeduplicationDuration)
        .value
        .getOrElse(
          throw new IllegalArgumentException(s"Unknown maxDeduplicationDuration")
        )
        .toConfig,
      clock = ledgerApiServerBootstrapUtils.clock,
      telemetry = telemetry,
      commandProgressTracker = commandProgressTracker,
      ledgerApiStore = participantNodePersistentState.map(_.ledgerApiStore),
      ledgerApiIndexer = ledgerApiIndexer,
      loggerFactory = loggerFactory,
    ).owner()
    new ResourceOwnerFlagCloseableOps(ledgerApiServerOwner)
      .acquireFlagCloseable("Ledger API Server")
      .recoverWith { err =>
        val failure =
          // The MigrateOnEmptySchema exception is private, thus match on the expected message
          if (err.getMessage.contains("migrate-on-empty-schema"))
            new RuntimeException(s"Please run `$name.db.migrate` to apply pending migrations", err)
          else err

        Future.failed(failure)
      }
  }

  sealed trait LedgerApiServerError extends Product with Serializable with PrettyPrinting {
    protected def errorMessage: String = ""
    def cause: Throwable
    def asRuntimeException(additionalMessage: String = ""): RuntimeException =
      new RuntimeException(
        if (additionalMessage.isEmpty) errorMessage else s"$additionalMessage $errorMessage",
        cause,
      )
  }

  sealed trait LedgerApiServerErrorWithoutCause extends LedgerApiServerError {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    override def cause: Throwable = null
  }

  final case class FailedToConfigureLedgerApiStorage(override protected val errorMessage: String)
      extends LedgerApiServerErrorWithoutCause {
    override protected def pretty: Pretty[FailedToConfigureLedgerApiStorage] =
      prettyOfClass(unnamedParam(_.errorMessage.unquoted))
  }
}
