// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import cats.Eval
import com.daml.executors.InstrumentedExecutors
import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.ledger.api.v2.experimental_features.ExperimentalCommandInspectionService
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.version_service.OffsetCheckpointFeature
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.entries.LoggingEntries
import com.daml.nameof.NameOf.functionFullName
import com.daml.tracing.Telemetry
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.auth.{AuthServiceWildcard, CantonAdminTokenAuthService}
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.{InProcessGrpcName, ProcessingTimeout}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.http.HttpApiServer
import com.digitalasset.canton.ledger.api.auth.CachedJwtVerifierLoader
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  IdentityProviderId,
  User,
  UserRight,
}
import com.digitalasset.canton.ledger.localstore.*
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.ledger.participant.state.metrics.TimedSyncService
import com.digitalasset.canton.ledger.participant.state.{InternalStateService, PackageSyncService}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.LifeCycle.FastCloseableChannel
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{ApiRequestLogger, CantonGrpcUtil}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.protocol.ContractAuthenticator
import com.digitalasset.canton.platform.ResourceOwnerOps
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.ratelimiting.{
  RateLimitingInterceptor,
  ThreadpoolCheck,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiUserManagementService
import com.digitalasset.canton.platform.apiserver.{ApiServiceOwner, LedgerFeatures}
import com.digitalasset.canton.platform.config.{
  IdentityProviderManagementConfig,
  IndexServiceConfig,
}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.dao.events.{ContractLoader, LfValueTranslation}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.{BindableService, ServerInterceptor, ServerServiceDefinition}
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** The StartableStoppableLedgerApi enables a canton participant node to start and stop the ledger
  * API server depending on whether the participant node is a High Availability active or passive
  * replica.
  *
  * @param config
  *   ledger api server configuration
  * @param executionContext
  *   the execution context
  */
class StartableStoppableLedgerApiServer(
    config: CantonLedgerApiServerWrapper.Config,
    telemetry: Telemetry,
    futureSupervisor: FutureSupervisor,
    parameters: ParticipantNodeParameters,
    commandProgressTracker: CommandProgressTracker,
    ledgerApiStore: Eval[LedgerApiStore],
    ledgerApiIndexer: Eval[LedgerApiIndexer],
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    actorSystem: ActorSystem,
    tracer: Tracer,
) extends FlagCloseableAsync
    with NamedLogging {

  // Use a simple execution queue as locking to ensure only one start and stop run at a time.
  private val execQueue = new SimpleExecutionQueue(
    "start-stop-ledger-api-server-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = parameters.exitOnFatalFailures,
  )

  private val ledgerApiResource = new AtomicReference[Option[Resource[Unit]]](None)

  override protected def loggerFactory: NamedLoggerFactory = config.loggerFactory
  override protected def timeouts: ProcessingTimeout =
    config.cantonParameterConfig.processingTimeouts

  /** Start the ledger API server and remember the resource.
    *
    * Assumes that ledger api is currently stopped erroring otherwise. If asked to start during
    * shutdown ignores start.
    *
    * A possible improvement to consider in the future is to abort start upon subsequent call to
    * stop. As is the stop will wait until an inflight start completes.
    */
  def start()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    execQueue.execute(
      performUnlessClosingF(functionFullName) {
        ledgerApiResource.get match {
          case Some(_) =>
            logger.info(
              "Attempt to start ledger API server, but ledger API server already started. Ignoring."
            )
            Future.unit
          case None =>
            logger.info(
              "Starting ledger API server."
            )
            val ledgerApiServerResource =
              buildLedgerApiServerOwner().acquire()(
                ResourceContext(executionContext)
              )
            FutureUtil.logOnFailure(
              ledgerApiServerResource.asFuture.map { _ =>
                ledgerApiResource.set(Some(ledgerApiServerResource))
              },
              "Failed to start ledger API server",
            )
        }
      }.onShutdown {
        logger.info("Not starting ledger API server as we're shutting down")
      },
      "start ledger API server",
    )

  /** Stops the ledger API server, e.g. upon shutdown or when participant becomes passive.
    */
  def stop()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    execQueue.execute(
      ledgerApiResource.get match {
        case Some(ledgerApiServerToStop) =>
          logger.info("Stopping ledger API server")
          config.syncService.unregisterInternalStateService()
          FutureUtil.logOnFailure(
            ledgerApiServerToStop.release().map { _ =>
              logger.info("Successfully stopped ledger API server")
              ledgerApiResource.set(None)
            },
            "Failed to stop ledger API server",
          )
        case None =>
          logger.info("ledger API server already stopped")
          Future.unit
      },
      "stop ledger API server",
    )

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      logger.info("Shutting down ledger API server")
      Seq(
        AsyncCloseable("ledger API server", stop().unwrap, timeouts.shutdownNetwork),
        SyncCloseable("ledger-api-server-queue", execQueue.close()),
      )
    }

  private def buildLedgerApiServerOwner(
  )(implicit traceContext: TraceContext) = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    val indexServiceConfig = config.serverConfig.indexService
    val authServices = new CantonAdminTokenAuthService(Some(config.adminToken)) +:
      (
        if (config.serverConfig.authServices.isEmpty)
          List(AuthServiceWildcard)
        else
          config.serverConfig.authServices.map(
            _.create(
              config.serverConfig.jwtTimestampLeeway,
              loggerFactory,
            )
          )
      )

    val jwtVerifierLoader =
      new CachedJwtVerifierLoader(metrics = config.metrics, loggerFactory = loggerFactory)

    val apiInfoService = new GrpcApiInfoService(CantonGrpcUtil.ApiName.LedgerApi)
      with BindableService {
      override def bindService(): ServerServiceDefinition =
        ApiInfoServiceGrpc.bindService(this, executionContext)
    }
    val dbSupport = ledgerApiStore.value.ledgerApiDbSupport
    val inMemoryState = ledgerApiIndexer.value.inMemoryState
    val timedSyncService = new TimedSyncService(config.syncService, config.metrics)
    logger.debug(
      s"Ledger API Server is initializing with ledgerApiStore=$ledgerApiStore, ledgerApiIndexer=$ledgerApiIndexer, dbSupport=$dbSupport, inMemoryState=$inMemoryState"
    )
    for {
      contractLoader <- {
        import config.cantonParameterConfig.ledgerApiServerParameters.contractLoader.*
        ContractLoader
          .create(
            contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
              inMemoryState.stringInterningView
            ),
            dbDispatcher = dbSupport.dbDispatcher,
            metrics = config.metrics,
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
            config.metrics.lapi.threadpool.apiQueryServices.toString,
            indexServiceConfig.apiQueryServicesThreadPoolSize.getOrElse(
              IndexServiceConfig.DefaultQueryServicesThreadPoolSize(noTracingLogger)
            ),
          )
        )
        .afterReleased(logger.info("ReadApiServiceExecutionContext released"))
      indexService <- new IndexServiceOwner(
        dbSupport = dbSupport,
        config = indexServiceConfig,
        participantId = config.participantId,
        metrics = config.metrics,
        inMemoryState = inMemoryState,
        tracer = config.tracerProvider.tracer,
        loggerFactory = loggerFactory,
        incompleteOffsets = (off, ps, tc) =>
          timedSyncService.incompleteReassignmentOffsets(off, ps.getOrElse(Set.empty))(tc),
        contractLoader = contractLoader,
        getPackageMetadataSnapshot = timedSyncService.getPackageMetadataSnapshot(_),
        lfValueTranslation = new LfValueTranslation(
          metrics = config.metrics,
          engineO = Some(config.engine),
          loadPackage = (packageId, loggingContext) =>
            timedSyncService.getLfArchive(packageId)(loggingContext.traceContext),
          resolvePackageName = timedSyncService.resolvePackageName,
          loggerFactory = loggerFactory,
        ),
        queryExecutionContext = queryExecutionContext,
        commandExecutionContext = executionContext,
      )
      _ = timedSyncService.registerInternalStateService(new InternalStateService {
        override def activeContracts(
            partyIds: Set[LfPartyId],
            validAt: Option[Offset],
        )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
          indexService.getActiveContracts(
            filter = EventFormat(
              filtersByParty =
                partyIds.view.map(_ -> CumulativeFilter.templateWildcardFilter(true)).toMap,
              filtersForAnyParty = None,
              verbose = false,
            ),
            activeAt = validAt,
          )(new LoggingContextWithTrace(LoggingEntries.empty, traceContext))
      })
      userManagementStore = getUserManagementStore(dbSupport, loggerFactory)
      partyRecordStore = new PersistentPartyRecordStore(
        dbSupport = dbSupport,
        metrics = config.metrics,
        timeProvider = TimeProvider.UTC,
        executionContext = executionContext,
        loggerFactory = loggerFactory,
      )
      contractAuthenticator = ContractAuthenticator(
        config.syncService.pureCryptoApi
      )

      // TODO(i21582) The prepare endpoint of the interactive submission service does not suffix
      // contract IDs of the transaction yet. This means enrichment of the transaction may fail
      // when processing unsuffixed contract IDs. For that reason we disable this requirement via the flag below.
      // When CIDs are suffixed, we can re-use the LfValueTranslation from the index service created above
      lfValueTranslationForInteractiveSubmission = new LfValueTranslation(
        metrics = config.metrics,
        engineO =
          Some(new Engine(config.engine.config.copy(requireSuffixedGlobalContractId = false))),
        loadPackage = (packageId, loggingContext) =>
          timedSyncService.getLfArchive(packageId)(loggingContext.traceContext),
        resolvePackageName = timedSyncService.resolvePackageName,
        loggerFactory = loggerFactory,
      )

      _ <- ApiServiceOwner(
        indexService = indexService,
        submissionTracker = inMemoryState.submissionTracker,
        partyAllocationTracker = inMemoryState.partyAllocationTracker,
        commandProgressTracker = commandProgressTracker,
        userManagementStore = userManagementStore,
        identityProviderConfigStore = getIdentityProviderConfigStore(
          dbSupport,
          config.serverConfig.identityProviderManagement,
          loggerFactory,
        ),
        partyRecordStore = partyRecordStore,
        participantId = config.participantId,
        command = config.serverConfig.commandService,
        managementServiceTimeout = config.serverConfig.managementServiceTimeout,
        userManagement = config.serverConfig.userManagementService,
        partyManagementServiceConfig = config.serverConfig.partyManagementService,
        tls = config.serverConfig.tls,
        address = Some(config.serverConfig.address),
        maxInboundMessageSize = config.serverConfig.maxInboundMessageSize.unwrap,
        port = config.serverConfig.port,
        seeding = config.cantonParameterConfig.ledgerApiServerParameters.contractIdSeeding,
        syncService = timedSyncService,
        healthChecks = new HealthChecks(
          // TODO(i21015): Possible issues with health check reporting: disconnected sequencer can be reported as healthy; possibly reporting protocol processing/CantonSyncService general health needed
          "write" -> (() => config.syncService.currentWriteHealth()),
          "indexer" -> ledgerApiIndexer.value.indexerHealth,
        ),
        metrics = config.metrics,
        timeServiceBackend = config.testingTimeService,
        otherServices = Seq(apiInfoService),
        otherInterceptors = getInterceptors(dbSupport.dbDispatcher.executor),
        engine = config.engine,
        queryExecutionContext = queryExecutionContext,
        commandExecutionContext = executionContext,
        checkOverloaded = config.syncService.checkOverloaded,
        ledgerFeatures = getLedgerFeatures,
        maxDeduplicationDuration = config.maxDeduplicationDuration,
        authServices = authServices,
        jwtVerifierLoader = jwtVerifierLoader,
        jwtTimestampLeeway = config.serverConfig.jwtTimestampLeeway,
        tokenExpiryGracePeriodForStreams =
          config.cantonParameterConfig.ledgerApiServerParameters.tokenExpiryGracePeriodForStreams,
        meteringReportKey = config.meteringReportKey,
        engineLoggingConfig = config.cantonParameterConfig.engine.submissionPhaseLogging,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        authenticateSerializableContract = contractAuthenticator.authenticateSerializable,
        authenticateFatContractInstance = contractAuthenticator.authenticateFat,
        dynParamGetter = config.syncService.dynamicSynchronizerParameterGetter,
        interactiveSubmissionServiceConfig = config.serverConfig.interactiveSubmissionService,
        lfValueTranslation = lfValueTranslationForInteractiveSubmission,
        keepAlive = config.serverConfig.keepAliveServer,
      )
      _ <- startHttpApiIfEnabled(timedSyncService)
      _ <- config.serverConfig.userManagementService.additionalAdminUserId
        .fold(ResourceOwner.unit) { rawUserId =>
          ResourceOwner.forFuture { () =>
            createExtraAdminUser(rawUserId, userManagementStore)
          }
        }

    } yield ()
  }

  private def getIdentityProviderConfigStore(
      dbSupport: DbSupport,
      identityProviderManagement: IdentityProviderManagementConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): CachedIdentityProviderConfigStore =
    PersistentIdentityProviderConfigStore.cached(
      dbSupport = dbSupport,
      metrics = config.metrics,
      cacheExpiryAfterWrite = identityProviderManagement.cacheExpiryAfterWrite.underlying,
      maxIdentityProviders = IdentityProviderManagementConfig.MaxIdentityProviders,
      loggerFactory = loggerFactory,
    )

  private def getUserManagementStore(dbSupport: DbSupport, loggerFactory: NamedLoggerFactory)(
      implicit traceContext: TraceContext
  ): UserManagementStore =
    PersistentUserManagementStore.cached(
      dbSupport = dbSupport,
      metrics = config.metrics,
      timeProvider = TimeProvider.UTC,
      cacheExpiryAfterWriteInSeconds =
        config.serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
      maxCacheSize = config.serverConfig.userManagementService.maxCacheSize,
      maxRightsPerUser = config.serverConfig.userManagementService.maxRightsPerUser,
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
          Future.successful(())
        case other =>
          ApiUserManagementService.handleResult("creating extra admin user")(other).map(_ => ())
      }
  }

  private def getInterceptors(
      indexDbExecutor: QueueAwareExecutor & NamedExecutor
  ): List[ServerInterceptor] = List(
    new ApiRequestLogger(
      config.loggerFactory,
      config.cantonParameterConfig.loggingConfig.api,
    ),
    GrpcTelemetry
      .builder(config.tracerProvider.openTelemetry)
      .build()
      .newServerInterceptor(),
  ) ::: config.serverConfig.rateLimit
    .map(rateLimit =>
      RateLimitingInterceptor(
        loggerFactory = loggerFactory,
        metrics = config.metrics,
        config = rateLimit,
        additionalChecks = List(
          ThreadpoolCheck(
            name = "Environment Execution Threadpool",
            limit = rateLimit.maxApiServicesQueueSize,
            queue = executionContext,
            loggerFactory = loggerFactory,
          ),
          ThreadpoolCheck(
            name = "Index DB Threadpool",
            limit = rateLimit.maxApiServicesIndexDbQueueSize,
            queue = indexDbExecutor,
            loggerFactory = loggerFactory,
          ),
        ),
      )
    )
    .toList

  private def getLedgerFeatures: LedgerFeatures = LedgerFeatures(
    staticTime = config.testingTimeService.isDefined,
    commandInspectionService =
      ExperimentalCommandInspectionService.of(supported = config.enableCommandInspection),
    offsetCheckpointFeature = OffsetCheckpointFeature.of(
      maxOffsetCheckpointEmissionDelay = Some(
        (config.serverConfig.indexService.offsetCheckpointCacheUpdateInterval + config.serverConfig.indexService.idleStreamOffsetCheckpointTimeout).toProtoPrimitive
      )
    ),
    topologyAwarePackageSelection = config.serverConfig.topologyAwarePackageSelection.enabled,
  )

  private def startHttpApiIfEnabled(packageSyncService: PackageSyncService): ResourceOwner[Unit] =
    config.jsonApiConfig
      .fold(ResourceOwner.unit) { jsonApiConfig =>
        for {
          channel <- ResourceOwner
            .forReleasable(() =>
              InProcessChannelBuilder
                .forName(InProcessGrpcName.forPort(config.serverConfig.clientConfig.port))
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
            config.serverConfig.tls,
            channel,
            packageSyncService,
            loggerFactory,
          )(
            config.jsonApiMetrics
          ).afterReleased(noTracingLogger.info("JSON-API HTTP Server is released"))
        } yield ()
      }
}
