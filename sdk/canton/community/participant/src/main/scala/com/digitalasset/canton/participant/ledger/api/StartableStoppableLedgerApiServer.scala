// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.entries.LoggingEntries
import com.daml.nameof.NameOf.functionFullName
import com.daml.tracing.Telemetry
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.{MemoryStorageConfig, ProcessingTimeout}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.HttpApiServer
import com.digitalasset.canton.ledger.api.auth.CachedJwtVerifierLoader
import com.digitalasset.canton.ledger.api.domain.{Filters, TransactionFilter}
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.localstore.*
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.ledger.participant.state.InternalStateService
import com.digitalasset.canton.ledger.participant.state.metrics.{
  TimedReadService,
  TimedWriteService,
}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{
  ApiRequestLogger,
  CantonGrpcUtil,
  ClientChannelBuilder,
}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.MutablePackageNameMapResolver
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.platform.LedgerApiServer
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandExecutor.AuthenticateContract
import com.digitalasset.canton.platform.apiserver.ratelimiting.{
  RateLimitingInterceptor,
  ThreadpoolCheck,
}
import com.digitalasset.canton.platform.apiserver.{ApiServiceOwner, LedgerFeatures}
import com.digitalasset.canton.platform.config.{IdentityProviderManagementConfig, ServerRole}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig,
  IndexerServiceOwner,
  IndexerStartupMode,
}
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import com.digitalasset.canton.platform.store.dao.events.ContractLoader
import com.digitalasset.canton.platform.store.packagemeta.InMemoryPackageMetadataStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import io.grpc.{BindableService, ServerInterceptor, ServerServiceDefinition}
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** The StartableStoppableLedgerApi enables a canton participant node to start and stop the ledger API server
  * depending on whether the participant node is a High Availability active or passive replica.
  *
  * @param config ledger api server configuration
  * @param participantDataSourceConfig configuration for the data source (e.g., jdbc url)
  * @param dbConfig the Index DB config
  * @param executionContext the execution context
  */
class StartableStoppableLedgerApiServer(
    config: CantonLedgerApiServerWrapper.Config,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    dbConfig: DbSupport.DbConfig,
    telemetry: Telemetry,
    futureSupervisor: FutureSupervisor,
    packageNameMapResolver: MutablePackageNameMapResolver,
    parameters: ParticipantNodeParameters,
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
  )

  private val ledgerApiResource = new AtomicReference[Option[Resource[Unit]]](None)

  override protected def loggerFactory: NamedLoggerFactory = config.loggerFactory
  override protected def timeouts: ProcessingTimeout =
    config.cantonParameterConfig.processingTimeouts

  /** Start the ledger API server and remember the resource.
    *
    * Assumes that ledger api is currently stopped erroring otherwise. If asked to start during shutdown ignores start.
    *
    * A possible improvement to consider in the future is to abort start upon subsequent call to stop. As is the stop
    * will wait until an inflight start completes.
    */
  def start()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    execQueue.execute(
      performUnlessClosingF(functionFullName) {
        ledgerApiResource.get match {
          case Some(_ledgerApiAlreadyStarted) =>
            logger.info(
              "Attempt to start ledger API server, but ledger API server already started. Ignoring."
            )
            Future.unit
          case None =>
            val ledgerApiServerResource =
              buildLedgerApiServerOwner().acquire()(
                ResourceContext(executionContext)
              )
            FutureUtil.logOnFailure(
              ledgerApiServerResource.asFuture.map { _unit =>
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
          config.syncService.unregisterInternalStateService()
          FutureUtil.logOnFailure(
            ledgerApiServerToStop.release().map { _unit =>
              logger.debug("Successfully stopped ledger API server")
              ledgerApiResource.set(None)
            },
            "Failed to stop ledger API server",
          )
        case None =>
          logger.debug("ledger API server already stopped")
          Future.unit
      },
      "stop ledger API server",
    )

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      logger.debug("Shutting down ledger API server")
      Seq(
        AsyncCloseable("ledger API server", stop().unwrap, timeouts.shutdownNetwork),
        SyncCloseable("ledger-api-server-queue", execQueue.close()),
      )
    }

  private def buildLedgerApiServerOwner(
  )(implicit traceContext: TraceContext) = {

    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    val startupMode: IndexerStartupMode = config.storageConfig match {
      // ledger api server needs an H2 db to run in memory
      case _: MemoryStorageConfig => IndexerStartupMode.MigrateAndStart
      case _ => IndexerStartupMode.JustStart
    }
    val numIndexer = config.indexerConfig.ingestionParallelism.unwrap
    val numLedgerApi = dbConfig.connectionPool.connectionPoolSize
    logger.info(s"Creating storage, num-indexer: $numIndexer, num-ledger-api: $numLedgerApi")

    val indexServiceConfig = config.serverConfig.indexService

    val authService = new CantonAdminTokenAuthService(
      config.adminToken,
      parent = config.serverConfig.authServices.map(
        _.create(
          config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway,
          loggerFactory,
        )
      ),
    )

    val jwtVerifierLoader = new CachedJwtVerifierLoader(metrics = config.metrics)

    val apiInfoService = new GrpcApiInfoService(CantonGrpcUtil.ApiName.LedgerApi)
      with BindableService {
      override def bindService(): ServerServiceDefinition =
        ApiInfoServiceGrpc.bindService(this, executionContext)
    }

    for {
      (inMemoryState, inMemoryStateUpdaterFlow) <-
        LedgerApiServer.createInMemoryStateAndUpdater(
          indexServiceConfig,
          config.serverConfig.commandService.maxCommandsInFlight,
          config.metrics,
          executionContext,
          tracer,
          loggerFactory,
        )
      packageMetadataStore = new InMemoryPackageMetadataStore(
        inMemoryState.packageMetadataView
      )
      _ <- ResourceOwner.forReleasable(() =>
        packageNameMapResolver.setReference(() =>
          packageMetadataStore.getSnapshot.getUpgradablePackageMap
        )
      )(_ => Future.successful(packageNameMapResolver.unset()))
      timedReadService = new TimedReadService(config.syncService, config.metrics)
      dbSupport <- DbSupport
        .owner(
          serverRole = ServerRole.ApiServer,
          metrics = config.metrics,
          dbConfig = dbConfig,
          loggerFactory = loggerFactory,
        )
      indexerHealth <- new IndexerServiceOwner(
        config.participantId,
        participantDataSourceConfig,
        timedReadService,
        config.indexerConfig,
        config.metrics,
        inMemoryState,
        inMemoryStateUpdaterFlow,
        executionContext,
        tracer,
        loggerFactory,
        startupMode = startupMode,
        dataSourceProperties = DbSupport.DataSourceProperties(
          connectionPool = IndexerConfig
            .createConnectionPoolConfig(
              ingestionParallelism = config.indexerConfig.ingestionParallelism.unwrap,
              connectionTimeout = config.serverConfig.databaseConnectionTimeout.underlying,
            ),
          postgres = config.serverConfig.postgresDataSource,
        ),
        highAvailability = config.indexerHaConfig,
        indexServiceDbDispatcher = Some(dbSupport.dbDispatcher),
      )
      contractLoader <- {
        import config.cantonParameterConfig.ledgerApiServerParameters.contractLoader.*
        ContractLoader.create(
          contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
            inMemoryState.ledgerEndCache,
            inMemoryState.stringInterningView,
          ),
          dbDispatcher = dbSupport.dbDispatcher,
          metrics = config.metrics,
          maxQueueSize = maxQueueSize.value,
          maxBatchSize = maxBatchSize.value,
          parallelism = parallelism.value,
          loggerFactory = loggerFactory,
        )
      }
      indexService <- new IndexServiceOwner(
        dbSupport = dbSupport,
        config = indexServiceConfig,
        participantId = config.participantId,
        metrics = config.metrics,
        servicesExecutionContext = executionContext,
        engine = config.engine,
        inMemoryState = inMemoryState,
        tracer = config.tracerProvider.tracer,
        loggerFactory = loggerFactory,
        incompleteOffsets = timedReadService.incompleteReassignmentOffsets(_, _)(_),
        contractLoader = contractLoader,
      )
      _ = timedReadService.registerInternalStateService(new InternalStateService {
        override def activeContracts(
            partyIds: Set[LfPartyId],
            validAt: Option[Offset],
        )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
          indexService.getActiveContracts(
            filter = TransactionFilter(
              filtersByParty = partyIds.view.map(_ -> Filters.noFilter).toMap,
              alwaysPopulateCreatedEventBlob = true,
            ),
            verbose = false,
            activeAtO = validAt,
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
      serializableContractAuthenticator = SerializableContractAuthenticator(
        config.syncService.pureCryptoApi,
        parameters,
      )

      authenticateContract: AuthenticateContract = c =>
        serializableContractAuthenticator.authenticate(c)

      timedWriteService = new TimedWriteService(config.syncService, config.metrics)
      _ <- ApiServiceOwner(
        submissionTracker = inMemoryState.submissionTracker,
        indexService = indexService,
        userManagementStore = userManagementStore,
        packageMetadataStore = packageMetadataStore,
        identityProviderConfigStore = getIdentityProviderConfigStore(
          dbSupport,
          config.serverConfig.identityProviderManagement,
          loggerFactory,
        ),
        partyRecordStore = partyRecordStore,
        participantId = config.participantId,
        command = config.serverConfig.commandService,
        initSyncTimeout = config.serverConfig.initSyncTimeout,
        managementServiceTimeout = config.serverConfig.managementServiceTimeout,
        userManagement = config.serverConfig.userManagementService,
        partyManagementServiceConfig = config.serverConfig.partyManagementService,
        tls = config.serverConfig.tls
          .map(LedgerApiServerConfig.ledgerApiServerTlsConfigFromCantonServerConfig),
        address = Some(config.serverConfig.address),
        maxInboundMessageSize = config.serverConfig.maxInboundMessageSize.unwrap,
        port = config.serverConfig.port,
        seeding = config.cantonParameterConfig.ledgerApiServerParameters.contractIdSeeding,
        optWriteService = Some(timedWriteService),
        readService = timedReadService,
        healthChecks = new HealthChecks(
          "read" -> timedReadService,
          "write" -> (() => config.syncService.currentWriteHealth()),
          "indexer" -> indexerHealth,
        ),
        metrics = config.metrics,
        timeServiceBackend = config.testingTimeService,
        otherServices = Seq(apiInfoService),
        otherInterceptors = getInterceptors(dbSupport.dbDispatcher.executor),
        engine = config.engine,
        authorityResolver = config.syncService.cantonAuthorityResolver,
        servicesExecutionContext = executionContext,
        checkOverloaded = config.syncService.checkOverloaded,
        ledgerFeatures = getLedgerFeatures,
        maxDeduplicationDuration = config.maxDeduplicationDuration,
        authService = authService,
        jwtVerifierLoader = jwtVerifierLoader,
        jwtTimestampLeeway =
          config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway,
        tokenExpiryGracePeriodForStreams =
          config.cantonParameterConfig.ledgerApiServerParameters.tokenExpiryGracePeriodForStreams,
        meteringReportKey = config.meteringReportKey,
        engineLoggingConfig = config.cantonParameterConfig.engine.submissionPhaseLogging,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        authenticateContract = authenticateContract,
        dynParamGetter = config.syncService.dynamicDomainParameterGetter,
        disableUpgradeValidation = config.cantonParameterConfig.disableUpgradeValidation,
      )
      _ <- startHttpApiIfEnabled
      _ <- {
        config.serverConfig.userManagementService.additionalAdminUserId.fold(ResourceOwner.unit) {
          rawUserId =>
            ResourceOwner.forFuture { () =>
              userManagementStore.createExtraAdminUser(rawUserId)
            }
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
    )(executionContext, traceContext)

  private def getInterceptors(
      indexerExecutor: QueueAwareExecutor & NamedExecutor
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
            queue = indexerExecutor,
            loggerFactory = loggerFactory,
          ),
        ),
      )
    )
    .toList

  private def getLedgerFeatures: LedgerFeatures = LedgerFeatures(
    staticTime = config.testingTimeService.isDefined
  )

  private def startHttpApiIfEnabled: ResourceOwner[Unit] =
    config.jsonApiConfig
      .fold(ResourceOwner.unit) { jsonApiConfig =>
        for {
          channel <- ResourceOwner
            .forReleasable(() =>
              ClientChannelBuilder.createChannelToTrustedServer(config.serverConfig.clientConfig)
            ) { channel => Future(channel.shutdown().discard) }
          _ <- HttpApiServer(jsonApiConfig, channel, loggerFactory)(config.jsonApiMetrics)
        } yield ()
      }
}
