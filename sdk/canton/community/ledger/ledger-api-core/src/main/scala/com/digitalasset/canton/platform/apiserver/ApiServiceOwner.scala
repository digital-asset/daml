// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.resources.ResourceOwner
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.{AuthService, Authorizer}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{
  KeepAliveServerConfig,
  NonNegativeDuration,
  NonNegativeFiniteDuration,
  TlsServerConfig,
}
import com.digitalasset.canton.ledger.api.IdentityProviderConfig
import com.digitalasset.canton.ledger.api.auth.*
import com.digitalasset.canton.ledger.api.auth.interceptor.UserBasedAuthorizationInterceptor
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandExecutor.AuthenticateContract
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandProgressTracker,
  DynamicSynchronizerParameterGetter,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.platform.apiserver.services.TimeProviderType
import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  IdentityProviderManagementConfig,
  InteractiveSubmissionServiceConfig,
  PartyManagementServiceConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.{BindableService, ServerInterceptor}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import java.time.Clock
import scala.collection.immutable
import scala.concurrent.{ExecutionContextExecutor, Future}

object ApiServiceOwner {

  def apply(
      // configuration parameters
      address: Option[String] = DefaultAddress, // This defaults to "localhost" when set to `None`.
      maxInboundMessageSize: Int = DefaultMaxInboundMessageSize,
      port: Port = DefaultPort,
      tls: Option[TlsServerConfig] = DefaultTls,
      seeding: Seeding = DefaultSeeding,
      managementServiceTimeout: NonNegativeFiniteDuration =
        ApiServiceOwner.DefaultManagementServiceTimeout,
      ledgerFeatures: LedgerFeatures,
      maxDeduplicationDuration: NonNegativeFiniteDuration,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      tokenExpiryGracePeriodForStreams: Option[NonNegativeDuration],
      // immutable configuration parameters
      participantId: Ref.ParticipantId,
      meteringReportKey: MeteringReportKey = CommunityKey,
      // objects
      indexService: IndexService,
      submissionTracker: SubmissionTracker,
      partyAllocationTracker: PartyAllocation.Tracker,
      commandProgressTracker: CommandProgressTracker,
      userManagementStore: UserManagementStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      command: CommandServiceConfig = ApiServiceOwner.DefaultCommandServiceConfig,
      syncService: state.SyncService,
      healthChecks: HealthChecks,
      metrics: LedgerApiServerMetrics,
      timeServiceBackend: Option[TimeServiceBackend] = None,
      otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
      otherInterceptors: List[ServerInterceptor] = List.empty,
      engine: Engine,
      queryExecutionContext: ExecutionContextExecutor,
      commandExecutionContext: ExecutionContextExecutor,
      checkOverloaded: TraceContext => Option[state.SubmissionResult] =
        _ => None, // Used for Canton rate-limiting,
      authService: AuthService,
      jwtVerifierLoader: JwtVerifierLoader,
      userManagement: UserManagementServiceConfig = ApiServiceOwner.DefaultUserManagement,
      partyManagementServiceConfig: PartyManagementServiceConfig =
        ApiServiceOwner.DefaultPartyManagementServiceConfig,
      engineLoggingConfig: EngineLoggingConfig,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
      authenticateContract: AuthenticateContract,
      dynParamGetter: DynamicSynchronizerParameterGetter,
      interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
      lfValueTranslation: LfValueTranslation,
      keepAlive: Option[KeepAliveServerConfig],
  )(implicit
      actorSystem: ActorSystem,
      materializer: Materializer,
      traceContext: TraceContext,
      tracer: Tracer,
  ): ResourceOwner[ApiService] = {
    import com.digitalasset.canton.platform.ResourceOwnerOps
    val logger = loggerFactory.getTracedLogger(getClass)

    val authorizer = new Authorizer(
      now = Clock.systemUTC.instant _,
      participantId = participantId,
      ongoingAuthorizationFactory = UserBasedOngoingAuthorization.Factory(
        now = Clock.systemUTC.instant _,
        userManagementStore = userManagementStore,
        userRightsCheckIntervalInSeconds = userManagement.cacheExpiryAfterWriteInSeconds,
        pekkoScheduler = actorSystem.scheduler,
        jwtTimestampLeeway = jwtTimestampLeeway,
        tokenExpiryGracePeriodForStreams =
          tokenExpiryGracePeriodForStreams.map(_.asJavaApproximation),
        loggerFactory = loggerFactory,
      )(commandExecutionContext, traceContext),
      jwtTimestampLeeway = jwtTimestampLeeway,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )
    val healthChecksWithIndexService = healthChecks + ("index" -> indexService)

    val identityProviderConfigLoader = new IdentityProviderConfigLoader {
      override def getIdentityProviderConfig(issuer: String)(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[IdentityProviderConfig] =
        identityProviderConfigStore.getActiveIdentityProviderByIssuer(issuer)(
          loggingContext,
          commandExecutionContext,
        )
    }

    for {
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner()
        .afterReleased(logger.info(s"ExecutionSequencerFactory is released for LedgerApiService"))
      apiServicesOwner = ApiServices(
        participantId = participantId,
        syncService = syncService,
        indexService = indexService,
        authorizer = authorizer,
        engine = engine,
        timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        timeProviderType =
          timeServiceBackend.fold[TimeProviderType](TimeProviderType.WallClock)(_ =>
            TimeProviderType.Static
          ),
        submissionTracker = submissionTracker,
        partyAllocationTracker = partyAllocationTracker,
        commandProgressTracker = commandProgressTracker,
        commandConfig = command,
        optTimeServiceBackend = timeServiceBackend,
        queryExecutionContext = queryExecutionContext,
        commandExecutionContext = commandExecutionContext,
        metrics = metrics,
        healthChecks = healthChecksWithIndexService,
        seedService = SeedService(seeding),
        managementServiceTimeout = managementServiceTimeout.underlying,
        checkOverloaded = checkOverloaded,
        userManagementStore = userManagementStore,
        identityProviderConfigStore = identityProviderConfigStore,
        partyRecordStore = partyRecordStore,
        ledgerFeatures = ledgerFeatures,
        maxDeduplicationDuration = maxDeduplicationDuration,
        userManagementServiceConfig = userManagement,
        partyManagementServiceConfig = partyManagementServiceConfig,
        engineLoggingConfig = engineLoggingConfig,
        meteringReportKey = meteringReportKey,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        authenticateContract = authenticateContract,
        dynParamGetter = dynParamGetter,
        interactiveSubmissionServiceConfig = interactiveSubmissionServiceConfig,
        lfValueTranslation = lfValueTranslation,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(materializer, executionSequencerFactory, tracer).withServices(otherServices)
      // for all the top level gRPC servicing apparatus we use the writeApiServicesExecutionContext
      apiService <- LedgerApiService(
        apiServicesOwner,
        port,
        maxInboundMessageSize,
        address,
        tls,
        new UserBasedAuthorizationInterceptor(
          authService = authService,
          Option.when(userManagement.enabled)(userManagementStore),
          new IdentityProviderAwareAuthServiceImpl(
            identityProviderConfigLoader = identityProviderConfigLoader,
            jwtVerifierLoader = jwtVerifierLoader,
            loggerFactory = loggerFactory,
          )(commandExecutionContext),
          telemetry,
          loggerFactory,
          commandExecutionContext,
        ) :: otherInterceptors,
        commandExecutionContext,
        metrics,
        keepAlive,
        loggerFactory,
      ).afterReleased(logger.info(s"LedgerApiService is released"))
    } yield {
      logger.info(
        s"Initialized API server listening to port = ${apiService.port} ${if (tls.isDefined) "using tls"
          else "without tls"}."
      )
      apiService
    }
  }

  val DefaultPort: Port = Port.tryCreate(6865)
  val DefaultAddress: Option[String] = None
  val DefaultTls: Option[TlsServerConfig] = None
  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024
  val DefaultSeeding: Seeding = Seeding.Strong
  val DefaultManagementServiceTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(2)
  val DefaultUserManagement: UserManagementServiceConfig =
    UserManagementServiceConfig.default(enabled = false)
  val DefaultPartyManagementServiceConfig: PartyManagementServiceConfig =
    PartyManagementServiceConfig.default
  val DefaultIdentityProviderManagementConfig: IdentityProviderManagementConfig =
    IdentityProviderManagementConfig()
  val DefaultCommandServiceConfig: CommandServiceConfig = CommandServiceConfig.Default
}
