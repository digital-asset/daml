// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.resources.ResourceOwner
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.{AuthService, Authorizer}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{
  NonNegativeDuration,
  NonNegativeFiniteDuration,
  TlsServerConfig,
}
import com.digitalasset.canton.ledger.api.auth.*
import com.digitalasset.canton.ledger.api.auth.interceptor.UserBasedAuthorizationInterceptor
import com.digitalasset.canton.ledger.api.domain
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
  DynamicDomainParameterGetter,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.platform.apiserver.services.TimeProviderType
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
      initSyncTimeout: NonNegativeFiniteDuration = ApiServiceOwner.DefaultInitSyncTimeout,
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
      commandProgressTracker: CommandProgressTracker,
      userManagementStore: UserManagementStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      command: CommandServiceConfig = ApiServiceOwner.DefaultCommandServiceConfig,
      writeService: state.WriteService,
      healthChecks: HealthChecks,
      metrics: LedgerApiServerMetrics,
      timeServiceBackend: Option[TimeServiceBackend] = None,
      otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
      otherInterceptors: List[ServerInterceptor] = List.empty,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
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
      dynParamGetter: DynamicDomainParameterGetter,
      interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      actorSystem: ActorSystem,
      materializer: Materializer,
      traceContext: TraceContext,
      tracer: Tracer,
  ): ResourceOwner[ApiService] = {

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
      )(servicesExecutionContext, traceContext),
      jwtTimestampLeeway = jwtTimestampLeeway,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )
    val healthChecksWithIndexService = healthChecks + ("index" -> indexService)

    val identityProviderConfigLoader = new IdentityProviderConfigLoader {
      override def getIdentityProviderConfig(issuer: String)(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[domain.IdentityProviderConfig] =
        identityProviderConfigStore.getActiveIdentityProviderByIssuer(issuer)(
          loggingContext,
          servicesExecutionContext,
        )
    }

    for {
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner()
      apiServicesOwner = new ApiServices.Owner(
        participantId = participantId,
        writeService = writeService,
        indexService = indexService,
        authorizer = authorizer,
        engine = engine,
        timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        timeProviderType =
          timeServiceBackend.fold[TimeProviderType](TimeProviderType.WallClock)(_ =>
            TimeProviderType.Static
          ),
        submissionTracker = submissionTracker,
        initSyncTimeout = initSyncTimeout.underlying,
        commandProgressTracker = commandProgressTracker,
        commandConfig = command,
        optTimeServiceBackend = timeServiceBackend,
        servicesExecutionContext = servicesExecutionContext,
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
      )(materializer, executionSequencerFactory, tracer)
        .map(_.withServices(otherServices))
      apiService <- new LedgerApiService(
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
          )(servicesExecutionContext),
          telemetry,
          loggerFactory,
          servicesExecutionContext,
        ) :: otherInterceptors,
        servicesExecutionContext,
        metrics,
        loggerFactory,
      )
    } yield {
      loggerFactory
        .getTracedLogger(getClass)
        .info(
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
  val DefaultInitSyncTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)
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
