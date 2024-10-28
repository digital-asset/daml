// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.buildinfo.BuildInfo
import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.tracing.Telemetry
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{NonNegativeDuration, NonNegativeFiniteDuration}
import com.digitalasset.canton.ledger.api.auth.*
import com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.tls.TlsConfiguration
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.configuration.LedgerId
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexService
import com.digitalasset.canton.ledger.participant.state.v2.ReadService
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandExecutor.AuthenticateUpgradableContract
import com.digitalasset.canton.platform.apiserver.execution.{
  AuthorityResolver,
  CommandProgressTracker,
  DynamicDomainParameterGetter,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  PartyManagementServiceConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.platform.localstore.{
  IdentityProviderManagementConfig,
  PackageMetadataStore,
}
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.platform.store.cache.PruningOffsetCache
import com.digitalasset.canton.tracing.TraceContext
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
      apiStreamShutdownTimeout: NonNegativeFiniteDuration =
        ApiServiceOwner.DefaultApiStreamShutdownTimeout,
      address: Option[String] = DefaultAddress, // This defaults to "localhost" when set to `None`.
      maxInboundMessageSize: Int = DefaultMaxInboundMessageSize,
      port: Port = DefaultPort,
      tls: Option[TlsConfiguration] = DefaultTls,
      seeding: Seeding = DefaultSeeding,
      configurationLoadTimeout: NonNegativeFiniteDuration =
        ApiServiceOwner.DefaultConfigurationLoadTimeout,
      managementServiceTimeout: NonNegativeFiniteDuration =
        ApiServiceOwner.DefaultManagementServiceTimeout,
      ledgerFeatures: LedgerFeatures,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      tokenExpiryGracePeriodForStreams: Option[NonNegativeDuration],
      enableExplicitDisclosure: Boolean = false,
      // immutable configuration parameters
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      meteringReportKey: MeteringReportKey = CommunityKey,
      // objects
      indexService: IndexService,
      submissionTracker: SubmissionTracker,
      commandProgressTracker: CommandProgressTracker,
      userManagementStore: UserManagementStore,
      packageMetadataStore: PackageMetadataStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      command: CommandServiceConfig = ApiServiceOwner.DefaultCommandServiceConfig,
      optWriteService: Option[state.WriteService],
      readService: ReadService,
      healthChecks: HealthChecks,
      metrics: Metrics,
      timeServiceBackend: Option[TimeServiceBackend] = None,
      otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
      otherInterceptors: List[ServerInterceptor] = List.empty,
      engine: Engine,
      authorityResolver: AuthorityResolver,
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
      authenticateUpgradableContract: AuthenticateUpgradableContract,
      dynParamGetter: DynamicDomainParameterGetter,
      pruningOffsetCache: PruningOffsetCache,
  )(implicit
      actorSystem: ActorSystem,
      materializer: Materializer,
      traceContext: TraceContext,
      tracer: Tracer,
  ): ResourceOwner[ApiService] = {

    val authorizer = new Authorizer(
      Clock.systemUTC.instant _,
      ledgerId,
      participantId,
      userManagementStore,
      servicesExecutionContext,
      userRightsCheckIntervalInSeconds = userManagement.cacheExpiryAfterWriteInSeconds,
      pekkoScheduler = actorSystem.scheduler,
      jwtTimestampLeeway = jwtTimestampLeeway,
      tokenExpiryGracePeriodForStreams =
        tokenExpiryGracePeriodForStreams.map(_.asJavaApproximation),
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )
    // TODO(i12283) LLP: Consider fusing the index health check with the indexer health check
    val healthChecksWithIndexService = healthChecks + ("index" -> indexService)

    val identityProviderConfigLoader = new IdentityProviderConfigLoader {
      override def getIdentityProviderConfig(issuer: LedgerId)(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[domain.IdentityProviderConfig] =
        identityProviderConfigStore.getActiveIdentityProviderByIssuer(issuer)
    }

    for {
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner()
      apiServicesOwner = new ApiServices.Owner(
        participantId = participantId,
        optWriteService = optWriteService,
        readService = readService,
        indexService = indexService,
        authorizer = authorizer,
        engine = engine,
        authorityResolver = authorityResolver,
        timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        timeProviderType =
          timeServiceBackend.fold[TimeProviderType](TimeProviderType.WallClock)(_ =>
            TimeProviderType.Static
          ),
        submissionTracker = submissionTracker,
        commandProgressTracker = commandProgressTracker,
        configurationLoadTimeout = configurationLoadTimeout.underlying,
        commandConfig = command,
        optTimeServiceBackend = timeServiceBackend,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        healthChecks = healthChecksWithIndexService,
        seedService = SeedService(seeding),
        managementServiceTimeout = managementServiceTimeout.underlying,
        checkOverloaded = checkOverloaded,
        userManagementStore = userManagementStore,
        packageMetadataStore = packageMetadataStore,
        identityProviderConfigStore = identityProviderConfigStore,
        partyRecordStore = partyRecordStore,
        ledgerFeatures = ledgerFeatures,
        userManagementServiceConfig = userManagement,
        partyManagementServiceConfig = partyManagementServiceConfig,
        apiStreamShutdownTimeout = apiStreamShutdownTimeout.underlying,
        engineLoggingConfig = engineLoggingConfig,
        meteringReportKey = meteringReportKey,
        enableExplicitDisclosure = enableExplicitDisclosure,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        authenticateUpgradableContract = authenticateUpgradableContract,
        dynParamGetter = dynParamGetter,
        pruningOffsetCache = pruningOffsetCache,
      )(materializer, executionSequencerFactory, tracer)
        .map(_.withServices(otherServices))
      apiService <- new LedgerApiService(
        apiServicesOwner,
        port,
        maxInboundMessageSize,
        address,
        tls,
        AuthorizationInterceptor(
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
          s"Initialized API server version ${BuildInfo.Version} with ledger-id = $ledgerId, port = ${apiService.port}."
        )
      apiService
    }
  }

  val DefaultPort: Port = Port.tryCreate(6865)
  val DefaultAddress: Option[String] = None
  val DefaultTls: Option[TlsConfiguration] = None
  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024
  val DefaultConfigurationLoadTimeout: NonNegativeFiniteDuration =
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
  val DefaultApiStreamShutdownTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(5)
}
