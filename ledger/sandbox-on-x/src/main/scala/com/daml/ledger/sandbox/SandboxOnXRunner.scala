// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.api.auth.{
  AuthServiceJWT,
  AuthServiceNone,
  AuthServiceStatic,
  AuthServiceWildcard,
}
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalContractIds,
  ExperimentalExplicitDisclosure,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.{Update, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.{Metrics, OpenTelemetryMeterOwner}
import com.daml.platform.LedgerApiServer
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.ratelimiting.ThreadpoolCheck.ThreadpoolCount
import com.daml.platform.apiserver.ratelimiting.{RateLimitingInterceptor, ThreadpoolCheck}
import com.daml.platform.apiserver.{LedgerFeatures, TimeServiceBackend}
import com.daml.platform.config.ParticipantConfig
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.DbType
import com.daml.ports.Port

import scala.concurrent.ExecutionContextExecutorService
import scala.util.Try

object SandboxOnXRunner {
  val RunnerName = "sandbox-on-x"
  private val logger = ContextualizedLogger.get(getClass)

  def owner(
      configAdaptor: BridgeConfigAdaptor,
      config: Config,
      bridgeConfig: BridgeConfig,
  ): ResourceOwner[Port] =
    new ResourceOwner[Port] {
      override def acquire()(implicit context: ResourceContext): Resource[Port] =
        SandboxOnXRunner
          .run(bridgeConfig, config, configAdaptor)
          .acquire()
    }

  def run(
      bridgeConfig: BridgeConfig,
      config: Config,
      configAdaptor: BridgeConfigAdaptor,
  ): ResourceOwner[Port] = newLoggingContext { implicit loggingContext =>
    implicit val actorSystem: ActorSystem = ActorSystem(RunnerName)
    implicit val materializer: Materializer = Materializer(actorSystem)

    for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits in a `for` comprehension.
      _ <- ResourceOwner.forActorSystem(() => actorSystem)
      _ <- ResourceOwner.forMaterializer(() => materializer)
      openTelemetryMeter <- OpenTelemetryMeterOwner(
        config.metrics.enabled,
        Some(config.metrics.reporter),
      )

      (participantId, dataSource, participantConfig) <- assertSingleParticipant(config)
      metrics <- MetricsOwner(openTelemetryMeter, config.metrics, participantId)
      timeServiceBackendO = configAdaptor.timeServiceBackend(participantConfig.apiServer)
      (stateUpdatesFeedSink, stateUpdatesSource) <- AkkaSubmissionsBridge()

      servicesThreadPoolSize = participantConfig.servicesThreadPoolSize
      servicesExecutionContext <- buildServicesExecutionContext(metrics, servicesThreadPoolSize)

      buildWriteServiceLambda = buildWriteService(
        participantId = participantId,
        feedSink = stateUpdatesFeedSink,
        bridgeConfig = bridgeConfig,
        materializer = materializer,
        loggingContext = loggingContext,
        metricsFactory = metrics.dropwizardFactory,
        servicesThreadPoolSize = servicesThreadPoolSize,
        servicesExecutionContext = servicesExecutionContext,
        timeServiceBackendO = timeServiceBackendO,
        stageBufferSize = bridgeConfig.stageBufferSize,
      )
      apiServer <- new LedgerApiServer(
        ledgerFeatures = LedgerFeatures(
          staticTime = timeServiceBackendO.isDefined,
          commandDeduplicationFeatures = CommandDeduplicationFeatures.of(
            deduplicationPeriodSupport = Some(
              CommandDeduplicationPeriodSupport.of(
                CommandDeduplicationPeriodSupport.OffsetSupport.OFFSET_NOT_SUPPORTED,
                CommandDeduplicationPeriodSupport.DurationSupport.DURATION_NATIVE_SUPPORT,
              )
            ),
            deduplicationType = CommandDeduplicationType.ASYNC_ONLY,
            maxDeduplicationDurationEnforced = true,
          ),
          contractIdFeatures = ExperimentalContractIds.of(
            v1 = ExperimentalContractIds.ContractIdV1Support.NON_SUFFIXED
          ),
          explicitDisclosure = ExperimentalExplicitDisclosure.of(true),
        ),
        authService = configAdaptor.authService(participantConfig),
        jwtVerifierLoader =
          configAdaptor.jwtVerifierLoader(participantConfig, metrics, servicesExecutionContext),
        buildWriteService = buildWriteServiceLambda,
        engine = new Engine(config.engine),
        ledgerId = config.ledgerId,
        participantConfig = participantConfig,
        participantDataSourceConfig = dataSource,
        participantId = participantId,
        readService = new BridgeReadService(
          ledgerId = config.ledgerId,
          maximumDeduplicationDuration = bridgeConfig.maxDeduplicationDuration,
          stateUpdatesSource,
        ),
        timeServiceBackendO = timeServiceBackendO,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        explicitDisclosureUnsafeEnabled = true,
        rateLimitingInterceptor =
          participantConfig.apiServer.rateLimit.map(buildRateLimitingInterceptor(metrics)),
      )(actorSystem, materializer).owner
    } yield {
      logInitializationHeader(
        config,
        participantId,
        participantConfig,
        dataSource,
        bridgeConfig,
      )
      apiServer.port
    }
  }

  def assertSingleParticipant(
      config: Config
  ): ResourceOwner[(Ref.ParticipantId, ParticipantDataSourceConfig, ParticipantConfig)] = for {
    (participantId, participantConfig) <- validateSingleParticipantConfigured(config)
    dataSource <- validateDataSource(config, participantId)
  } yield (participantId, dataSource, participantConfig)

  private def validateDataSource(
      config: Config,
      participantId: Ref.ParticipantId,
  ): ResourceOwner[ParticipantDataSourceConfig] =
    ResourceOwner.forTry(() =>
      Try(
        config.dataSource.getOrElse(
          participantId,
          throw new IllegalArgumentException(
            s"Data Source has not been provided for participantId=$participantId"
          ),
        )
      )
    )

  private def validateSingleParticipantConfigured(
      config: Config
  ): ResourceOwner[(Ref.ParticipantId, ParticipantConfig)] =
    config.participants.toList match {

      case (participantId, participantConfig) :: Nil =>
        ResourceOwner.successful(
          (participantConfig.participantIdOverride.getOrElse(participantId), participantConfig)
        )
      case _ =>
        ResourceOwner.failed {
          val loggingMessage = "Sandbox-on-X can only be run with a single participant."
          newLoggingContext(logger.info(loggingMessage)(_))
          new IllegalArgumentException(loggingMessage)
        }
    }

  // Builds the write service and uploads the initialization DARs
  def buildWriteService(
      participantId: Ref.ParticipantId,
      feedSink: Sink[(Offset, Update), NotUsed],
      bridgeConfig: BridgeConfig,
      materializer: Materializer,
      loggingContext: LoggingContext,
      metricsFactory: Factory,
      servicesThreadPoolSize: Int,
      servicesExecutionContext: ExecutionContextExecutorService,
      timeServiceBackendO: Option[TimeServiceBackend],
      stageBufferSize: Int,
  ): IndexService => ResourceOwner[WriteService] = { indexService =>
    val bridgeMetrics = new BridgeMetrics(factory = metricsFactory)
    for {
      ledgerBridge <- LedgerBridge.owner(
        participantId,
        bridgeConfig,
        indexService,
        bridgeMetrics,
        servicesThreadPoolSize,
        timeServiceBackendO.getOrElse(TimeProvider.UTC),
        stageBufferSize,
      )(loggingContext, servicesExecutionContext)
      writeService <- ResourceOwner.forCloseable(() =>
        new BridgeWriteService(
          feedSink = feedSink,
          submissionBufferSize = bridgeConfig.submissionBufferSize,
          ledgerBridge = ledgerBridge,
          bridgeMetrics = bridgeMetrics,
        )(materializer, loggingContext)
      )
    } yield writeService
  }

  private def buildServicesExecutionContext(
      metrics: Metrics,
      servicesThreadPoolSize: Int,
  ): ResourceOwner[ExecutionContextExecutorService] =
    ResourceOwner
      .forExecutorService(() =>
        InstrumentedExecutors.newWorkStealingExecutor(
          metrics.daml.lapi.threadpool.apiServices,
          servicesThreadPoolSize,
          metrics.dropwizardFactory.registry,
          metrics.executorServiceMetrics,
        )
      )

  private def logInitializationHeader(
      config: Config,
      participantId: Ref.ParticipantId,
      participantConfig: ParticipantConfig,
      participantDataSourceConfig: ParticipantDataSourceConfig,
      extra: BridgeConfig,
  ): Unit = {
    val apiServerConfig = participantConfig.apiServer
    val authentication =
      participantConfig.authentication.create(participantConfig.jwtTimestampLeeway) match {
        case _: AuthServiceJWT => "JWT-based authentication"
        case AuthServiceNone => "none authenticated"
        case _: AuthServiceStatic => "static authentication"
        case AuthServiceWildcard => "all unauthenticated allowed"
        case other => other.getClass.getSimpleName
      }

    val ledgerDetails =
      Seq[(String, String)](
        "index DB backend" -> DbType
          .jdbcType(participantDataSourceConfig.jdbcUrl)
          .name,
        "participant-id" -> participantId,
        "ledger-id" -> config.ledgerId,
        "port" -> apiServerConfig.port.toString,
        "time mode" -> apiServerConfig.timeProviderType.description,
        "allowed language versions" -> s"[min = ${config.engine.allowedLanguageVersions.min}, max = ${config.engine.allowedLanguageVersions.max}]",
        "authentication" -> authentication,
        "contract ids seeding" -> apiServerConfig.seeding.toString,
      ).map { case (key, value) =>
        s"$key = $value"
      }.mkString(", ")

    logger.withoutContext.info(
      s"Initialized {} with {}, version {}, {}",
      RunnerName,
      if (extra.conflictCheckingEnabled) "conflict checking ledger bridge"
      else "pass-through ledger bridge (no conflict checking)",
      BuildInfo.Version,
      ledgerDetails,
    )
  }

  def buildRateLimitingInterceptor(
      metrics: Metrics
  )(config: RateLimitingConfig): RateLimitingInterceptor = {

    val apiServices: ThreadpoolCount = new ThreadpoolCount(metrics)(
      "Api Services Threadpool",
      metrics.daml.lapi.threadpool.apiServices,
    )
    val apiServicesCheck = ThreadpoolCheck(apiServices, config.maxApiServicesQueueSize)

    RateLimitingInterceptor(metrics, config, List(apiServicesCheck))

  }

}
