// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.{
  AuthServiceJWT,
  AuthServiceNone,
  AuthServiceStatic,
  AuthServiceWildcard,
}
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalContractIds,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v2.{ReadService, Update, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.{newLoggingContext, newLoggingContextWith}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.apiserver._
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.store.{DbSupport, DbType, LfValueTranslationCache}
import com.daml.platform.usermanagement.{PersistentUserManagementStore, UserManagementConfig}
import com.daml.resources.{AbstractResourceOwner, ProgramResource}
import com.typesafe.config.ConfigFactory

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.chaining._
import PureConfigReaderWriter._
import com.daml.ledger.configuration.LedgerId

object SandboxOnXRunner {
  val RunnerName = "sandbox-on-x"
  private val logger = ContextualizedLogger.get(getClass)

  def run(configObject: com.typesafe.config.Config = ConfigFactory.load()): Unit = {
    val config = ConfigLoader.loadConfigUnsafe[Config]("ledger", configObject)
    val bridge = ConfigLoader.loadConfigUnsafe[BridgeConfig]("bridge", configObject)
    val configAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor
    new ProgramResource(
      owner = SandboxOnXRunner.owner(configAdaptor, config, bridge)
    ).run(ResourceContext.apply)
  }

  def owner(
      configAdaptor: BridgeConfigAdaptor,
      config: Config,
      bridgeConfig: BridgeConfig,
  ): AbstractResourceOwner[ResourceContext, Unit] = {
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        SandboxOnXRunner.run(configAdaptor, config, bridgeConfig)
      }
    }
  }

  def run(
      configAdaptor: BridgeConfigAdaptor,
      config: Config,
      bridgeConfig: BridgeConfig,
  )(implicit resourceContext: ResourceContext): Resource[Unit] = {
    implicit val actorSystem: ActorSystem = ActorSystem(RunnerName)
    implicit val materializer: Materializer = Materializer(actorSystem)

    for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits in a `for` comprehension.
      _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
      _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

      // Start the ledger
      (participantId, participantConfig) <- validateCombinedParticipantMode(config)
      _ <- buildLedger(
        participantId,
        config,
        participantConfig,
        bridgeConfig,
        materializer,
        actorSystem,
        configAdaptor,
      ).acquire()
    } yield logInitializationHeader(config, participantId, participantConfig, bridgeConfig)
  }

  def validateCombinedParticipantMode(
      config: Config
  ): Resource[(Ref.ParticipantId, ParticipantConfig)] =
    config.participants.toList match {
      case (participantId, participantConfig) :: Nil
          if participantConfig.runMode == ParticipantRunMode.Combined =>
        Resource.successful((participantId, participantConfig))
      case _ =>
        Resource.failed {
          val loggingMessage = "Sandbox-on-X can only be run in a single COMBINED participant mode."
          newLoggingContext(logger.info(loggingMessage)(_))
          new IllegalArgumentException(loggingMessage)
        }
    }

  def buildLedger(implicit
      participantId: Ref.ParticipantId,
      config: Config,
      participantConfig: ParticipantConfig,
      bridgeConfig: BridgeConfig,
      materializer: Materializer,
      actorSystem: ActorSystem,
      configAdaptor: BridgeConfigAdaptor,
      metrics: Option[Metrics] = None,
  ): ResourceOwner[(ApiServer, WriteService, IndexService)] = {
    val apiServerConfig: ApiServerConfig = participantConfig.apiServer
    val sharedEngine = new Engine(config.engine)

    newLoggingContextWith("participantId" -> participantId) { implicit loggingContext =>
      for {
        metrics <- metrics.map(ResourceOwner.successful).getOrElse(buildMetrics(participantId))
        translationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
          config = participantConfig.lfValueTranslationCache,
          metrics = metrics,
        )

        (stateUpdatesFeedSink, stateUpdatesSource) <- AkkaSubmissionsBridge()

        servicesThreadPoolSize = Runtime.getRuntime.availableProcessors()
        servicesExecutionContext <- buildServicesExecutionContext(
          metrics,
          servicesThreadPoolSize,
        )

        readServiceWithSubscriber = new BridgeReadService(
          ledgerId = config.ledgerId,
          maximumDeduplicationDuration = bridgeConfig.maxDeduplicationDuration,
          stateUpdatesSource,
        )

        indexerHealthChecks <- buildIndexerServer(
          metrics,
          new TimedReadService(readServiceWithSubscriber, metrics),
          translationCache,
          participantId,
          participantConfig,
        )

        dbSupport <- DbSupport
          .owner(
            serverRole = ServerRole.ApiServer,
            metrics = metrics,
            dbConfig = apiServerConfig.database,
          )

        indexService <- StandaloneIndexService(
          ledgerId = config.ledgerId,
          config = participantConfig.indexService,
          metrics = metrics,
          engine = sharedEngine,
          servicesExecutionContext = servicesExecutionContext,
          lfValueTranslationCache = translationCache,
          dbSupport = dbSupport,
          participantId = participantId,
        )

        timeServiceBackend = configAdaptor.timeServiceBackend(apiServerConfig)

        writeService <- buildWriteService(
          participantId,
          stateUpdatesFeedSink,
          indexService,
          metrics,
          servicesExecutionContext,
          servicesThreadPoolSize,
          timeServiceBackend,
          participantConfig,
          bridgeConfig,
        )

        apiServer <- buildStandaloneApiServer(
          sharedEngine,
          indexService,
          metrics,
          servicesExecutionContext,
          new TimedWriteService(writeService, metrics),
          indexerHealthChecks,
          timeServiceBackend,
          dbSupport,
          config.ledgerId,
          apiServerConfig,
          participantId,
          configAdaptor,
        )
      } yield (apiServer, writeService, indexService)
    }
  }

  private def buildStandaloneApiServer(
      sharedEngine: Engine,
      indexService: IndexService,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContextExecutorService,
      writeService: WriteService,
      healthChecksWithIndexer: HealthChecks,
      timeServiceBackend: Option[TimeServiceBackend],
      dbSupport: DbSupport,
      ledgerId: LedgerId,
      apiServerConfig: ApiServerConfig,
      participantId: Ref.ParticipantId,
      configAdaptor: BridgeConfigAdaptor,
  )(implicit
      actorSystem: ActorSystem,
      loggingContext: LoggingContext,
  ): ResourceOwner[ApiServer] =
    StandaloneApiServer(
      indexService = indexService,
      ledgerId = ledgerId,
      config = apiServerConfig,
      optWriteService = Some(writeService),
      healthChecks = healthChecksWithIndexer + ("write" -> writeService),
      metrics = metrics,
      timeServiceBackend = timeServiceBackend,
      otherInterceptors = List.empty,
      engine = sharedEngine,
      servicesExecutionContext = servicesExecutionContext,
      userManagementStore = PersistentUserManagementStore.cached(
        dbSupport = dbSupport,
        metrics = metrics,
        cacheExpiryAfterWriteInSeconds =
          apiServerConfig.userManagement.cacheExpiryAfterWriteInSeconds,
        maxCacheSize = apiServerConfig.userManagement.maxCacheSize,
        maxRightsPerUser = UserManagementConfig.MaxRightsPerUser,
        timeProvider = TimeProvider.UTC,
      )(servicesExecutionContext, loggingContext),
      ledgerFeatures = LedgerFeatures(
        staticTime = timeServiceBackend.isDefined,
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
      ),
      participantId = participantId,
      authService = configAdaptor.authService(apiServerConfig),
    )

  private def buildIndexerServer(
      metrics: Metrics,
      readService: ReadService,
      translationCache: LfValueTranslationCache.Cache,
      participantId: Ref.ParticipantId,
      participantConfig: ParticipantConfig,
  )(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
  ): ResourceOwner[HealthChecks] =
    for {
      indexerHealth <- new StandaloneIndexerServer(
        participantId = participantId,
        readService = readService,
        config = participantConfig.indexer,
        metrics = metrics,
        lfValueTranslationCache = translationCache,
      )
    } yield new HealthChecks(
      "read" -> readService,
      "indexer" -> indexerHealth,
    )

  private def buildServicesExecutionContext(
      metrics: Metrics,
      servicesThreadPoolSize: Int,
  ): ResourceOwner[ExecutionContextExecutorService] =
    ResourceOwner
      .forExecutorService(() =>
        new InstrumentedExecutorService(
          Executors.newWorkStealingPool(servicesThreadPoolSize),
          metrics.registry,
          metrics.daml.lapi.threadpool.apiServices.toString,
        )
      )
      .map(ExecutionContext.fromExecutorService)

  private def buildMetrics(participantId: Ref.ParticipantId)(implicit
      participantConfig: ParticipantConfig,
      config: Config,
  ): ResourceOwner[Metrics] =
    Metrics
      .fromSharedMetricRegistries(participantConfig.metricsRegistryName(participantId))
      .tap(_.registry.registerAll(new JvmMetricSet))
      .pipe { metrics =>
        config.metrics.reporter
          .fold(ResourceOwner.unit)(reporter =>
            ResourceOwner
              .forCloseable(() => reporter.register(metrics.registry))
              .map(_.start(config.metrics.reportingInterval.toMillis, TimeUnit.MILLISECONDS))
          )
          .map(_ => metrics)
      }

  // Builds the write service and uploads the initialization DARs
  private def buildWriteService(
      participantId: Ref.ParticipantId,
      feedSink: Sink[(Offset, Update), NotUsed],
      indexService: IndexService,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContext,
      servicesThreadPoolSize: Int,
      timeServiceBackend: Option[TimeServiceBackend],
      participantConfig: ParticipantConfig,
      bridgeConfig: BridgeConfig,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[WriteService] = {
    implicit val ec: ExecutionContext = servicesExecutionContext
    val bridgeMetrics = new BridgeMetrics(metrics)
    for {
      ledgerBridge <- LedgerBridge.owner(
        participantId,
        participantConfig,
        bridgeConfig,
        indexService,
        bridgeMetrics,
        servicesThreadPoolSize,
        timeServiceBackend.getOrElse(TimeProvider.UTC),
      )
      writeService <- ResourceOwner.forCloseable(() =>
        new BridgeWriteService(
          feedSink = feedSink,
          submissionBufferSize = bridgeConfig.submissionBufferSize,
          ledgerBridge = ledgerBridge,
          bridgeMetrics = bridgeMetrics,
        )
      )
    } yield writeService
  }

  private def logInitializationHeader(
      config: Config,
      participantId: Ref.ParticipantId,
      participantConfig: ParticipantConfig,
      extra: BridgeConfig,
  ): Unit = {
    val apiServerConfig = participantConfig.apiServer
    val authentication = apiServerConfig.authentication.create() match {
      case _: AuthServiceJWT => "JWT-based authentication"
      case AuthServiceNone => "none authenticated"
      case _: AuthServiceStatic => "static authentication"
      case AuthServiceWildcard => "all unauthenticated allowed"
      case other => other.getClass.getSimpleName
    }

    val ledgerDetails =
      Seq[(String, String)](
        "run-mode" -> s"${participantConfig.runMode} participant",
        "index DB backend" -> DbType
          .jdbcType(apiServerConfig.database.jdbcUrl)
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
}
