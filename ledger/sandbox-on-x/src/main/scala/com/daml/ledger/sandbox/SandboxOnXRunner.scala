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
import com.daml.error.ErrorCodesVersionSwitcher
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
import com.daml.ledger.participant.state.kvutils.app._
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v2.{ReadService, Update, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext.{newLoggingContext, newLoggingContextWith}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.apiserver._
import com.daml.platform.configuration.{PartyConfiguration, ServerRole}
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.usermanagement.PersistentUserManagementStore

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.chaining._

object SandboxOnXRunner {
  val RunnerName = "sandbox-on-x"
  private val logger = ContextualizedLogger.get(getClass)

  def owner(args: collection.Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(
        RunnerName,
        BridgeConfigProvider.extraConfigParser,
        BridgeConfigProvider.defaultExtraConfig,
        args,
      )
      .flatMap(owner)

  private def owner(originalConfig: Config[BridgeConfig]): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        val config = BridgeConfigProvider.manipulateConfig(originalConfig)
        val errorFactories = ErrorFactories(
          new ErrorCodesVersionSwitcher(originalConfig.enableSelfServiceErrorCodes)
        )

        config.mode match {
          case Mode.DumpIndexMetadata(jdbcUrls) =>
            DumpIndexMetadata(jdbcUrls, errorFactories, RunnerName)
            sys.exit(0)
          case Mode.Run =>
            run(config)
        }
      }
    }

  private def run(
      config: Config[BridgeConfig]
  )(implicit resourceContext: ResourceContext): Resource[Unit] = {
    implicit val actorSystem: ActorSystem = ActorSystem(RunnerName)
    implicit val materializer: Materializer = Materializer(actorSystem)

    for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits in a `for` comprehension.
      _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
      _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

      // Start the ledger
      participantConfig <- validateCombinedParticipantMode(config)
      _ <- buildLedger(
        config,
        participantConfig,
        materializer,
        actorSystem,
      ).acquire()
    } yield ()
  }

  def validateCombinedParticipantMode(
      config: Config[BridgeConfig]
  ): Resource[ParticipantConfig] =
    config.participants.toList match {
      case participantConfig :: Nil if participantConfig.mode == ParticipantRunMode.Combined =>
        Resource.successful(participantConfig)
      case _ =>
        Resource.failed {
          val loggingMessage = "Sandbox-on-X can only be run in a single COMBINED participant mode."
          newLoggingContext(logger.info(loggingMessage)(_))
          new IllegalArgumentException(loggingMessage)
        }
    }

  def buildLedger(implicit
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      materializer: Materializer,
      actorSystem: ActorSystem,
      metrics: Option[Metrics] = None,
  ): ResourceOwner[(ApiServer, WriteService)] = {
    implicit val apiServerConfig: ApiServerConfig =
      BridgeConfigProvider.apiServerConfig(participantConfig, config)
    val sharedEngine = new Engine(
      EngineConfig(
        allowedLanguageVersions = config.allowedLanguageVersions,
        profileDir = config.extra.profileDir,
        stackTraceMode = config.extra.stackTraces,
      )
    )

    newLoggingContextWith("participantId" -> participantConfig.participantId) {
      implicit loggingContext =>
        for {
          metrics <- metrics.map(ResourceOwner.successful).getOrElse(buildMetrics)
          translationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
            eventConfiguration = config.lfValueTranslationEventCache,
            contractConfiguration = config.lfValueTranslationContractCache,
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
            maxDedupSeconds = config.extra.maxDedupSeconds,
            stateUpdatesSource,
          )

          indexerHealthChecks <- buildIndexerServer(
            metrics,
            new TimedReadService(readServiceWithSubscriber, metrics),
            translationCache,
          )

          dbSupport <- DbSupport
            .owner(
              jdbcUrl = apiServerConfig.jdbcUrl,
              serverRole = ServerRole.ApiServer,
              connectionPoolSize = apiServerConfig.databaseConnectionPoolSize,
              connectionTimeout = apiServerConfig.databaseConnectionTimeout,
              metrics = metrics,
            )

          indexService <- StandaloneIndexService(
            ledgerId = config.ledgerId,
            config = apiServerConfig,
            metrics = metrics,
            engine = sharedEngine,
            servicesExecutionContext = servicesExecutionContext,
            lfValueTranslationCache = translationCache,
            dbSupport = dbSupport,
          )

          timeServiceBackend = BridgeConfigProvider.timeServiceBackend(config)

          writeService <- buildWriteService(
            stateUpdatesFeedSink,
            indexService,
            metrics,
            servicesExecutionContext,
            servicesThreadPoolSize,
            timeServiceBackend,
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
          )
        } yield {
          logInitializationHeader(config, participantConfig)
          apiServer -> writeService
        }
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
  )(implicit
      actorSystem: ActorSystem,
      loggingContext: LoggingContext,
      config: Config[BridgeConfig],
      apiServerConfig: ApiServerConfig,
  ): ResourceOwner[ApiServer] =
    StandaloneApiServer(
      indexService = indexService,
      ledgerId = config.ledgerId,
      config = apiServerConfig,
      commandConfig = config.commandConfig,
      submissionConfig = config.submissionConfig,
      partyConfig = PartyConfiguration(config.extra.implicitPartyAllocation),
      optWriteService = Some(writeService),
      authService = config.extra.authService,
      healthChecks = healthChecksWithIndexer + ("write" -> writeService),
      metrics = metrics,
      timeServiceBackend = timeServiceBackend,
      otherInterceptors = BridgeConfigProvider.interceptors(config),
      engine = sharedEngine,
      servicesExecutionContext = servicesExecutionContext,
      userManagementStore = PersistentUserManagementStore.cached(
        dbSupport = dbSupport,
        metrics = metrics,
        cacheExpiryAfterWriteInSeconds = config.userManagementConfig.cacheExpiryAfterWriteInSeconds,
        maximumCacheSize = config.userManagementConfig.maximumCacheSize,
      )(servicesExecutionContext),
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
    )

  private def buildIndexerServer(
      metrics: Metrics,
      readService: ReadService,
      translationCache: LfValueTranslationCache.Cache,
  )(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
      participantConfig: ParticipantConfig,
      config: Config[BridgeConfig],
  ): ResourceOwner[HealthChecks] =
    for {
      indexerHealth <- new StandaloneIndexerServer(
        readService = readService,
        config = BridgeConfigProvider.indexerConfig(participantConfig, config),
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

  private def buildMetrics(implicit
      participantConfig: ParticipantConfig,
      config: Config[BridgeConfig],
  ): ResourceOwner[Metrics] =
    BridgeConfigProvider
      .createMetrics(participantConfig, config)
      .tap(_.registry.registerAll(new JvmMetricSet))
      .pipe { metrics =>
        config.metricsReporter
          .fold(ResourceOwner.unit)(reporter =>
            ResourceOwner
              .forCloseable(() => reporter.register(metrics.registry))
              .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
          )
          .map(_ => metrics)
      }

  // Builds the write service and uploads the initialization DARs
  private def buildWriteService(
      feedSink: Sink[(Offset, Update), NotUsed],
      indexService: IndexService,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContext,
      servicesThreadPoolSize: Int,
      timeServiceBackend: Option[TimeServiceBackend],
  )(implicit
      materializer: Materializer,
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      loggingContext: LoggingContext,
  ): ResourceOwner[WriteService] = {
    implicit val ec: ExecutionContext = servicesExecutionContext
    val bridgeMetrics = new BridgeMetrics(metrics)
    for {
      ledgerBridge <- LedgerBridge.owner(
        config,
        participantConfig,
        indexService,
        bridgeMetrics,
        servicesThreadPoolSize,
        timeServiceBackend.getOrElse(TimeProvider.UTC),
      )
      writeService <- ResourceOwner.forCloseable(() =>
        new BridgeWriteService(
          feedSink = feedSink,
          submissionBufferSize = config.extra.submissionBufferSize,
          ledgerBridge = ledgerBridge,
          bridgeMetrics = bridgeMetrics,
        )
      )
    } yield writeService
  }

  private def logInitializationHeader(
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
  ): Unit = {
    val authentication = BridgeConfigProvider.authService(config) match {
      case _: AuthServiceJWT => "JWT-based authentication"
      case AuthServiceNone => "none authenticated"
      case _: AuthServiceStatic => "static authentication"
      case AuthServiceWildcard => "all unauthenticated allowed"
      case other => other.getClass.getSimpleName
    }

    val ledgerDetails =
      Seq[(String, String)](
        "run-mode" -> s"${participantConfig.mode} participant",
        "participant-id" -> participantConfig.participantId,
        "ledger-id" -> config.ledgerId,
        "port" -> participantConfig.port.toString,
        "time mode" -> config.extra.timeProviderType.description,
        "allowed language versions" -> s"[min = ${config.allowedLanguageVersions.min}, max = ${config.allowedLanguageVersions.max}]",
        "authentication" -> authentication,
        "contract ids seeding" -> config.seeding.toString,
      ).map { case (key, value) =>
        s"$key = $value"
      }.mkString(", ")

    logger.withoutContext.info(
      s"Initialized {} with {}, version {}, {}",
      RunnerName,
      if (config.extra.conflictCheckingEnabled) "conflict checking ledger bridge"
      else "pass-through ledger bridge (no conflict checking)",
      BuildInfo.Version,
      ledgerDetails,
    )
  }
}
