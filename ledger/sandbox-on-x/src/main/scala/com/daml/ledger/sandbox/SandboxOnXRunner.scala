// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.version_service.{
  CommandDeduplicationFeatures,
  DeduplicationPeriodSupport,
  ParticipantDeduplicationSupport,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.ledger.participant.state.index.v2.{IndexService, UserManagementStore}
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  DumpIndexMetadata,
  Mode,
  ParticipantConfig,
  ParticipantRunMode,
}
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v2.{ReadService, Update, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext.{newLoggingContext, newLoggingContextWith}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.apiserver.{
  ApiServer,
  ApiServerConfig,
  StandaloneApiServer,
  StandaloneIndexService,
}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.usermanagement.PersistentUserManagementStore
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
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

    val sharedEngine = new Engine(
      EngineConfig(config.allowedLanguageVersions, forbidV0ContractId = true)
    )

    for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits in a `for` comprehension.
      _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
      _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

      // Start the ledger
      participantConfig <- validateCombinedParticipantMode(config)
      _ <- buildLedger(sharedEngine)(
        config,
        participantConfig,
        materializer,
        actorSystem,
      ).acquire()
    } yield ()
  }

  private def validateCombinedParticipantMode(
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

  private def buildLedger(
      sharedEngine: Engine
  )(implicit
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      materializer: Materializer,
      actorSystem: ActorSystem,
  ): ResourceOwner[Unit] = {
    implicit val apiServerConfig: ApiServerConfig =
      BridgeConfigProvider.apiServerConfig(participantConfig, config)

    newLoggingContextWith("participantId" -> participantConfig.participantId) {
      implicit loggingContext =>
        for {
          metrics <- buildMetrics
          translationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
            eventConfiguration = config.lfValueTranslationEventCache,
            contractConfiguration = config.lfValueTranslationContractCache,
            metrics = metrics,
          )

          (stateUpdatesFeedSink, stateUpdatesSource) <- AkkaSubmissionsBridge()

          servicesExecutionContext <- buildServicesExecutionContext(metrics)

          readServiceWithSubscriber = new BridgeReadService(
            ledgerId = config.ledgerId,
            maxDedupSeconds = config.extra.maxDedupSeconds,
            stateUpdatesSource,
          )

          indexerHealthChecks <- buildIndexerServer(
            metrics,
            servicesExecutionContext,
            new TimedReadService(readServiceWithSubscriber, metrics),
            translationCache,
          )

          indexService <- standaloneIndexService(
            sharedEngine,
            config,
            apiServerConfig,
            metrics,
            translationCache,
            servicesExecutionContext,
          )

          writeService <- buildWriteService(stateUpdatesFeedSink, servicesExecutionContext)

          // TODO participant user management: Instantiate PersistentUserManagement if  DbDispatcher can be obtained
          userManagementStore = new InMemoryUserManagementStore
          _ <- buildStandaloneApiServer(
            sharedEngine,
            indexService,
            metrics,
            servicesExecutionContext,
            new TimedWriteService(writeService, metrics),
            indexerHealthChecks,
            userManagementStore = userManagementStore,
          )
        } yield ()
    }
  }

  private def standaloneIndexService(
      sharedEngine: Engine,
      config: Config[BridgeConfig],
      apiServerConfig: ApiServerConfig,
      metrics: Metrics,
      translationCache: LfValueTranslationCache.Cache,
      servicesExecutionContext: ExecutionContextExecutorService,
  )(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
  ): ResourceOwner[IndexService] =
    for {
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
    } yield indexService

  private def buildStandaloneApiServer(
      sharedEngine: Engine,
      indexService: IndexService,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContextExecutorService,
      writeService: WriteService,
      healthChecksWithIndexer: HealthChecks,
      userManagementStore: UserManagementStore,
  )(implicit
      actorSystem: ActorSystem,
      loggingContext: LoggingContext,
      config: Config[BridgeConfig],
      apiServerConfig: ApiServerConfig,
  ): ResourceOwner[ApiServer] = {
    StandaloneApiServer(
      indexService = indexService,
      ledgerId = config.ledgerId,
      config = apiServerConfig,
      commandConfig = config.commandConfig,
      submissionConfig = config.submissionConfig,
      partyConfig = BridgeConfigProvider.partyConfig(config),
      optWriteService = Some(writeService),
      authService = BridgeConfigProvider.authService(config),
      healthChecks = healthChecksWithIndexer + ("write" -> writeService),
      metrics = metrics,
      timeServiceBackend = BridgeConfigProvider.timeServiceBackend(config),
      otherInterceptors = BridgeConfigProvider.interceptors(config),
      engine = sharedEngine,
      servicesExecutionContext = servicesExecutionContext,
      userManagementStore = userManagementStore,
      commandDeduplicationFeatures = CommandDeduplicationFeatures.of(
        Some(
          DeduplicationPeriodSupport.of(
            DeduplicationPeriodSupport.OffsetSupport.OFFSET_NOT_SUPPORTED,
            DeduplicationPeriodSupport.DurationSupport.DURATION_NATIVE_SUPPORT,
          )
        ),
        ParticipantDeduplicationSupport.PARTICIPANT_DEDUPLICATION_NOT_SUPPORTED,
      ),
      userManagementBasedAuthorizer = PersistentUserManagementStore.cachedAuthorizer(
        userManagementStore = userManagementStore
      )(servicesExecutionContext, loggingContext),
    )
  }

  private def buildIndexerServer(
      metrics: Metrics,
      servicesExecutionContext: ExecutionContextExecutorService,
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
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        lfValueTranslationCache = translationCache,
      )
    } yield new HealthChecks(
      "read" -> readService,
      "indexer" -> indexerHealth,
    )

  private def buildServicesExecutionContext(
      metrics: Metrics
  ): ResourceOwner[ExecutionContextExecutorService] =
    ResourceOwner
      .forExecutorService(() =>
        new InstrumentedExecutorService(
          Executors.newWorkStealingPool(),
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
      servicesExecutionContext: ExecutionContext,
  )(implicit
      materializer: Materializer,
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      loggingContext: LoggingContext,
  ): ResourceOwner[BridgeWriteService] = {
    implicit val ec: ExecutionContext = servicesExecutionContext
    for {
      writeService <- ResourceOwner
        .forCloseable(() =>
          new BridgeWriteService(
            feedSink = feedSink,
            participantId = participantConfig.participantId,
            submissionBufferSize = config.extra.submissionBufferSize,
          )
        )
      _ <- ResourceOwner.forFuture(() =>
        Future.sequence(
          config.archiveFiles.map(path =>
            DefaultTelemetry.runFutureInSpan(SpanName.RunnerUploadDar, SpanKind.Internal) {
              implicit telemetryContext =>
                val submissionId = Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)
                for {
                  dar <- Future.fromTry(DarParser.readArchiveFromFile(path.toFile).toTry)
                  _ <- writeService.uploadPackages(submissionId, dar.all, None).toScala
                } yield ()
            }
          )
        )
      )
    } yield writeService
  }
}
