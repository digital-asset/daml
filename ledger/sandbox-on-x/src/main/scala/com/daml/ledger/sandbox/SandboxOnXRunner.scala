// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  Mode,
  ParticipantConfig,
  ParticipantRunMode,
}
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v2.{ReadService, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.{BridgeConfig, BridgeConfigProvider, BridgeWriteService}
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext.{newLoggingContext, newLoggingContextWith}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.apiserver.{StandaloneApiServer, StandaloneIndexService}
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{IndexMetadata, LfValueTranslationCache}
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.chaining._
import scala.util.{Failure, Success}

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

  def owner(originalConfig: Config[BridgeConfig]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
      val config = BridgeConfigProvider.manipulateConfig(originalConfig)
      val errorFactories = ErrorFactories(
        new ErrorCodesVersionSwitcher(originalConfig.enableSelfServiceErrorCodes)
      )

      config.mode match {
        case Mode.DumpIndexMetadata(jdbcUrls) =>
          dumpIndexMetadata(jdbcUrls, errorFactories)
          sys.exit(0)
        case Mode.Run =>
          run(config)
      }
    }
  }

  private def dumpIndexMetadata(
      jdbcUrls: Seq[String],
      errorFactories: ErrorFactories,
  )(implicit resourceContext: ResourceContext): Resource[Unit] = {
    val logger = ContextualizedLogger.get(this.getClass)
    import ExecutionContext.Implicits.global

    implicit val actorSystem: ActorSystem = ActorSystem(RunnerName)
    implicit val materializer: Materializer = Materializer(actorSystem)

    Resource.sequenceIgnoringValues(for (jdbcUrl <- jdbcUrls) yield {
      newLoggingContext { implicit loggingContext: LoggingContext =>
        Resource.fromFuture(IndexMetadata.read(jdbcUrl, errorFactories).andThen {
          case Failure(exception) =>
            logger.error("Error while retrieving the index metadata", exception)
          case Success(metadata) =>
            logger.warn(s"ledger_id: ${metadata.ledgerId}")
            logger.warn(s"participant_id: ${metadata.participantId}")
            logger.warn(s"ledger_end: ${metadata.ledgerEnd}")
            logger.warn(s"version: ${metadata.participantIntegrationApiVersion}")
        })
      }
    })
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
      _ <- buildLedger(config, participantConfig, sharedEngine)
    } yield ()
  }

  private def validateCombinedParticipantMode(config: Config[BridgeConfig]) =
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
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      sharedEngine: Engine,
  )(implicit
      resourceContext: ResourceContext,
      materializer: Materializer,
      actorSystem: ActorSystem,
  ) = {
    val apiServerConfig = BridgeConfigProvider.apiServerConfig(participantConfig, config)

    def buildStandaloneApiServer(
        sharedEngine: Engine,
        indexService: IndexService,
        metrics: Metrics,
        servicesExecutionContext: ExecutionContextExecutorService,
        writeService: WriteService,
        healthChecksWithIndexer: HealthChecks,
    )(implicit loggingContext: LoggingContext) =
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
      ).acquire()

    def buildIndexerServer(
        metrics: Metrics,
        servicesExecutionContext: ExecutionContextExecutorService,
        readService: ReadService,
        translationCache: LfValueTranslationCache.Cache,
    )(implicit loggingContext: LoggingContext) =
      for {
        indexerHealth <- new StandaloneIndexerServer(
          readService = readService,
          config = BridgeConfigProvider.indexerConfig(participantConfig, config),
          servicesExecutionContext = servicesExecutionContext,
          metrics = metrics,
          lfValueTranslationCache = translationCache,
        ).acquire()
      } yield new HealthChecks(
        "read" -> readService,
        "indexer" -> indexerHealth,
      )

    def buildServicesExecutionContext(metrics: Metrics) =
      ResourceOwner
        .forExecutorService(() =>
          new InstrumentedExecutorService(
            Executors.newWorkStealingPool(),
            metrics.registry,
            metrics.daml.lapi.threadpool.apiServices.toString,
          )
        )
        .map(ExecutionContext.fromExecutorService)
        .acquire()

    def buildMetrics =
      BridgeConfigProvider
        .createMetrics(participantConfig, config)
        .tap(_.registry.registerAll(new JvmMetricSet))
        .pipe { metrics =>
          config.metricsReporter
            .fold(Resource.unit)(reporter =>
              ResourceOwner
                .forCloseable(() => reporter.register(metrics.registry))
                .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
                .acquire()
            )
            .map(_ => metrics)
        }

    // Builds the write service and uploads the initialization DARs
    def buildWriteService(
        readServiceWithFeedSubscriber: ReadServiceWithFeedSubscriber
    )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext) =
      for {
        writeService <- BridgeWriteService
          .owner(
            readServiceWithFeedSubscriber = readServiceWithFeedSubscriber,
            config = config,
            participantConfig = participantConfig,
          )
          .acquire()
        _ <- Resource.sequence(
          config.archiveFiles.map(path =>
            Resource.fromFuture(
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

    newLoggingContextWith("participantId" -> participantConfig.participantId) {
      implicit loggingContext =>
        for {
          metrics <- buildMetrics
          translationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
            eventConfiguration = config.lfValueTranslationEventCache,
            contractConfiguration = config.lfValueTranslationContractCache,
            metrics = metrics,
          )

          servicesExecutionContext <- buildServicesExecutionContext(metrics)

          readServiceWithSubscriber = ReadServiceWithFeedSubscriber(
            ledgerId = config.ledgerId,
            maxDedupSeconds = config.extra.maxDedupSeconds,
          )

          indexerHealthChecks <- buildIndexerServer(
            metrics,
            servicesExecutionContext,
            new TimedReadService(readServiceWithSubscriber, metrics),
            translationCache,
          )

          indexService <- StandaloneIndexService(
            ledgerId = config.ledgerId,
            config = apiServerConfig,
            metrics = metrics,
            engine = sharedEngine,
            servicesExecutionContext = servicesExecutionContext,
            lfValueTranslationCache = translationCache,
          ).acquire()

          writeService <- buildWriteService(readServiceWithSubscriber)(
            servicesExecutionContext,
            loggingContext,
          )

          _ <- buildStandaloneApiServer(
            sharedEngine,
            indexService,
            metrics,
            servicesExecutionContext,
            new TimedWriteService(writeService, metrics),
            indexerHealthChecks,
          )
        } yield ()
    }
  }
}
