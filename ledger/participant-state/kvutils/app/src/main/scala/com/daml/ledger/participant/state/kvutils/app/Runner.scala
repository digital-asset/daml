// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.participant.state.kvutils.app.Config.EngineMode
import com.daml.ledger.participant.state.v1.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v1.{SubmissionId, WritePackagesService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.archive.DarReader
import com.daml.lf.engine._
import com.daml.lf.language.LanguageVersion
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.JvmMetricSet
import com.daml.platform.apiserver.StandaloneApiServer
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.store.{IndexMetadata, LfValueTranslationCache}
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class Runner[T <: ReadWriteService, Extra](
    name: String,
    factory: LedgerFactory[ReadWriteService, Extra],
) {
  def owner(args: collection.Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(name, factory.extraConfigParser, factory.defaultExtraConfig, args)
      .flatMap(owner)

  def owner(originalConfig: Config[Extra]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
      val config = factory.manipulateConfig(originalConfig)

      config.mode match {
        case Mode.DumpIndexMetadata(jdbcUrls) =>
          dumpIndexMetadata(jdbcUrls, originalConfig.enableAppendOnlySchema)
          sys.exit(0)
        case Mode.Run =>
          run(config)
      }
    }
  }

  private def dumpIndexMetadata(
      jdbcUrls: Seq[String],
      enableAppendOnlySchema: Boolean,
  )(implicit resourceContext: ResourceContext): Resource[Unit] = {
    val logger = ContextualizedLogger.get(this.getClass)
    import ExecutionContext.Implicits.global
    Resource.sequenceIgnoringValues(for (jdbcUrl <- jdbcUrls) yield {
      newLoggingContext { implicit loggingContext: LoggingContext =>
        Resource.fromFuture(IndexMetadata.read(jdbcUrl, enableAppendOnlySchema).andThen {
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
      config: Config[Extra]
  )(implicit resourceContext: ResourceContext): Resource[Unit] = {
    implicit val actorSystem: ActorSystem = ActorSystem(
      "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-")
    )
    implicit val materializer: Materializer = Materializer(actorSystem)

    val allowedLanguageVersions =
      config.engineMode match {
        case EngineMode.Stable => LanguageVersion.StableVersions
        case EngineMode.EarlyAccess => LanguageVersion.EarlyAccessVersions
        case EngineMode.Dev => LanguageVersion.DevVersions
      }
    val sharedEngine = new Engine(EngineConfig(allowedLanguageVersions))

    newLoggingContext { implicit loggingContext =>
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits in a `for` comprehension.
        _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
        _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

        // initialize all configured participants
        _ <- Resource.sequence(config.participants.map { participantConfig =>
          val metrics = factory.createMetrics(participantConfig, config)
          metrics.registry.registerAll(new JvmMetricSet)
          val lfValueTranslationCache =
            LfValueTranslationCache.Cache.newInstrumentedInstance(
              eventConfiguration = config.lfValueTranslationEventCache,
              contractConfiguration = config.lfValueTranslationContractCache,
              metrics = metrics,
            )
          for {
            _ <- config.metricsReporter.fold(Resource.unit)(reporter =>
              ResourceOwner
                .forCloseable(() => reporter.register(metrics.registry))
                .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
                .acquire()
            )
            ledger <- factory
              .readWriteServiceOwner(config, participantConfig, sharedEngine)
              .acquire()
            readService = new TimedReadService(ledger, metrics)
            writeService = new TimedWriteService(ledger, metrics)
            healthChecks = new HealthChecks(
              "read" -> readService,
              "write" -> writeService,
            )
            _ <- Resource.sequence(
              config.archiveFiles.map(path =>
                Resource.fromFuture(uploadDar(path, writeService)(resourceContext.executionContext))
              )
            )
            servicesExecutionContext <- ResourceOwner
              .forExecutorService(() =>
                new InstrumentedExecutorService(
                  Executors.newWorkStealingPool(),
                  metrics.registry,
                  metrics.daml.lapi.threadpool.apiServices.toString,
                )
              )
              .map(ExecutionContext.fromExecutorService)
              .acquire()
            _ <- participantConfig.mode match {
              case ParticipantRunMode.Combined | ParticipantRunMode.Indexer =>
                new StandaloneIndexerServer(
                  readService = readService,
                  config = factory.indexerConfig(participantConfig, config),
                  servicesExecutionContext = servicesExecutionContext,
                  metrics = metrics,
                  lfValueTranslationCache = lfValueTranslationCache,
                ).acquire()
              case ParticipantRunMode.LedgerApiServer =>
                Resource.unit
            }
            _ <- participantConfig.mode match {
              case ParticipantRunMode.Combined | ParticipantRunMode.LedgerApiServer =>
                new StandaloneApiServer(
                  ledgerId = config.ledgerId,
                  config = factory.apiServerConfig(participantConfig, config),
                  commandConfig = factory.commandConfig(participantConfig, config),
                  partyConfig = factory.partyConfig(config),
                  ledgerConfig = factory.ledgerConfig(config),
                  optWriteService = Some(writeService),
                  authService = factory.authService(config),
                  healthChecks = healthChecks,
                  metrics = metrics,
                  timeServiceBackend = factory.timeServiceBackend(config),
                  otherInterceptors = factory.interceptors(config),
                  engine = sharedEngine,
                  servicesExecutionContext = servicesExecutionContext,
                  lfValueTranslationCache = lfValueTranslationCache,
                ).acquire()
              case ParticipantRunMode.Indexer =>
                Resource.unit
            }
          } yield ()
        })
      } yield ()
    }
  }

  private def uploadDar(from: Path, to: WritePackagesService)(implicit
      executionContext: ExecutionContext
  ): Future[Unit] = DefaultTelemetry.runFutureInSpan(SpanName.RunnerUploadDar, SpanKind.Internal) {
    implicit telemetryContext =>
      val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
      for {
        dar <- Future(
          DarReader[Archive] { case (_, x) => Try(Archive.parseFrom(x)) }
            .readArchiveFromFile(from.toFile)
            .get
        )
        _ <- to.uploadPackages(submissionId, dar.all, None).toScala
      } yield ()
  }
}
