// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox

import error.ErrorCodesVersionSwitcher
import ledger.api.health.HealthChecks
import ledger.participant.state.index.v2.IndexService
import ledger.participant.state.kvutils.app.{Config, Mode, ParticipantRunMode}
import ledger.participant.state.v2.WritePackagesService
import ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import ledger.resources.{Resource, ResourceContext, ResourceOwner}
import lf.archive.DarParser
import lf.data.Ref
import lf.engine.{Engine, EngineConfig}
import logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import logging.{ContextualizedLogger, LoggingContext}
import metrics.JvmMetricSet
import platform.apiserver.{StandaloneApiServer, StandaloneIndexService}
import platform.indexer.StandaloneIndexerServer
import platform.server.api.validation.ErrorFactories
import platform.store.{IndexMetadata, LfValueTranslationCache}
import telemetry.{DefaultTelemetry, SpanKind, SpanName}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.InstrumentedExecutorService

import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class SandboxOnXRunner(
    name: String
) {
  def owner(args: collection.Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(name, BridgeConfig.extraConfigParser, BridgeConfig.defaultExtraConfig, args)
      .flatMap(owner)

  def owner(originalConfig: Config[BridgeConfig]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
      val config = BridgeConfig.manipulateConfig(originalConfig)
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
    implicit val actorSystem: ActorSystem = ActorSystem(
      "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-")
    )
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
    implicit val actorSystem: ActorSystem = ActorSystem(
      "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-")
    )
    implicit val materializer: Materializer = Materializer(actorSystem)

    val sharedEngine = new Engine(
      EngineConfig(
        config.allowedLanguageVersions,
        forbidV0ContractId = true,
      )
    )

    val indexServiceRef = new AtomicReference[Option[IndexService]](None)

    newLoggingContext { implicit loggingContext =>
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits in a `for` comprehension.
        _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
        _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

        // initialize all configured participants
        _ <- Resource.sequence(config.participants.map { participantConfig =>
          withEnrichedLoggingContext("participantId" -> participantConfig.participantId) {
            implicit loggingContext =>
              val metrics = BridgeConfig.createMetrics(participantConfig, config)
              metrics.registry.registerAll(new JvmMetricSet)
              val lfValueTranslationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
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
                // LEDGER needs INDEX_SERVICE
                soxReadService = new SoXReadService(config.ledgerId, 15 /* fill in */ )(
                  loggingContext,
                  materializer,
                )
                readService = new TimedReadService(soxReadService, metrics)
                indexerHealthCheck <- participantConfig.mode match {
                  case ParticipantRunMode.Combined | ParticipantRunMode.Indexer =>
                    // INDEXER needs READ_SERVICE
                    new StandaloneIndexerServer(
                      readService = readService,
                      config = BridgeConfig.indexerConfig(participantConfig, config),
                      servicesExecutionContext = servicesExecutionContext,
                      metrics = metrics,
                      lfValueTranslationCache = lfValueTranslationCache,
                    ).acquire().map(healthCheck => Seq("indexer" -> healthCheck))
                  case ParticipantRunMode.LedgerApiServer =>
                    Resource.successful(Seq.empty)
                }
                apiServerConfig = BridgeConfig.apiServerConfig(participantConfig, config)
                // INDEX_SERVICE needs the ledger initialized (needs INDEXER)
                indexService <- StandaloneIndexService(
                  ledgerId = config.ledgerId,
                  config = apiServerConfig,
                  metrics = metrics,
                  engine = sharedEngine,
                  servicesExecutionContext = servicesExecutionContext,
                  lfValueTranslationCache = lfValueTranslationCache,
                ).acquire()
                _ <- Resource.successful(indexServiceRef.set(Some(indexService)))
                writeService <- ConflictCheckingWriteService
                  .owner(
                    soxReadService,
                    participantId = participantConfig.participantId,
                    ledgerId = config.ledgerId,
                    // TODO SoX: Wire up
                    maxDedupSeconds = config.maxDeduplicationDuration
                      .getOrElse(Duration.ofSeconds(15))
                      .getSeconds
                      .toInt,
                    // TODO Sox: Configure
                    submissionBufferSize = 128,
                    // TODO Sox: Remove
                    indexService = indexService,
                    metrics = metrics,
                    implicitPartyAllocation = false, // TODO SoX: Wire up
                  )(materializer, loggingContext)
                  .acquire()
                timedWriteService = new TimedWriteService(writeService, metrics)
                healthChecks = new HealthChecks(
                  "read" -> readService,
                  "write" -> timedWriteService,
                )
                _ <- Resource.sequence(
                  config.archiveFiles.map(path =>
                    Resource.fromFuture(
                      uploadDar(path, timedWriteService)(
                        loggingContext,
                        resourceContext.executionContext,
                      )
                    )
                  )
                )
                _ <- participantConfig.mode match {
                  case ParticipantRunMode.Combined | ParticipantRunMode.LedgerApiServer =>
                    StandaloneApiServer(
                      indexService = indexService,
                      ledgerId = config.ledgerId,
                      config = apiServerConfig,
                      commandConfig = config.commandConfig,
                      submissionConfig = config.submissionConfig,
                      partyConfig = BridgeConfig.partyConfig(config),
                      optWriteService = Some(timedWriteService),
                      authService = BridgeConfig.authService(config),
                      healthChecks =
                        indexerHealthCheck.headOption.fold(healthChecks)(healthChecks + _),
                      metrics = metrics,
                      timeServiceBackend = BridgeConfig.timeServiceBackend(config),
                      otherInterceptors = BridgeConfig.interceptors(config),
                      engine = sharedEngine,
                      servicesExecutionContext = servicesExecutionContext,
                    ).acquire()
                  case ParticipantRunMode.Indexer =>
                    Resource.unit
                }
              } yield ()
          }
        })
      } yield ()
    }
  }

  private def uploadDar(from: Path, to: WritePackagesService)(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Unit] = DefaultTelemetry.runFutureInSpan(SpanName.RunnerUploadDar, SpanKind.Internal) {
    implicit telemetryContext =>
      val submissionId = Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)
      for {
        dar <- Future.fromTry(DarParser.readArchiveFromFile(from.toFile).toTry)
        _ <- to.uploadPackages(submissionId, dar.all, None).toScala
      } yield ()
  }
}
