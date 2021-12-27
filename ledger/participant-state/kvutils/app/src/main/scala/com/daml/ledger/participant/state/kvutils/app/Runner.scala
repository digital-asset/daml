// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.version_service.{
  CommandDeduplicationFeatures,
  DeduplicationPeriodSupport,
  ParticipantDeduplicationSupport,
}
import com.daml.ledger.participant.state.v2.WritePackagesService
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import com.daml.metrics.JvmMetricSet
import com.daml.platform.apiserver.services.ApiVersionService
import com.daml.platform.apiserver.{StandaloneApiServer, StandaloneIndexService}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.usermanagement.PersistentUserManagementStore
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}

final class Runner[T <: ReadWriteService, Extra](
    name: String,
    factory: LedgerFactory[Extra],
    configProvider: ConfigProvider[Extra],
) {
  def owner(args: collection.Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(name, configProvider.extraConfigParser, configProvider.defaultExtraConfig, args)
      .flatMap(owner)

  def owner(originalConfig: Config[Extra]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
      val config = configProvider.manipulateConfig(originalConfig)
      val errorFactories = ErrorFactories(
        new ErrorCodesVersionSwitcher(originalConfig.enableSelfServiceErrorCodes)
      )

      config.mode match {
        case Mode.DumpIndexMetadata(jdbcUrls) =>
          DumpIndexMetadata(jdbcUrls, errorFactories, name)
          sys.exit(0)
        case Mode.Run =>
          run(config)
      }
    }
  }

  private def run(
      config: Config[Extra]
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
              val metrics = configProvider.createMetrics(participantConfig, config)
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
                ledgerFactory <- factory
                  .readWriteServiceFactoryOwner(
                    config,
                    participantConfig,
                    sharedEngine,
                    metrics,
                  )(materializer, servicesExecutionContext, loggingContext)
                  .acquire()
                writePackageService = ledgerFactory.writePackagesService()
                _ <- Resource.sequence(
                  config.archiveFiles.map(path =>
                    Resource.fromFuture(
                      uploadDar(path, writePackageService)(
                        loggingContext,
                        resourceContext.executionContext,
                      )
                    )
                  )
                )
                healthChecksWithIndexer <- participantConfig.mode match {
                  case ParticipantRunMode.Combined | ParticipantRunMode.Indexer =>
                    val readService = new TimedReadService(ledgerFactory.readService(), metrics)
                    for {
                      indexerHealth <- new StandaloneIndexerServer(
                        readService = readService,
                        config = configProvider.indexerConfig(participantConfig, config),
                        servicesExecutionContext = servicesExecutionContext,
                        metrics = metrics,
                        lfValueTranslationCache = lfValueTranslationCache,
                      ).acquire()
                    } yield {
                      new HealthChecks(
                        "read" -> readService,
                        "indexer" -> indexerHealth,
                      )
                    }
                  case ParticipantRunMode.LedgerApiServer =>
                    Resource.successful(new HealthChecks())
                }
                apiServerConfig = configProvider.apiServerConfig(participantConfig, config)
                _ <- participantConfig.mode match {
                  case ParticipantRunMode.Combined | ParticipantRunMode.LedgerApiServer =>
                    for {
                      dbSupport <- DbSupport
                        .owner(
                          jdbcUrl = apiServerConfig.jdbcUrl,
                          serverRole = ServerRole.ApiServer,
                          connectionPoolSize = apiServerConfig.databaseConnectionPoolSize,
                          connectionTimeout = apiServerConfig.databaseConnectionTimeout,
                          metrics = metrics,
                        )
                        .acquire()
                      userManagementStore =
                        new PersistentUserManagementStore(
                          dbDispatcher = dbSupport.dbDispatcher,
                          metrics = metrics,
                          maxNumberOfUserRightsPerUser =
                            ApiVersionService.MaxNumberOfUserRightsPerUser,
                        )
                      indexService <- StandaloneIndexService(
                        dbSupport = dbSupport,
                        ledgerId = config.ledgerId,
                        config = apiServerConfig,
                        metrics = metrics,
                        engine = sharedEngine,
                        servicesExecutionContext = servicesExecutionContext,
                        lfValueTranslationCache = lfValueTranslationCache,
                      ).acquire()
                      factory = new KeyValueDeduplicationSupportFactory(
                        ledgerFactory,
                        config,
                        indexService,
                      )(implicitly, servicesExecutionContext)
                      writeService = new TimedWriteService(factory.writeService(), metrics)
                      _ <- StandaloneApiServer(
                        indexService = indexService,
                        userManagementStore = userManagementStore,
                        ledgerId = config.ledgerId,
                        config = apiServerConfig,
                        commandConfig = config.commandConfig,
                        submissionConfig = config.submissionConfig,
                        partyConfig = configProvider.partyConfig(config),
                        optWriteService = Some(writeService),
                        authService = configProvider.authService(config),
                        healthChecks = healthChecksWithIndexer + ("write" -> writeService),
                        metrics = metrics,
                        timeServiceBackend = configProvider.timeServiceBackend(config),
                        otherInterceptors = configProvider.interceptors(config),
                        engine = sharedEngine,
                        servicesExecutionContext = servicesExecutionContext,
                        commandDeduplicationFeatures = CommandDeduplicationFeatures.of(
                          Some(
                            DeduplicationPeriodSupport.of(
                              offsetSupport =
                                DeduplicationPeriodSupport.OffsetSupport.OFFSET_CONVERT_TO_DURATION,
                              durationSupport =
                                DeduplicationPeriodSupport.DurationSupport.DURATION_NATIVE_SUPPORT,
                            )
                          ),
                          ParticipantDeduplicationSupport.PARTICIPANT_DEDUPLICATION_NOT_SUPPORTED,
                        ),
                      ).acquire()
                    } yield {}
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
