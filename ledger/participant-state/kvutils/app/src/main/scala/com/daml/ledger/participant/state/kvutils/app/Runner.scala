// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.util.concurrent.{Executors, TimeUnit}
import akka.actor.ActorSystem
import akka.stream.Materializer
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
import com.daml.ledger.runner.common.{
  Config,
  ConfigProvider,
  DumpIndexMetadata,
  Mode,
  ParticipantConfig,
  ParticipantRunMode,
}
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.JvmMetricSet
import com.daml.platform.apiserver.{LedgerFeatures, StandaloneApiServer, StandaloneIndexService}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.usermanagement.{PersistentUserManagementStore, UserManagementConfig}
import com.daml.ports.Port

import scala.concurrent.ExecutionContext

final class Runner[T <: ReadWriteService, Extra](
    name: String,
    factory: LedgerFactory[Extra],
    configProvider: ConfigProvider[Extra],
) {
  private val cleanedName = "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-")
  private val logger = ContextualizedLogger.get(getClass)

  def owner(args: collection.Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(name, configProvider.extraConfigParser, configProvider.defaultExtraConfig, args)
      .flatMap(owner)

  def owner(originalConfig: Config[Extra]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
      val config = configProvider.manipulateConfig(originalConfig)
      config.mode match {
        case Mode.DumpIndexMetadata(jdbcUrls) =>
          DumpIndexMetadata(jdbcUrls, name)
          sys.exit(0)
        case Mode.Run =>
          run(config)
      }
    }
  }

  private[app] def run(
      config: Config[Extra]
  )(implicit resourceContext: ResourceContext): Resource[Unit] = {
    implicit val actorSystem: ActorSystem = ActorSystem(cleanedName)
    implicit val materializer: Materializer = Materializer(actorSystem)

    newLoggingContext { implicit loggingContext =>
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits in a `for` comprehension.
        _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
        _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

        sharedEngine = new Engine(
          EngineConfig(
            allowedLanguageVersions = config.allowedLanguageVersions,
            forbidV0ContractId = true,
            profileDir = config.profileDir,
            stackTraceMode = config.stackTraces,
          )
        )

        // initialize all configured participants
        _ <- Resource.sequenceIgnoringValues(
          config.participants.map(participantConfig =>
            runParticipant(config, participantConfig, sharedEngine)
          )
        )
      } yield logInitializationHeader(config)
    }
  }

  private def logInitializationHeader(config: Config[Extra]): Unit = {
    val authentication = configProvider.authService(config) match {
      case _: AuthServiceJWT => "JWT-based authentication"
      case AuthServiceNone => "none authenticated"
      case _: AuthServiceStatic => "static authentication"
      case AuthServiceWildcard => "all unauthenticated allowed"
      case other => other.getClass.getSimpleName
    }
    val participantsInitializationText = config.participants
      .map(participantConfig =>
        s"{participant-id = ${participantConfig.participantId}, shared-name = ${participantConfig.shardName}, run-mode = ${participantConfig.mode}, port = ${participantConfig.port.toString}}"
      )
      .mkString("[", ", ", "]")
    logger.withoutContext.info(
      s"Initialized {} version {} with ledger-id = {}, ledger = {}, allowed language versions = {}, authentication = {}, contract ids seeding = {} with participants: {}",
      name,
      BuildInfo.Version,
      config.ledgerId,
      factory.ledgerName,
      s"[min = ${config.allowedLanguageVersions.min}, max = ${config.allowedLanguageVersions.max}]",
      authentication,
      config.seeding,
      participantsInitializationText,
    )
  }

  private[app] def runParticipant(
      config: Config[Extra],
      participantConfig: ParticipantConfig,
      sharedEngine: Engine,
  )(implicit
      resourceContext: ResourceContext,
      loggingContext: LoggingContext,
      actorSystem: ActorSystem,
      materializer: Materializer,
  ): Resource[Option[Port]] =
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
          healthChecksWithIndexer <- participantConfig.mode match {
            case ParticipantRunMode.Combined | ParticipantRunMode.Indexer =>
              val readService = new TimedReadService(ledgerFactory.readService(), metrics)
              for {
                indexerHealth <- new StandaloneIndexerServer(
                  readService = readService,
                  config = configProvider.indexerConfig(participantConfig, config),
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
          port <- participantConfig.mode match {
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
                userManagementStore = PersistentUserManagementStore.cached(
                  dbSupport = dbSupport,
                  metrics = metrics,
                  cacheExpiryAfterWriteInSeconds =
                    config.userManagementConfig.cacheExpiryAfterWriteInSeconds,
                  maxCacheSize = config.userManagementConfig.maxCacheSize,
                  maxRightsPerUser = UserManagementConfig.MaxRightsPerUser,
                  timeProvider = TimeProvider.UTC,
                )(servicesExecutionContext, loggingContext)
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
                  indexService,
                )(implicitly, servicesExecutionContext)
                writeService = new TimedWriteService(factory.writeService(), metrics)
                timeServiceBackend = configProvider.timeServiceBackend(config)
                apiServer <- StandaloneApiServer(
                  indexService = indexService,
                  userManagementStore = userManagementStore,
                  ledgerId = config.ledgerId,
                  config = apiServerConfig,
                  commandConfig = config.commandConfig,
                  partyConfig = configProvider.partyConfig(config),
                  optWriteService = Some(writeService),
                  authService = configProvider.authService(config),
                  healthChecks = healthChecksWithIndexer + ("write" -> writeService),
                  metrics = metrics,
                  timeServiceBackend = timeServiceBackend,
                  otherInterceptors = configProvider.interceptors(config),
                  engine = sharedEngine,
                  servicesExecutionContext = servicesExecutionContext,
                  ledgerFeatures = LedgerFeatures(
                    staticTime = timeServiceBackend.isDefined,
                    commandDeduplicationFeatures = CommandDeduplicationFeatures.of(
                      deduplicationPeriodSupport = Some(
                        CommandDeduplicationPeriodSupport.of(
                          offsetSupport =
                            CommandDeduplicationPeriodSupport.OffsetSupport.OFFSET_CONVERT_TO_DURATION,
                          durationSupport =
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
                  userManagementConfig = config.userManagementConfig,
                ).acquire()
              } yield Some(apiServer.port)
            case ParticipantRunMode.Indexer =>
              Resource.successful(None)
          }
        } yield port
    }
}
