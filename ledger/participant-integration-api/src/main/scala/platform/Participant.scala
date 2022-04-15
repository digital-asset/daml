// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink}
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.api.util.TimeProvider
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
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.runner.common.{Config, ParticipantConfig}
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.ledger.sandbox._
import com.daml.lf.engine.{Engine, EngineConfig, ValueEnricher}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.apiserver._
import com.daml.platform.configuration.{PartyConfiguration, ServerRole}
import com.daml.platform.index.{LedgerBuffersUpdater, ParticipantInMemoryState}
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.store.appendonlydao.JdbcLedgerDao
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, DbType, LfValueTranslationCache}
import com.daml.platform.usermanagement.{PersistentUserManagementStore, UserManagementConfig}

import scala.util.chaining._

import scala.util.control

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

trait Participant {
  def apiServer: ApiServer
  def writeService: WriteService
  def indexService: IndexService
}

object Participant {
  def owner(implicit
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      ledgerName: String,
  ): ResourceOwner[Participant] = {
    implicit val apiServerConfig: ApiServerConfig =
      BridgeConfigProvider.apiServerConfig(participantConfig, config)

    val sharedEngine = new Engine(
      EngineConfig(
        allowedLanguageVersions = config.allowedLanguageVersions,
        profileDir = config.profileDir,
        stackTraceMode = config.stackTraces,
      )
    )

    newLoggingContextWith("participantId" -> participantConfig.participantId) {
      implicit loggingContext =>
        implicit val actorSystem: ActorSystem = ActorSystem(ledgerName)
        implicit val materializer: Materializer = Materializer(actorSystem)

        for {
          // Take ownership of the actor system and materializer so they're cleaned up properly.
          // This is necessary because we can't declare them as implicits in a `for` comprehension.
          _ <- ResourceOwner.forActorSystem(() => actorSystem)
          _ <- ResourceOwner.forMaterializer(() => materializer)

          metrics <- buildMetrics
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
            maximumDeduplicationDuration = config.maxDeduplicationDuration.getOrElse(
              BridgeConfigProvider.DefaultMaximumDeduplicationDuration
            ),
            stateUpdatesSource,
          )

          dbSupport <- DbSupport
            .owner(
              jdbcUrl = apiServerConfig.jdbcUrl,
              serverRole = ServerRole.ApiServer,
              connectionPoolSize = apiServerConfig.databaseConnectionPoolSize,
              connectionTimeout = apiServerConfig.databaseConnectionTimeout,
              metrics = metrics,
            )

          stringInterningView = createStringInterningView(apiServerConfig.jdbcUrl, metrics)
          ledgerEndCache = MutableLedgerEndCache()

          generalDispatcher <-
            Dispatcher.owner[Offset](
              name = "sql-ledger",
              zeroIndex = Offset.beforeBegin,
              headAtInitialization = Offset.beforeBegin,
            )

          updateLedgerApiLedgerEnd = (ledgerEnd: LedgerEnd) => {
            ledgerEndCache.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))
            // the order here is very important: first we need to make data available for point-wise lookups
            // and SQL queries, and only then we can make it available on the streams.
            // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
            generalDispatcher.signalNewHead(ledgerEnd.lastOffset)
          }

          participantInMemoryState = ParticipantInMemoryState.build(
            apiServerConfig = apiServerConfig,
            metrics = metrics,
            servicesExecutionContext = servicesExecutionContext,
            updateLedgerApiLedgerEnd = updateLedgerApiLedgerEnd,
          )

          ledgerApiBuffersUpdateFlow = LedgerBuffersUpdater.flow
            .mapAsync(1) { batch =>
              participantInMemoryState.updateBatch(batch)
            }

          indexerHealthChecks <- buildIndexerServer(
            metrics,
            new TimedReadService(readServiceWithSubscriber, metrics),
            translationCache,
            stringInterningView,
            ledgerApiBuffersUpdateFlow,
          )

          ledgerDao = JdbcLedgerDao.read(
            dbSupport = dbSupport,
            eventsPageSize = config.eventsPageSize,
            eventsProcessingParallelism = config.eventsProcessingParallelism,
            acsIdPageSize = config.acsIdPageSize,
            acsIdFetchingParallelism = config.acsIdFetchingParallelism,
            acsContractFetchingParallelism = config.acsContractFetchingParallelism,
            acsGlobalParallelism = config.acsGlobalParallelism,
            acsIdQueueLimit = config.acsIdQueueLimit,
            servicesExecutionContext = servicesExecutionContext,
            metrics = metrics,
            lfValueTranslationCache = translationCache,
            enricher = Some(new ValueEnricher(sharedEngine)),
            participantId = participantConfig.participantId,
            ledgerEndCache = ledgerEndCache,
            stringInterning = stringInterningView,
            materializer = materializer,
          )

          ledgerEnd <- ResourceOwner.forFuture(() => ledgerDao.lookupLedgerEnd())
          _ = updateLedgerApiLedgerEnd(ledgerEnd)

          indexServiceR <- StandaloneIndexService(
            ledgerId = config.ledgerId,
            config = apiServerConfig,
            metrics = metrics,
            engine = sharedEngine,
            servicesExecutionContext = servicesExecutionContext,
            lfValueTranslationCache = translationCache,
            generalDispatcher = generalDispatcher,
            ledgerReadDao = ledgerDao,
            participantInMemoryState = participantInMemoryState,
          )

          timeServiceBackend = BridgeConfigProvider.timeServiceBackend(config)

          writeServiceR <- buildWriteService(
            stateUpdatesFeedSink,
            indexServiceR,
            metrics,
            servicesExecutionContext,
            servicesThreadPoolSize,
            timeServiceBackend,
          )

          apiServerR <- buildStandaloneApiServer(
            sharedEngine,
            indexServiceR,
            metrics,
            servicesExecutionContext,
            new TimedWriteService(writeServiceR, metrics),
            indexerHealthChecks,
            timeServiceBackend,
            dbSupport,
          )
        } yield new Participant {
          override def apiServer: ApiServer = apiServerR

          override def writeService: WriteService = writeServiceR

          override def indexService: IndexService = indexServiceR
        }
    }
  }

  private def createStringInterningView(jdbcUrl: String, metrics: Metrics) = {
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType)
    val stringInterningStorageBackend = storageBackendFactory.createStringInterningStorageBackend

    new StringInterningView(
      loadPrefixedEntries = (fromExclusive, toInclusive, dbDispatcher) =>
        implicit loggingContext =>
          dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
    )
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
      partyConfig = PartyConfiguration(config.extra.implicitPartyAllocation),
      optWriteService = Some(writeService),
      authService = config.authService,
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
        maxCacheSize = config.userManagementConfig.maxCacheSize,
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
      userManagementConfig = config.userManagementConfig,
    )

  private def buildIndexerServer(
      metrics: Metrics,
      readService: ReadService,
      translationCache: LfValueTranslationCache.Cache,
      stringInterningView: StringInterningView,
      updateInMemoryBuffersFlow: Flow[(Iterable[(Offset, Update)], LedgerEndMarker), Unit, NotUsed],
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
        stringInterningView = stringInterningView,
        updateInMemoryBuffersFlow = updateInMemoryBuffersFlow,
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
}
