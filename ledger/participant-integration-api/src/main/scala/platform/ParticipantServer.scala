// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalContractIds,
}
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.metrics.TimedWriteService
import com.daml.ledger.participant.state.v2.{ReadService, WriteService}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.apiserver._
import com.daml.platform.configuration.ServerRole
import com.daml.platform.index.LedgerBuffersUpdater
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend.ParameterStorageBackend
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.usermanagement.{PersistentUserManagementStore, UserManagementConfig}

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.chaining._

// TODO LLP: Consider renaming to LedgerApiServer??

// Participant init flow
//  INDEXER (needs ParticipantInMemoryState <- needs the LedgerEnd - to initialize the dispatcher and ContractStateCaches)
//    - IndexDB initialized? If not, create params table [HA]
//    - read ledgerEnd (Indexer)
//    - Initialize ParallelIngestion [HA]
//
// INDEX SVC
//    - read ledgerEnd
//    - verify ledgerId
//    - build in-memory state (mcsc and eventsBuffer) - ParticipantInMemoryState
//    DONE
//
class ParticipantServer(
    participantId: Ref.ParticipantId,
    ledgerId: LedgerId,
    participantConfig: ParticipantConfig,
    engineConfig: EngineConfig,
    metricsConfig: MetricsConfig,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    buildWriteService: (
        IndexService,
        Metrics,
        ExecutionContext,
        Int,
        Option[TimeServiceBackend],
    ) => ResourceOwner[WriteService],
    readService: ReadService,
    timeServiceBackendO: Option[TimeServiceBackend],
    authService: AuthService,
    metrics: Option[Metrics] = None, // TODO LLP: Why None here?
)(implicit materializer: Materializer, actorSystem: ActorSystem) {
  def owner: ResourceOwner[(ApiServer, WriteService, IndexService)] = {
    val sharedEngine = new Engine(engineConfig)

    newLoggingContextWith("participantId" -> participantId) { implicit loggingContext =>
      for {
        metrics <- metrics
          .map(ResourceOwner.successful)
          .getOrElse(buildMetrics(participantId))

        translationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
          config = participantConfig.lfValueTranslationCache,
          metrics = metrics,
        )

        servicesThreadPoolSize = Runtime.getRuntime.availableProcessors()
        servicesExecutionContext <- buildServicesExecutionContext(
          metrics,
          servicesThreadPoolSize,
        )

        dbSupport <- DbSupport
          .owner(
            serverRole = ServerRole.ApiServer,
            metrics = metrics,
            dbConfig = participantConfig.dataSourceProperties.createDbConfig(
              participantDataSourceConfig
            ),
          )

        // ParticipantInMemoryState: first step initialization
        participantInMemoryState <- ParticipantInMemoryState.owner(
          apiStreamShutdownTimeout = participantConfig.indexService.apiStreamShutdownTimeout,
          bufferedStreamsPageSize = participantConfig.indexService.bufferedStreamsPageSize,
          maxContractStateCacheSize = participantConfig.indexService.maxContractStateCacheSize,
          maxContractKeyStateCacheSize =
            participantConfig.indexService.maxContractKeyStateCacheSize,
          metrics = metrics,
          maxTransactionsInMemoryFanOutBufferSize =
            participantConfig.indexService.maxTransactionsInMemoryFanOutBufferSize,
        )

        ledgerBuffersUpdater = new LedgerBuffersUpdater(
          participantInMemoryState = participantInMemoryState,
          prepareUpdatesParallelism = 2, // TODO LLP: CLI
          metrics = metrics,
        )

        indexerHealthChecks <-
          for {
            indexerHealth <- new StandaloneIndexerServer(
              participantId = participantId,
              participantDataSourceConfig = participantDataSourceConfig,
              readService = readService,
              config = participantConfig.indexer,
              metrics = metrics,
              stringInterningView = participantInMemoryState.stringInterningView,
              lfValueTranslationCache = translationCache,
              // TODO LLP: Rename
              apiUpdaterFlow = { offset =>
                participantInMemoryState.reset(offset)
                ledgerBuffersUpdater.flow
              },
            )
          } yield new HealthChecks(
            "read" -> readService,
            "indexer" -> indexerHealth,
          )

        ledgerEnd <- fetchLedgerEnd(dbSupport, metrics)
        // ParticipantInMemoryState: second step initialization
        _ <- participantInMemoryState.initialized(ledgerEnd)

        // Needs ParticipantInMemoryState
        indexService <- StandaloneIndexService(
          ledgerId = ledgerId,
          initLedgerEnd = ledgerEnd,
          config = participantConfig.indexService,
          metrics = metrics,
          engine = sharedEngine,
          servicesExecutionContext = servicesExecutionContext,
          lfValueTranslationCache = translationCache,
          participantId = participantId,
          participantInMemoryState = participantInMemoryState,
          dbSupport = dbSupport,
          stringInterningView = stringInterningView,
        )

        writeService <- buildWriteService(
          indexService,
          metrics,
          servicesExecutionContext,
          servicesThreadPoolSize,
          timeServiceBackendO,
        )

        apiServer <- buildApiServer(
          sharedEngine,
          indexService,
          metrics,
          servicesExecutionContext,
          new TimedWriteService(writeService, metrics),
          indexerHealthChecks,
          timeServiceBackendO,
          dbSupport,
          ledgerId,
          participantConfig.apiServer,
          participantId,
        )
      } yield (apiServer, writeService, indexService)
    }
  }

  private def fetchLedgerEnd(dbSupport: DbSupport, metrics: Metrics)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[ParameterStorageBackend.LedgerEnd] = {
    val dbDispatcher = dbSupport.dbDispatcher
    ResourceOwner.forFuture(() =>
      dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(
        dbSupport.storageBackendFactory.createParameterStorageBackend.ledgerEnd
      )
    )
  }

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

  private def buildMetrics(participantId: Ref.ParticipantId): ResourceOwner[Metrics] =
    Metrics
      .fromSharedMetricRegistries(participantId)
      .tap(_.registry.registerAll(new JvmMetricSet))
      .pipe { metrics =>
        metricsConfig.reporter
          .fold(ResourceOwner.unit)(reporter =>
            ResourceOwner
              .forCloseable(() => reporter.register(metrics.registry))
              .map(_.start(metricsConfig.reportingInterval.toMillis, TimeUnit.MILLISECONDS))
          )
          .map(_ => metrics)
      }

  private def buildApiServer(
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
      authService = authService,
    )
}
