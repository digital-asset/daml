// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.platform.configuration.{PartyConfiguration, ServerRole}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
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
import com.daml.ledger.runner.common.{Config, ConfigProvider, ParticipantConfig}
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.apiserver.{
  ApiServer,
  ApiServerConfig,
  LedgerFeatures,
  StandaloneApiServer,
  StandaloneIndexService,
  TimeServiceBackend,
}
import com.daml.platform.index.{LedgerBuffersUpdater, ParticipantInMemoryState}
import com.daml.platform.indexer.{IndexerConfig, StandaloneIndexerServer}
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.usermanagement.{PersistentUserManagementStore, UserManagementConfig}
import io.grpc.ServerInterceptor

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.chaining._

class Participant(
    val apiServer: ApiServer,
    val writeService: WriteService,
    val indexService: IndexService,
)

object Participant {
  def owner(implicit
      config: Config[_],
      participantConfig: ParticipantConfig,
      apiServerConfig: ApiServerConfig,
      indexerConfig: IndexerConfig,
      timeServiceBackendO: Option[TimeServiceBackend],
      readService: ReadService,
      buildWriteService: (
          Metrics,
          ExecutionContext,
          Int,
          IndexService,
      ) => ResourceOwner[WriteService],
      actorSystem: ActorSystem,
      materializer: Materializer,
      implicitPartyAllocation: Boolean,
      interceptors: List[ServerInterceptor],
  ): ResourceOwner[Participant] = {

    val sharedEngine = new Engine(
      EngineConfig(
        allowedLanguageVersions = config.allowedLanguageVersions,
        profileDir = config.profileDir,
        stackTraceMode = config.stackTraces,
      )
    )

    newLoggingContextWith("participantId" -> participantConfig.participantId) {
      implicit loggingContext =>
        for {
          metrics <- buildMetrics
          translationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
            eventConfiguration = config.lfValueTranslationEventCache,
            contractConfiguration = config.lfValueTranslationContractCache,
            metrics = metrics,
          )

          servicesThreadPoolSize = Runtime.getRuntime.availableProcessors()
          servicesExecutionContext <- buildServicesExecutionContext(
            metrics,
            servicesThreadPoolSize,
          )

          dbSupport <- DbSupport
            .owner(
              jdbcUrl = apiServerConfig.jdbcUrl,
              serverRole = ServerRole.ApiServer,
              connectionPoolSize = apiServerConfig.databaseConnectionPoolSize,
              connectionTimeout = apiServerConfig.databaseConnectionTimeout,
              metrics = metrics,
            )

          participantInMemoryState <- ParticipantInMemoryState.owner(
            apiServerConfig = apiServerConfig,
            metrics = metrics,
            servicesExecutionContext = servicesExecutionContext,
            jdbcUrl = apiServerConfig.jdbcUrl,
          )

          indexerHealthChecks <- buildIndexerServer(
            indexerConfig,
            metrics,
            new TimedReadService(readService, metrics),
            translationCache,
            participantInMemoryState.stringInterningView,
            LedgerBuffersUpdater.flow
              .mapAsync(1) { batch =>
                participantInMemoryState.updateBatch(batch)
              },
          )

          indexService <- StandaloneIndexService(
            dbSupport = dbSupport,
            ledgerId = config.ledgerId,
            config = apiServerConfig,
            metrics = metrics,
            engine = sharedEngine,
            servicesExecutionContext = servicesExecutionContext,
            lfValueTranslationCache = translationCache,
            participantInMemoryState = participantInMemoryState,
          )

          writeService <- buildWriteService(
            metrics,
            servicesExecutionContext,
            servicesThreadPoolSize,
            indexService,
          )

          apiServer <- buildStandaloneApiServer(
            sharedEngine,
            indexService,
            metrics,
            servicesExecutionContext,
            new TimedWriteService(writeService, metrics),
            indexerHealthChecks,
            timeServiceBackendO,
            dbSupport,
            implicitPartyAllocation,
            interceptors,
          )
        } yield new Participant(apiServer, writeService, indexService)
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
      implicitPartyAllocation: Boolean,
      interceptors: List[ServerInterceptor],
  )(implicit
      actorSystem: ActorSystem,
      loggingContext: LoggingContext,
      config: Config[_],
      apiServerConfig: ApiServerConfig,
  ): ResourceOwner[ApiServer] =
    StandaloneApiServer(
      indexService = indexService,
      ledgerId = config.ledgerId,
      config = apiServerConfig,
      commandConfig = config.commandConfig,
      partyConfig = PartyConfiguration(implicitPartyAllocation),
      optWriteService = Some(writeService),
      authService = config.authService,
      healthChecks = healthChecksWithIndexer + ("write" -> writeService),
      metrics = metrics,
      timeServiceBackend = timeServiceBackend,
      otherInterceptors = interceptors,
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
      indexerConfig: IndexerConfig,
      metrics: Metrics,
      readService: ReadService,
      translationCache: LfValueTranslationCache.Cache,
      stringInterningView: StringInterningView,
      updateInMemoryBuffersFlow: Flow[(Iterable[(Offset, Update)], LedgerEndMarker), Unit, NotUsed],
  )(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
  ): ResourceOwner[HealthChecks] =
    for {
      indexerHealth <- new StandaloneIndexerServer(
        readService = readService,
        config = indexerConfig,
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
      config: Config[_],
  ): ResourceOwner[Metrics] =
    ConfigProvider
      .createMetrics(participantConfig)
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
}
