// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v2.{ReadService, WriteService}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.Metrics
import com.daml.platform.apiserver._
import com.daml.platform.config.ParticipantConfig
import com.daml.platform.configuration.{IndexServiceConfig, ServerRole}
import com.daml.platform.index.{InMemoryStateUpdater, IndexServiceOwner}
import com.daml.platform.indexer.IndexerServiceOwner
import com.daml.platform.partymanagement.PersistentPartyRecordStore
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.DbSupport
import com.daml.platform.usermanagement.{PersistentUserManagementStore, UserManagementConfig}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

class LedgerApiServer(
    authService: AuthService,
    buildWriteService: IndexService => ResourceOwner[WriteService],
    engine: Engine,
    ledgerFeatures: LedgerFeatures,
    ledgerId: LedgerId,
    participantConfig: ParticipantConfig,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    participantId: Ref.ParticipantId,
    readService: ReadService,
    timeServiceBackendO: Option[TimeServiceBackend],
    servicesExecutionContext: ExecutionContextExecutorService,
    metrics: Metrics,
)(implicit actorSystem: ActorSystem, materializer: Materializer) {

  def owner: ResourceOwner[ApiService] = {
    newLoggingContextWith("participantId" -> participantId) { implicit loggingContext =>
      for {
        (inMemoryState, inMemoryStateUpdaterFlow) <-
          LedgerApiServer.createInMemoryStateAndUpdater(
            participantConfig.indexService,
            metrics,
            servicesExecutionContext,
          )

        timedReadService = new TimedReadService(readService, metrics)
        indexerHealthChecks <-
          for {
            indexerHealth <- new IndexerServiceOwner(
              participantId = participantId,
              participantDataSourceConfig = participantDataSourceConfig,
              readService = timedReadService,
              config = participantConfig.indexer,
              metrics = metrics,
              inMemoryState = inMemoryState,
              inMemoryStateUpdaterFlow = inMemoryStateUpdaterFlow,
              executionContext = servicesExecutionContext,
            )
          } yield new HealthChecks(
            "read" -> timedReadService,
            "indexer" -> indexerHealth,
          )

        readDbSupport <- DbSupport
          .owner(
            serverRole = ServerRole.ApiServer,
            metrics = metrics,
            dbConfig = participantConfig.dataSourceProperties.createDbConfig(
              participantDataSourceConfig
            ),
          )

        // TODO: Add test asserting that the indexService retries until IndexDB persistence comes up
        indexService <- new IndexServiceOwner(
          config = participantConfig.indexService,
          dbSupport = readDbSupport,
          initialLedgerId = domain.LedgerId(ledgerId),
          metrics = metrics,
          engine = engine,
          servicesExecutionContext = servicesExecutionContext,
          participantId = participantId,
          inMemoryState = inMemoryState,
        )(loggingContext)

        writeService <- buildWriteService(indexService)

        apiService <- buildApiService(
          ledgerFeatures,
          engine,
          indexService,
          metrics,
          servicesExecutionContext,
          new TimedWriteService(writeService, metrics),
          indexerHealthChecks,
          timeServiceBackendO,
          readDbSupport,
          ledgerId,
          participantConfig.apiServer,
          participantId,
        )
      } yield apiService
    }
  }

  private def buildApiService(
      ledgerFeatures: LedgerFeatures,
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
  ): ResourceOwner[ApiService] =
    ApiServiceOwner(
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
      partyRecordStore = new PersistentPartyRecordStore(
        dbSupport = dbSupport,
        metrics = metrics,
        timeProvider = TimeProvider.UTC,
        executionContext = servicesExecutionContext,
      ),
      ledgerFeatures = ledgerFeatures,
      participantId = participantId,
      authService = authService,
      jwtTimestampLeeway = participantConfig.jwtTimestampLeeway,
    )
}

object LedgerApiServer {
  def createInMemoryStateAndUpdater(
      indexServiceConfig: IndexServiceConfig,
      metrics: Metrics,
      executionContext: ExecutionContext,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[(InMemoryState, InMemoryStateUpdater.UpdaterFlow)] =
    for {
      inMemoryState <- InMemoryState.owner(
        apiStreamShutdownTimeout = indexServiceConfig.apiStreamShutdownTimeout,
        bufferedStreamsPageSize = indexServiceConfig.bufferedStreamsPageSize,
        maxContractStateCacheSize = indexServiceConfig.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = indexServiceConfig.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize =
          indexServiceConfig.maxTransactionsInMemoryFanOutBufferSize,
        executionContext = executionContext,
        metrics = metrics,
      )

      inMemoryStateUpdater <- InMemoryStateUpdater.owner(
        inMemoryState = inMemoryState,
        prepareUpdatesParallelism = indexServiceConfig.inMemoryStateUpdaterParallelism,
        preparePackageMetadataTimeOutWarning =
          indexServiceConfig.preparePackageMetadataTimeOutWarning,
        metrics = metrics,
      )
    } yield inMemoryState -> inMemoryStateUpdater
}
