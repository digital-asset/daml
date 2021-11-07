// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream._
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.LooseSyncChannel
import com.daml.platform.indexer.parallel.{
  InitializeParallelIngestion,
  ParallelIndexerFactory,
  ParallelIndexerSubscription,
}
import com.daml.platform.store.DbType.{
  AsynchronousCommit,
  LocalSynchronousCommit,
  SynchronousCommit,
}
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.DataSourceStorageBackend.DataSourceConfig
import com.daml.platform.store.backend.{
  DataSourceStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
}
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

object JdbcIndexer {
  private[daml] final class Factory(
      config: IndexerConfig,
      readService: state.ReadService,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      ledgerEndUpdateChannel: Option[LooseSyncChannel] = None,
  )(implicit materializer: Materializer) {

    def initialized(
        resetSchema: Boolean = false
    )(implicit loggingContext: LoggingContext): ResourceOwner[Indexer] = {
      val dbType = DbType.jdbcType(config.jdbcUrl)
      val ingestionParallelism = dbType match {
        case DbType.M | DbType.H2Database => 1
        case _ => config.ingestionParallelism
      }
      val factory = StorageBackendFactory.of(dbType)
      val dataSourceStorageBackend = factory.createDataSourceStorageBackend
      val ingestionStorageBackend = factory.createIngestionStorageBackend
      val parameterStorageBackend = factory.createParameterStorageBackend
      val DBLockStorageBackend = factory.createDBLockStorageBackend
      val resetStorageBackend = factory.createResetStorageBackend
      val stringInterningStorageBackend = factory.createStringInterningStorageBackend
      val indexer = ParallelIndexerFactory(
        jdbcUrl = config.jdbcUrl,
        inputMappingParallelism = config.inputMappingParallelism,
        batchingParallelism = config.batchingParallelism,
        ingestionParallelism = ingestionParallelism,
        dataSourceConfig = DataSourceConfig(
          postgresConfig = PostgresDataSourceConfig(
            synchronousCommit = Some(config.asyncCommitMode match {
              case SynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.On
              case AsynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.Off
              case LocalSynchronousCommit =>
                PostgresDataSourceConfig.SynchronousCommitValue.Local
            }),
            tcpKeepalivesIdle = config.postgresTcpKeepalivesIdle,
            tcpKeepalivesInterval = config.postgresTcpKeepalivesInterval,
            tcpKeepalivesCount = config.postgresTcpKeepalivesCount,
          )
        ),
        haConfig = config.haConfig,
        metrics = metrics,
        dbLockStorageBackend = DBLockStorageBackend,
        dataSourceStorageBackend = dataSourceStorageBackend,
        initializeParallelIngestion = InitializeParallelIngestion(
          providedParticipantId = config.participantId,
          parameterStorageBackend = parameterStorageBackend,
          ingestionStorageBackend = ingestionStorageBackend,
          metrics = metrics,
        ),
        parallelIndexerSubscription = ParallelIndexerSubscription(
          parameterStorageBackend = parameterStorageBackend,
          ingestionStorageBackend = ingestionStorageBackend,
          participantId = config.participantId,
          translation = new LfValueTranslation(
            cache = lfValueTranslationCache,
            metrics = metrics,
            enricherO = None,
            loadPackage = (_, _) => Future.successful(None),
          ),
          compressionStrategy =
            if (config.enableCompression) CompressionStrategy.allGZIP(metrics)
            else CompressionStrategy.none(metrics),
          maxInputBufferSize = config.maxInputBufferSize,
          inputMappingParallelism = config.inputMappingParallelism,
          batchingParallelism = config.batchingParallelism,
          ingestionParallelism = ingestionParallelism,
          submissionBatchSize = config.submissionBatchSize,
          tailingRateLimitPerSecond = config.tailingRateLimitPerSecond,
          batchWithinMillis = config.batchWithinMillis,
          metrics = metrics,
          ledgerEndUpdateChannel = ledgerEndUpdateChannel,
        ),
        stringInterningStorageBackend = stringInterningStorageBackend,
        mat = materializer,
        readService = readService,
      )
      if (resetSchema) {
        reset(resetStorageBackend, dataSourceStorageBackend).flatMap(_ => indexer)
      } else {
        indexer
      }
    }

    private def reset(
        resetStorageBackend: ResetStorageBackend,
        dataSourceStorageBackend: DataSourceStorageBackend,
    )(implicit loggingContext: LoggingContext): ResourceOwner[Unit] =
      ResourceOwner.forFuture(() =>
        Future(
          Using.resource(dataSourceStorageBackend.createDataSource(config.jdbcUrl).getConnection)(
            resetStorageBackend.reset
          )
        )(servicesExecutionContext)
      )

  }
}
