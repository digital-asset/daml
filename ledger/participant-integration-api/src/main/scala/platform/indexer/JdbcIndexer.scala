// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream._
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
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
import com.daml.platform.store.backend.StorageBackend
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
  )(implicit materializer: Materializer, loggingContext: LoggingContext) {

    def initialized(resetSchema: Boolean = false): ResourceOwner[Indexer] = {
      val storageBackend = StorageBackend.of(DbType.jdbcType(config.jdbcUrl))
      val indexer = ParallelIndexerFactory(
        jdbcUrl = config.jdbcUrl,
        inputMappingParallelism = config.inputMappingParallelism,
        batchingParallelism = config.batchingParallelism,
        ingestionParallelism = config.ingestionParallelism,
        dataSourceConfig = DataSourceConfig(
          postgresConfig = PostgresDataSourceConfig(
            synchronousCommit = Some(config.asyncCommitMode match {
              case SynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.On
              case AsynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.Off
              case LocalSynchronousCommit =>
                PostgresDataSourceConfig.SynchronousCommitValue.Local
            })
          )
        ),
        haConfig = config.haConfig,
        metrics = metrics,
        storageBackend = storageBackend,
        initializeParallelIngestion = InitializeParallelIngestion(
          providedParticipantId = config.participantId,
          storageBackend = storageBackend,
          metrics = metrics,
        ),
        parallelIndexerSubscription = ParallelIndexerSubscription(
          storageBackend = storageBackend,
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
          ingestionParallelism = config.ingestionParallelism,
          submissionBatchSize = config.submissionBatchSize,
          tailingRateLimitPerSecond = config.tailingRateLimitPerSecond,
          batchWithinMillis = config.batchWithinMillis,
          metrics = metrics,
        ),
        mat = materializer,
        readService = readService,
      )
      if (resetSchema) {
        reset(storageBackend).flatMap(_ => indexer)
      } else {
        indexer
      }
    }

    private def reset(storageBackend: StorageBackend[_]): ResourceOwner[Unit] =
      ResourceOwner.forFuture(() =>
        Future(
          Using.resource(storageBackend.createDataSource(config.jdbcUrl).getConnection)(
            storageBackend.reset
          )
        )(servicesExecutionContext)
      )

  }
}
