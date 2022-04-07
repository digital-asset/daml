// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Flow
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
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
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import scala.concurrent.Future

object JdbcIndexer {
  private[daml] final class Factory(
      config: IndexerConfig,
      readService: state.ReadService,
      stringInterningView: StringInterningView,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      updateInMemoryBuffersFlow: Flow[(Iterable[(Offset, Update)], LedgerEndMarker), Unit, NotUsed],
  )(implicit materializer: Materializer) {

    def initialized()(implicit loggingContext: LoggingContext): ResourceOwner[Indexer] = {
      val factory = StorageBackendFactory.of(DbType.jdbcType(config.jdbcUrl))
      val dataSourceStorageBackend = factory.createDataSourceStorageBackend
      val ingestionStorageBackend = factory.createIngestionStorageBackend
      val meteringStoreBackend = factory.createMeteringStorageWriteBackend
      val parameterStorageBackend = factory.createParameterStorageBackend
      val meteringParameterStorageBackend = factory.createMeteringParameterStorageBackend
      val DBLockStorageBackend = factory.createDBLockStorageBackend
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
          ingestionParallelism = config.ingestionParallelism,
          submissionBatchSize = config.submissionBatchSize,
          batchWithinMillis = config.batchWithinMillis,
          metrics = metrics,
        ),
        stringInterningView = stringInterningView,
        meteringAggregator = new MeteringAggregator.Owner(
          meteringStore = meteringStoreBackend,
          meteringParameterStore = meteringParameterStorageBackend,
          parameterStore = parameterStorageBackend,
          metrics = metrics,
        ).apply,
        mat = materializer,
        readService = readService,
        updateInMemoryBuffersFlow = updateInMemoryBuffersFlow,
      )
      indexer
    }
  }
}
