// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream._
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.InMemoryState
import com.daml.platform.index.InMemoryStateUpdater
import com.daml.platform.indexer.parallel.{
  InitializeParallelIngestion,
  ParallelIndexerFactory,
  ParallelIndexerSubscription,
}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend.{
  ParameterStorageBackend,
  StorageBackendFactory,
  StringInterningStorageBackend,
}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.interning.UpdatingStringInterningView
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import scala.concurrent.Future

object JdbcIndexer {
  private[daml] final class Factory(
      participantId: Ref.ParticipantId,
      participantDataSourceConfig: ParticipantDataSourceConfig,
      config: IndexerConfig,
      readService: state.ReadService,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      inMemoryState: InMemoryState,
      apiUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
  )(implicit materializer: Materializer) {

    def initialized()(implicit loggingContext: LoggingContext): ResourceOwner[Indexer] = {
      val factory = StorageBackendFactory.of(DbType.jdbcType(participantDataSourceConfig.jdbcUrl))
      val dataSourceStorageBackend = factory.createDataSourceStorageBackend
      val ingestionStorageBackend = factory.createIngestionStorageBackend
      val meteringStoreBackend = factory.createMeteringStorageWriteBackend
      val parameterStorageBackend = factory.createParameterStorageBackend
      val meteringParameterStorageBackend = factory.createMeteringParameterStorageBackend
      val DBLockStorageBackend = factory.createDBLockStorageBackend
      val stringInterningStorageBackend = factory.createStringInterningStorageBackend
      val dbConfig = IndexerConfig.dataSourceProperties(config)
      val indexer = ParallelIndexerFactory(
        inputMappingParallelism = config.inputMappingParallelism,
        batchingParallelism = config.batchingParallelism,
        dbConfig = dbConfig.createDbConfig(participantDataSourceConfig),
        haConfig = config.highAvailability,
        metrics = metrics,
        dbLockStorageBackend = DBLockStorageBackend,
        dataSourceStorageBackend = dataSourceStorageBackend,
        initializeParallelIngestion = InitializeParallelIngestion(
          providedParticipantId = participantId,
          parameterStorageBackend = parameterStorageBackend,
          ingestionStorageBackend = ingestionStorageBackend,
          stringInterningStorageBackend = stringInterningStorageBackend,
          metrics = metrics,
        ),
        parallelIndexerSubscription = ParallelIndexerSubscription(
          parameterStorageBackend = parameterStorageBackend,
          ingestionStorageBackend = ingestionStorageBackend,
          participantId = participantId,
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
          maxTailerBatchSize = config.maxTailerBatchSize,
          maxOutputBatchedBufferSize = config.maxOutputBatchedBufferSize,
          metrics = metrics,
          inMemoryStateUpdaterFlow = apiUpdaterFlow,
        ),
        meteringAggregator = new MeteringAggregator.Owner(
          meteringStore = meteringStoreBackend,
          meteringParameterStore = meteringParameterStorageBackend,
          parameterStore = parameterStorageBackend,
          metrics = metrics,
        ).apply,
        mat = materializer,
        readService = readService,
        initializeInMemoryState = dbDispatcher =>
          ledgerEnd =>
            inMemoryState.initializeTo(ledgerEnd)((updatingStringInterningView, ledgerEnd) =>
              updateStringInterningView(
                stringInterningStorageBackend,
                metrics,
                dbDispatcher,
                updatingStringInterningView,
                ledgerEnd,
              )
            ),
        stringInterningView = inMemoryState.stringInterningView,
      )

      indexer
    }
  }

  private def updateStringInterningView(
      stringInterningStorageBackend: StringInterningStorageBackend,
      metrics: Metrics,
      dbDispatcher: DbDispatcher,
      updatingStringInterningView: UpdatingStringInterningView,
      ledgerEnd: ParameterStorageBackend.LedgerEnd,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    updatingStringInterningView.update(ledgerEnd.lastStringInterningId)(
      (fromExclusive, toInclusive) =>
        implicit loggingContext =>
          dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
    )
}
