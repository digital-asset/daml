// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream._
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.{InMemoryState, PackageId}
import com.daml.platform.index.InMemoryStateUpdater
import com.daml.platform.indexer.parallel.{
  InitializeParallelIngestion,
  ParallelIndexerFactory,
  ParallelIndexerSubscription,
}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend.{
  PackageStorageBackend,
  ParameterStorageBackend,
  StorageBackendFactory,
  StringInterningStorageBackend,
}
import com.daml.platform.store.cache.ImmutableLedgerEndCache
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.interning.UpdatingStringInterningView
import com.daml.platform.store.packagemeta.PackageMetadataView
import com.daml.platform.store.utils.QueueBasedConcurrencyLimiter
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}

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
            engineO = None,
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
          metrics = metrics,
          inMemoryStateUpdaterFlow = apiUpdaterFlow,
          packageMetadataView = inMemoryState.packageMetadataView,
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
            inMemoryState.initializeTo(ledgerEnd)(
              updateStringInterningView = (updatingStringInterningView, ledgerEnd) =>
                updateStringInterningView(
                  stringInterningStorageBackend,
                  metrics,
                  dbDispatcher,
                  updatingStringInterningView,
                  ledgerEnd,
                ),
              updatePackageMetadataView = updatePackageMetadataView(
                factory.createPackageStorageBackend(
                  ImmutableLedgerEndCache(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
                ),
                metrics,
                dbDispatcher,
                _,
                4, // TODO DPP-1068: needs to be wired to config
                materializer.executionContext, // TODO DPP-1068: wire up service execution context here instead
              ),
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

  // We are loading all packages in a non deterministic order.
  // Please note this approach is only possible with the assumption that updatingPackageMetadataView.update can
  // be executed in any order.
  // This is potentially an expensive operation, should be avoided.
  // TODO DPP-1068: if this approach proves to be non-feasible, a materialized view of the Metadata needs to be created
  //                (similarly to StringInterningView)
  // TODO DPP-1068: add logging
  private def updatePackageMetadataView(
      packageStorageBackend: PackageStorageBackend,
      metrics: Metrics,
      dbDispatcher: DbDispatcher,
      updatingPackageMetadataView: PackageMetadataView,
      packageLoadingParallelism: Int,
      computationExecutionContext: ExecutionContext,
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    // we use this execution context both for expensive deserialization/computation and Future mapping operations
    implicit val ec: ExecutionContext = computationExecutionContext

    def loadPackages: Future[Set[PackageId]] =
      dbDispatcher.executeSql(metrics.daml.index.db.loadPackages) { connection =>
        packageStorageBackend.lfPackages(connection).keySet
      }

    def loadArchiveAndUpdateMetadata(packageId: PackageId): Future[Unit] =
      for {
        archiveBytes <- dbDispatcher.executeSql(metrics.daml.index.db.loadArchive) { connection =>
          packageStorageBackend
            .lfArchive(packageId)(connection)
            .get // safe to do here, since we looked the ids up from persistence
        }
        archive <- Future {
          ArchiveParser.assertFromByteArray(archiveBytes)
        }
        _ <- Future {
          updatingPackageMetadataView.update(archive)
        }
      } yield ()

    val concurrencyLimiter = new QueueBasedConcurrencyLimiter(packageLoadingParallelism, ec)
    def loadArchiveAndUpdateMetadatas(packageIds: Set[PackageId]): Future[Unit] =
      Future
        .traverse(packageIds)(
          // we limit these Futures, so maximum #packageLoadingParallelism can ran in parallel
          packageId =>
            concurrencyLimiter.execute(
              loadArchiveAndUpdateMetadata(packageId)
            )
        )
        .map(_ => ())

    loadPackages.flatMap(loadArchiveAndUpdateMetadatas)
  }
}
