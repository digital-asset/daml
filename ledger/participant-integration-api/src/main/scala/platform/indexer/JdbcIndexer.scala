// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
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
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import com.daml.platform.store.packagemeta.PackageMetadataView
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object JdbcIndexer {
  private val logger = ContextualizedLogger.get(this.getClass)

  private[daml] final class Factory(
      participantId: Ref.ParticipantId,
      participantDataSourceConfig: ParticipantDataSourceConfig,
      config: IndexerConfig,
      readService: state.ReadService,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      inMemoryState: InMemoryState,
      apiUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
      executionContext: ExecutionContext,
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
          maxTailerBatchSize = config.maxTailerBatchSize,
          maxOutputBatchedBufferSize = config.maxOutputBatchedBufferSize,
          metrics = metrics,
          inMemoryStateUpdaterFlow = apiUpdaterFlow,
          stringInterningView = inMemoryState.stringInterningView,
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
                executionContext,
                config.packageMetadataView,
              ),
            ),
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

  private def updatePackageMetadataView(
      packageStorageBackend: PackageStorageBackend,
      metrics: Metrics,
      dbDispatcher: DbDispatcher,
      packageMetadataView: PackageMetadataView,
      computationExecutionContext: ExecutionContext,
      config: PackageMetadataViewConfig,
  )(implicit loggingContext: LoggingContext, materializer: Materializer): Future[Unit] = {
    implicit val ec: ExecutionContext = computationExecutionContext
    logger.info("Package Metadata View initialization has been started.")
    val startedTime = System.currentTimeMillis()

    def loadLfArchive(packageId: PackageId): Future[(PackageId, Array[Byte])] =
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadArchive)(connection =>
          packageStorageBackend
            .lfArchive(packageId)(connection)
            .getOrElse(
              // should never happen as we received a reference to packageId
              sys.error(s"LfArchive does not exist by packageId=$packageId")
            )
        )
        .map(bytes => (packageId, bytes))

    def lfPackagesSource(): Future[Source[PackageId, NotUsed]] =
      dbDispatcher.executeSql(metrics.daml.index.db.loadPackages)(connection =>
        Source(packageStorageBackend.lfPackages(connection).keySet)
      )

    def toMetadataDefinition(packageBytes: Array[Byte]): PackageMetadata =
      PackageMetadata.from(ArchiveParser.assertFromByteArray(packageBytes))

    def processPackage(archive: (PackageId, Array[Byte])): Future[PackageMetadata] = {
      val (packageId, packageBytes) = archive
      Future(toMetadataDefinition(packageBytes)).recover { case NonFatal(e) =>
        logger.error(s"Failed to decode loaded LF Archive by packageId=$packageId", e)
        throw e
      }
    }

    val initFuture = Source
      .futureSource(lfPackagesSource())
      .mapAsyncUnordered(config.initLoadParallelism)(loadLfArchive)
      .mapAsyncUnordered(config.initProcessParallelism)(processPackage)
      .runWith(Sink.foreach(packageMetadataView.update))

    val check = checkInitTakesTooLong(config, startedTime, initFuture)
    initFuture
      .map { _ =>
        logger.info("Package Metadata View has been initialized")
        val _ = check.cancel()
      }(
        computationExecutionContext
      )
      .recover { case NonFatal(e) =>
        logger.error(s"Failed to initialize Package Metadata View", e)
        val _ = check.cancel()
        throw e
      }
  }
  private def checkInitTakesTooLong[T](
      config: PackageMetadataViewConfig,
      startedTime: Long,
      f: Future[T],
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): Cancellable =
    materializer.scheduleAtFixedRate(
      initialDelay = config.initTakesTooLongInitialDelay,
      interval = config.initTakesTooLongInterval,
      task = () =>
        if (!f.isCompleted) {
          val now = System.currentTimeMillis()
          logger.warn(
            s"Package Metadata View initialization takes to long (${now - startedTime}ms)"
          )
        },
    )
}
