// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.InMemoryState
import com.digitalasset.canton.platform.index.InMemoryStateUpdater
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.parallel.{
  InitializeParallelIngestion,
  ParallelIndexerFactory,
  ParallelIndexerSubscription,
  PostPublishData,
  ReassignmentOffsetPersistence,
}
import com.digitalasset.canton.platform.store.DbSupport.{
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.backend.StorageBackendFactory
import com.digitalasset.canton.platform.store.backend.h2.H2StorageBackendFactory
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.*

import scala.concurrent.{ExecutionContext, Future}

object JdbcIndexer {
  final class Factory(
      participantId: Ref.ParticipantId,
      participantDataSourceConfig: ParticipantDataSourceConfig,
      config: IndexerConfig,
      excludedPackageIds: Set[Ref.PackageId],
      metrics: LedgerApiServerMetrics,
      inMemoryState: InMemoryState,
      apiUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
      executionContext: ExecutionContext,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
      dataSourceProperties: DataSourceProperties,
      highAvailability: HaConfig,
      indexSericeDbDispatcher: Option[DbDispatcher],
      clock: Clock,
      reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
      postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
  )(implicit materializer: Materializer) {

    def initialized()(implicit traceContext: TraceContext): ResourceOwner[Indexer] = {
      val factory = StorageBackendFactory.of(
        DbType.jdbcType(participantDataSourceConfig.jdbcUrl),
        loggerFactory,
      )
      val dataSourceStorageBackend = factory.createDataSourceStorageBackend
      val ingestionStorageBackend = factory.createIngestionStorageBackend
      val meteringStoreBackend = factory.createMeteringStorageWriteBackend
      val parameterStorageBackend =
        factory.createParameterStorageBackend(inMemoryState.stringInterningView)
      val meteringParameterStorageBackend = factory.createMeteringParameterStorageBackend
      val DBLockStorageBackend = factory.createDBLockStorageBackend
      val stringInterningStorageBackend = factory.createStringInterningStorageBackend
      val completionStorageBackend =
        factory.createCompletionStorageBackend(inMemoryState.stringInterningView, loggerFactory)
      val dbConfig = dataSourceProperties
      // in case H2 backend, we share a single connection between indexer and index service
      // to prevent H2 synchronization bug to materialize
      // the ingestion parallelism is also limited to 1 in this case
      val (ingestionParallelism, indexerDbDispatcherOverride) =
        if (factory == H2StorageBackendFactory) 1 -> indexSericeDbDispatcher
        else config.ingestionParallelism.unwrap -> None
      ParallelIndexerFactory(
        inputMappingParallelism = config.inputMappingParallelism.unwrap,
        batchingParallelism = config.batchingParallelism.unwrap,
        dbConfig = dbConfig.createDbConfig(participantDataSourceConfig),
        haConfig = highAvailability,
        metrics = metrics,
        dbLockStorageBackend = DBLockStorageBackend,
        dataSourceStorageBackend = dataSourceStorageBackend,
        initializeParallelIngestion = InitializeParallelIngestion(
          providedParticipantId = participantId,
          parameterStorageBackend = parameterStorageBackend,
          ingestionStorageBackend = ingestionStorageBackend,
          completionStorageBackend = completionStorageBackend,
          stringInterningStorageBackend = stringInterningStorageBackend,
          updatingStringInterningView = inMemoryState.stringInterningView,
          postProcessor = postProcessor,
          metrics = metrics,
          loggerFactory = loggerFactory,
        ),
        parallelIndexerSubscription = ParallelIndexerSubscription(
          parameterStorageBackend = parameterStorageBackend,
          ingestionStorageBackend = ingestionStorageBackend,
          participantId = participantId,
          translation = new LfValueTranslation(
            metrics = metrics,
            engineO = None,
            loadPackage = (_, _) => Future.successful(None),
            loggerFactory = loggerFactory,
          ),
          compressionStrategy =
            if (config.enableCompression) CompressionStrategy.allGZIP(metrics)
            else CompressionStrategy.none(metrics),
          maxInputBufferSize = config.maxInputBufferSize.unwrap,
          inputMappingParallelism = config.inputMappingParallelism.unwrap,
          batchingParallelism = config.batchingParallelism.unwrap,
          ingestionParallelism = ingestionParallelism,
          submissionBatchSize = config.submissionBatchSize,
          maxTailerBatchSize = config.maxTailerBatchSize,
          postProcessingParallelism = config.postProcessingParallelism,
          maxOutputBatchedBufferSize = config.maxOutputBatchedBufferSize,
          excludedPackageIds = excludedPackageIds,
          metrics = metrics,
          inMemoryStateUpdaterFlow = apiUpdaterFlow,
          inMemoryState = inMemoryState,
          reassignmentOffsetPersistence = reassignmentOffsetPersistence,
          postProcessor = postProcessor,
          tracer = tracer,
          loggerFactory = loggerFactory,
        ),
        meteringAggregator = new MeteringAggregator.Owner(
          meteringStore = meteringStoreBackend,
          meteringParameterStore = meteringParameterStorageBackend,
          parameterStore = parameterStorageBackend,
          metrics = metrics,
          loggerFactory = loggerFactory,
        ).apply,
        mat = materializer,
        executionContext = executionContext,
        initializeInMemoryState = inMemoryState.initializeTo,
        loggerFactory = loggerFactory,
        indexerDbDispatcherOverride = indexerDbDispatcherOverride,
        clock = clock,
      )
    }
  }
}
