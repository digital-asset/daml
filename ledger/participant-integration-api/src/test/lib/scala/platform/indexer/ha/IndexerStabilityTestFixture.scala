// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.index.{LedgerBuffersUpdater, ParticipantInMemoryState}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, StandaloneIndexerServer}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.{
  EventsBuffer,
  MutableContractStateCaches,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Stores a running indexer and the read service the indexer is reading from.
  * The read service is used exclusively by this indexer.
  */
case class ReadServiceAndIndexer(
    readService: EndlessReadService,
    indexing: ReportsHealth,
)

case class Indexers(indexers: List[ReadServiceAndIndexer]) {
  // The list of all indexers that are running (determined by whether they have subscribed to the read service)
  def runningIndexers: List[ReadServiceAndIndexer] =
    indexers.filter(_.readService.isRunning)
  def resetAll(): Unit = indexers.foreach(_.readService.reset())
}

object IndexerStabilityTestFixture {

  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(
      updatesPerSecond: Int,
      indexerCount: Int,
      jdbcUrl: String,
      lockIdSeed: Int,
      materializer: Materializer,
  ): ResourceOwner[Indexers] = new ResourceOwner[Indexers] {
    override def acquire()(implicit context: ResourceContext): Resource[Indexers] = {
      createIndexers(
        updatesPerSecond = updatesPerSecond,
        indexerCount = indexerCount,
        jdbcUrl = jdbcUrl,
        lockIdSeed = lockIdSeed,
      )(context, materializer)
    }
  }

  private def createIndexers(
      updatesPerSecond: Int,
      indexerCount: Int,
      jdbcUrl: String,
      lockIdSeed: Int,
  )(implicit resourceContext: ResourceContext, materializer: Materializer): Resource[Indexers] = {
    val indexerConfig = IndexerConfig(
      participantId = EndlessReadService.participantId,
      jdbcUrl = jdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      haConfig = HaConfig(
        indexerLockId = lockIdSeed,
        indexerWorkerLockId = lockIdSeed + 1,
      ),
    )

    newLoggingContext { implicit loggingContext =>
      for {
        // This execution context is not used for indexing in the append-only schema, it can be shared
        servicesExecutionContext <- ResourceOwner
          .forExecutorService(() => Executors.newWorkStealingPool())
          .map(ExecutionContext.fromExecutorService)
          .acquire()

        // Start N indexers that all compete for the same database
        _ = logger.info(s"Starting $indexerCount indexers for database $jdbcUrl")
        indexers <- Resource
          .sequence(
            (1 to indexerCount).toList
              .map(i =>
                for {
                  // Create a read service
                  readService <- ResourceOwner
                    .forCloseable(() =>
                      withEnrichedLoggingContext("name" -> s"ReadService$i") {
                        readServiceLoggingContext =>
                          EndlessReadService(updatesPerSecond, s"$i")(readServiceLoggingContext)
                      }
                    )
                    .acquire()
                  metricRegistry = new MetricRegistry
                  metrics = new Metrics(metricRegistry)
                  updateInMemoryBuffersFlow <- ledgerApiUpdateFlow(
                    metrics,
                    servicesExecutionContext,
                  )
                  dbType = DbType.jdbcType(jdbcUrl)
                  storageBackendFactory = StorageBackendFactory.of(dbType)

                  stringInterningStorageBackend =
                    storageBackendFactory.createStringInterningStorageBackend
                  // Create an indexer and immediately start it
                  indexing <- new StandaloneIndexerServer(
                    readService = readService,
                    config = indexerConfig,
                    metrics = metrics,
                    lfValueTranslationCache = LfValueTranslationCache.Cache.none,
                    stringInterningView = new StringInterningView(
                      loadPrefixedEntries = (fromExclusive, toInclusive, dbDispatcher) =>
                        implicit loggingContext =>
                          dbDispatcher.executeSql(
                            metrics.daml.index.db.loadStringInterningEntries
                          ) {
                            stringInterningStorageBackend.loadStringInterningEntries(
                              fromExclusive,
                              toInclusive,
                            )
                          }
                    ),
                    updateInMemoryBuffersFlow = updateInMemoryBuffersFlow,
                  ).acquire()
                } yield ReadServiceAndIndexer(readService, indexing)
              )
          )
          .map(xs => Indexers(xs.toList))
      } yield indexers
    }
  }

  private def ledgerApiUpdateFlow(metrics: Metrics, executionContext: ExecutionContext)(implicit
      resourceContext: ResourceContext,
      loggingContext: LoggingContext,
  ) = for {
    generalDispatcher <-
      Dispatcher
        .owner[Offset](
          name = "sql-ledger",
          zeroIndex = Offset.beforeBegin,
          headAtInitialization = Offset.beforeBegin,
        )
        .acquire()
    ledgerEndCache = MutableLedgerEndCache()

    updateLedgerApiLedgerEnd = (ledgerEnd: LedgerEnd) => {
      ledgerEndCache.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))
      // the order here is very important: first we need to make data available for point-wise lookups
      // and SQL queries, and only then we can make it available on the streams.
      // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
      generalDispatcher.signalNewHead(ledgerEnd.lastOffset)
    }

    participantInMemoryState = new ParticipantInMemoryState(
      mutableContractStateCaches = MutableContractStateCaches.build(
        maxKeyCacheSize = 100000,
        maxContractsCacheSize = 100000,
        metrics = metrics,
      )(executionContext),
      completionsBuffer = new EventsBuffer[TransactionLogUpdate](
        maxBufferSize = 10000,
        metrics = metrics,
        bufferQualifier = "completions",
        ignoreMarker = _.isInstanceOf[LedgerEndMarker],
      ),
      transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
        // TODO LLP differentiate to completions
        maxBufferSize = 10000,
        metrics = metrics,
        bufferQualifier = "transactions",
        ignoreMarker = !_.isInstanceOf[TransactionLogUpdate.TransactionAccepted],
      ),
      updateLedgerApiLedgerEnd = updateLedgerApiLedgerEnd,
      metrics = metrics,
    )
  } yield LedgerBuffersUpdater.flow
    .mapAsync(1) { batch =>
      participantInMemoryState.updateBatch(batch)
    }
}
