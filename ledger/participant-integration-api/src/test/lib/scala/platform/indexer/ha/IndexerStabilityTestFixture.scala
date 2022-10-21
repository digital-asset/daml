// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.util.concurrent.Executors
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.LedgerApiServer
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.{IndexerConfig, IndexerServiceOwner, IndexerStartupMode}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig

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
      startupMode = IndexerStartupMode.MigrateAndStart(),
      highAvailability = HaConfig(
        indexerLockId = lockIdSeed,
        indexerWorkerLockId = lockIdSeed + 1,
      ),
    )

    newLoggingContext { implicit loggingContext =>
      val participantDataSourceConfig = ParticipantDataSourceConfig(jdbcUrl)
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
                  // create a new MetricRegistry for each indexer, so they don't step on each other toes:
                  // Gauges can only be registered once. A subsequent attempt results in an exception for the
                  // call MetricRegistry#register or MetricRegistry#registerGauge
                  metrics = new Metrics(new MetricRegistry)
                  (inMemoryState, inMemoryStateUpdaterFlow) <-
                    LedgerApiServer
                      .createInMemoryStateAndUpdater(
                        IndexServiceConfig(),
                        metrics,
                        servicesExecutionContext,
                      )
                      .acquire()

                  // Create an indexer and immediately start it
                  indexing <- new IndexerServiceOwner(
                    participantId = EndlessReadService.participantId,
                    participantDataSourceConfig = participantDataSourceConfig,
                    readService = readService,
                    config = indexerConfig,
                    metrics = metrics,
                    inMemoryState = inMemoryState,
                    inMemoryStateUpdaterFlow = inMemoryStateUpdaterFlow,
                    executionContext = servicesExecutionContext,
                  ).acquire()
                } yield ReadServiceAndIndexer(readService, indexing)
              )
          )
          .map(xs => Indexers(xs.toList))
      } yield indexers
    }
  }
}
