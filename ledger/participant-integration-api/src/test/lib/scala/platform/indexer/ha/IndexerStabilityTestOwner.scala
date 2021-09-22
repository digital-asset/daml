// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, StandaloneIndexerServer}
import com.daml.platform.store.LfValueTranslationCache

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
  private val rng = new scala.util.Random(123456789)
  private val logger = ContextualizedLogger.get(this.getClass)

  def randomIndexer: ReadServiceAndIndexer = indexers(rng.nextInt(indexers.length))
  def runningIndexer(implicit loggingContext: LoggingContext): ReadServiceAndIndexer = {
    val allRunning =
      indexers.filter(x => x.readService.stateUpdatesCalls.get() > 0 && !x.readService.aborted)
    if (allRunning.length != 1) {
      val message = s"Expected 1 running indexer, found ${allRunning.length}"
      logger.info(message)
      throw new RuntimeException(message)
    } else {
      allRunning.head
    }
  }
  def resetAll(): Unit = indexers.foreach(x => x.readService.reset())
}

object IndexerStabilityTestFixture {

  def owner(
      updatesPerSecond: Int,
      indexerCount: Int,
      jdbcUrl: String,
      materializer: Materializer,
  ): ResourceOwner[Indexers] = new ResourceOwner[Indexers] {
    override def acquire()(implicit context: ResourceContext): Resource[Indexers] = {
      createIndexers(
        updatesPerSecond = updatesPerSecond,
        indexerCount = indexerCount,
        jdbcUrl = jdbcUrl,
      )(context, materializer)
    }
  }

  private def createIndexers(
      updatesPerSecond: Int,
      indexerCount: Int,
      jdbcUrl: String,
  )(implicit resourceContext: ResourceContext, materializer: Materializer): Resource[Indexers] = {
    val indexerConfig = IndexerConfig(
      participantId = EndlessReadService.participantId,
      jdbcUrl = jdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      enableAppendOnlySchema = true,
      haConfig = HaConfig(enable = true),
    )

    newLoggingContext { implicit loggingContext =>
      for {
        // This execution context is not used for indexing in the append-only schema, it can be shared
        servicesExecutionContext <- ResourceOwner
          .forExecutorService(() => Executors.newWorkStealingPool())
          .map(ExecutionContext.fromExecutorService)
          .acquire()

        // Start N indexers that all compete for the same database
        _ = println(s"Starting $indexerCount indexers")
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
                  // Create an indexer and immediately start it
                  indexing <- new StandaloneIndexerServer(
                    readService = readService,
                    config = indexerConfig,
                    servicesExecutionContext = servicesExecutionContext,
                    metrics = metrics,
                    lfValueTranslationCache = LfValueTranslationCache.Cache.none,
                  ).acquire()
                } yield ReadServiceAndIndexer(readService, indexing)
              )
          )
          .map(xs => Indexers(xs.toList))
      } yield indexers
    }
  }
}
