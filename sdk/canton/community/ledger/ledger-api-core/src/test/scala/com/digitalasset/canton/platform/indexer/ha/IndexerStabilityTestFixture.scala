// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.{LedgerApiServerHistograms, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.LedgerApiServer
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.config.{CommandServiceConfig, IndexServiceConfig}
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig,
  IndexerServiceOwner,
  IndexerStartupMode,
}
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Stores a running indexer and the read service the indexer is reading from.
  * The read service is used exclusively by this indexer.
  */
final case class ReadServiceAndIndexer(
    readService: EndlessReadService,
    indexing: ReportsHealth,
)

final case class Indexers(indexers: List[ReadServiceAndIndexer]) {
  // The list of all indexers that are running (determined by whether they have subscribed to the read service)
  def runningIndexers: List[ReadServiceAndIndexer] =
    indexers.filter(_.readService.isRunning)
  def resetAll(): Unit = indexers.foreach(_.readService.reset())
}

final class IndexerStabilityTestFixture(loggerFactory: NamedLoggerFactory) {

  private val logger: TracedLogger = TracedLogger(loggerFactory.getLogger(getClass))
  val tracer: Tracer = NoReportingTracerProvider.tracer

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
    val indexerConfig = IndexerConfig()

    def createReaderAndIndexer(
        i: Int,
        participantDataSourceConfig: ParticipantDataSourceConfig,
        executionContext: ExecutionContext,
    )(implicit traceContext: TraceContext): Resource[ReadServiceAndIndexer] = {
      val loggerFactoryForIteration = loggerFactory.appendUnnamedKey("name", s"ReadService$i")
      for {
        // Create a read service
        readService <- ResourceOwner
          .forCloseable(() => EndlessReadService(updatesPerSecond, s"$i", loggerFactory))
          .acquire()
        // create a new MetricRegistry for each indexer, so they don't step on each other toes:
        // Gauges can only be registered once. A subsequent attempt results in an exception for the
        // call MetricRegistry#register or MetricRegistry#registerGauge
        metrics = {
          new LedgerApiServerMetrics(
            new LedgerApiServerHistograms(MetricName("test"))(new HistogramInventory()),
            NoOpMetricsFactory,
          )
        }
        (inMemoryState, inMemoryStateUpdaterFlow) <-
          LedgerApiServer
            .createInMemoryStateAndUpdater(
              commandProgressTracker = CommandProgressTracker.NoOp,
              IndexServiceConfig(),
              CommandServiceConfig.DefaultMaxCommandsInFlight,
              metrics,
              executionContext,
              tracer,
              loggerFactory,
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
          executionContext = executionContext,
          tracer = tracer,
          loggerFactory = loggerFactoryForIteration,
          startupMode = IndexerStartupMode.MigrateAndStart,
          dataSourceProperties = IndexerConfig.createDataSourcePropertiesForTesting(
            indexerConfig.ingestionParallelism.unwrap
          ),
          highAvailability = HaConfig(
            indexerLockId = lockIdSeed,
            indexerWorkerLockId = lockIdSeed + 1,
          ),
          indexServiceDbDispatcher = None,
        ).acquire()
      } yield ReadServiceAndIndexer(readService, indexing)
    }

    withNewTraceContext { implicit traceContext =>
      val participantDataSourceConfig = ParticipantDataSourceConfig(jdbcUrl)
      for {
        // This execution context is not used for indexing in the append-only schema, it can be shared
        servicesExecutionContext <- ResourceOwner
          .forExecutorService(() => Executors.newWorkStealingPool())
          .map(ExecutionContext.fromExecutorService)
          .acquire()

        // Start N indexers that all compete for the same database
        _ = logger.info(s"Starting $indexerCount indexers for database $jdbcUrl")
        migratingIndexer <- createReaderAndIndexer(
          0,
          participantDataSourceConfig,
          servicesExecutionContext,
        )
        indexers <- Resource
          .sequence(
            (1 until indexerCount).toList
              .map(
                createReaderAndIndexer(
                  _,
                  participantDataSourceConfig,
                  servicesExecutionContext,
                )
              )
          )
          .map(xs => Indexers(migratingIndexer +: xs.toList))
      } yield indexers
    }
  }
}
