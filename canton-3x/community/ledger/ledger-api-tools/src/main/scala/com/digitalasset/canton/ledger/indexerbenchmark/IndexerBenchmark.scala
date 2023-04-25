// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.indexerbenchmark

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.JvmMetricSet
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, ProxyMetricsFactory}
import com.daml.resources
import com.daml.telemetry.OpenTelemetryOwner
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{ReadService, Update}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.LedgerApiServer
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.{Indexer, IndexerServiceOwner, JdbcIndexer}
import com.digitalasset.canton.platform.store.DbSupport.DataSourceProperties
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.io.StdIn

// TODO(i12337): Use it or remove it
class IndexerBenchmark extends NamedLogging {

  override val loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root

  private val directEc = DirectExecutionContext(noTracingLogger)

  def run(
      createUpdates: () => Future[Source[(Offset, Traced[Update]), NotUsed]],
      config: Config,
      dataSourceProperties: DataSourceProperties,
      highAvailability: HaConfig,
  ): Future[Unit] = {
    withNewTraceContext { implicit traceContext =>
      val system = ActorSystem("IndexerBenchmark")
      implicit val materializer: Materializer = Materializer(system)
      implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

      val indexerExecutor = Executors.newWorkStealingPool()
      val indexerExecutionContext = ExecutionContext.fromExecutor(indexerExecutor)
      val tracer: Tracer = NoReportingTracerProvider.tracer

      println("Generating state updates...")
      val updates = Await.result(createUpdates(), Duration(10, "minute"))

      println("Creating read service and indexer...")
      val readService = createReadService(updates)

      val resource = for {
        metrics <- metricsResource(config).acquire()
        servicesExecutionContext <- ResourceOwner
          .forExecutorService(() => Executors.newWorkStealingPool())
          .map(ExecutionContext.fromExecutorService)
          .acquire()
        (inMemoryState, inMemoryStateUpdaterFlow) <-
          LedgerApiServer
            .createInMemoryStateAndUpdater(
              config.indexServiceConfig,
              256,
              metrics,
              indexerExecutionContext,
              tracer,
              loggerFactory,
              multiDomainEnabled = false,
            )
            .acquire()
        indexerFactory = new JdbcIndexer.Factory(
          config.participantId,
          config.dataSource,
          config.indexerConfig,
          readService,
          metrics,
          inMemoryState,
          inMemoryStateUpdaterFlow,
          servicesExecutionContext,
          tracer,
          loggerFactory,
          multiDomainEnabled = false,
          dataSourceProperties,
          highAvailability,
        )
        _ = println("Setting up the index database...")
        indexer <- indexer(config, indexerExecutionContext, indexerFactory)
        _ = println("Starting the indexing...")
        startTime = System.nanoTime()
        handle <- indexer.acquire()
        _ <- Resource.fromFuture(handle)
        stopTime = System.nanoTime()
        _ = println("Indexing done.")
        _ <- Resource.fromFuture(system.terminate())
        _ = indexerExecutor.shutdown()
      } yield {
        val result = new IndexerBenchmarkResult(
          config,
          metrics,
          startTime,
          stopTime,
        )

        println(result.banner)

        // Note: this allows the user to inspect the contents of an ephemeral database
        if (config.waitForUserInput) {
          println(
            s"Index database is still running at ${config.dataSource.jdbcUrl}."
          )
          StdIn.readLine("Press <enter> to terminate this process.").discard
        }

        if (result.failure) throw new RuntimeException("Indexer Benchmark failure.")
        ()
      }
      resource.asFuture
    }
  }

  private def indexer(
      config: Config,
      indexerExecutionContext: ExecutionContextExecutor,
      indexerFactory: JdbcIndexer.Factory,
  )(implicit
      traceContext: TraceContext,
      rc: ResourceContext,
  ): resources.Resource[ResourceContext, Indexer] =
    Await
      .result(
        IndexerServiceOwner
          .migrateOnly(config.dataSource.jdbcUrl, loggerFactory)
          .map(_ => indexerFactory.initialized(logger))(indexerExecutionContext),
        Duration(5, "minute"),
      )
      .acquire()

  private def metricsResource(config: Config) = {
    OpenTelemetryOwner(setAsGlobal = true, config.metricsReporter, Seq.empty).flatMap {
      openTelemetry =>
        val registry = new MetricRegistry
        val dropwizardFactory = new DropwizardMetricsFactory(registry)
        val openTelemetryFactory =
          new OpenTelemetryMetricsFactory(openTelemetry.getMeter("indexer-benchmark"))
        val inMemoryMetricFactory = new InMemoryMetricsFactory
        JvmMetricSet.registerObservers(openTelemetry)
        registry.registerAll(new JvmMetricSet)
        val metrics = new Metrics(
          new ProxyMetricsFactory(
            dropwizardFactory,
            inMemoryMetricFactory,
          ),
          new ProxyMetricsFactory(openTelemetryFactory, inMemoryMetricFactory),
          registry,
          reportExecutionContextMetrics = true,
        )
        config.metricsReporter
          .fold(ResourceOwner.unit)(reporter =>
            ResourceOwner
              .forCloseable(() => reporter.register(metrics.registry))
              .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
          )
          .map(_ => metrics)
    }
  }

  private[this] def createReadService(
      updates: Source[(Offset, Traced[Update]), NotUsed]
  ): ReadService = {
    new ReadService {
      override def stateUpdates(
          beginAfter: Option[Offset]
      )(implicit traceContext: TraceContext): Source[(Offset, Traced[Update]), NotUsed] = {
        assert(beginAfter.isEmpty, s"beginAfter is $beginAfter")
        updates
      }

      override def currentHealth(): HealthStatus = Healthy
    }
  }

  def runAndExit(
      config: Config,
      dataSourceProperties: DataSourceProperties,
      highAvailability: HaConfig,
      updates: () => Future[Source[(Offset, Traced[Update]), NotUsed]],
  ): Unit = {
    val result: Future[Unit] = new IndexerBenchmark()
      .run(updates, config, dataSourceProperties, highAvailability)
      .recover { case ex =>
        logger.error("Error running benchmark", ex)(TraceContext.empty)
        sys.exit(1)
      }(directEc)

    Await.result(result, Duration(100, "hour"))
    println("Done.")
    // We call sys.exit because some actor system or thread pool is still running, preventing a normal shutdown.
    sys.exit(0)
  }
}

object IndexerBenchmark {
  private val LedgerId = "IndexerBenchmarkLedger"
}
