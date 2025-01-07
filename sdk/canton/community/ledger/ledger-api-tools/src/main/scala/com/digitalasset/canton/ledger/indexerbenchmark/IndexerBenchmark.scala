// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// TODO(i12337): Use it or remove it
//// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
//// SPDX-License-Identifier: Apache-2.0
//
//package com.digitalasset.canton.ledger.indexerbenchmark
//
//import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
//import com.daml.metrics.api.noop.NoOpMetricsFactory
//import com.daml.metrics.api.{HistogramInventory, MetricName}
//import com.daml.resources
//import com.digitalasset.canton.concurrent.DirectExecutionContext
//import com.digitalasset.canton.data.Offset
//import com.digitalasset.canton.discard.Implicits.DiscardOps
//import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy}
//import com.digitalasset.canton.ledger.participant.state.{
//  InternalStateServiceProviderImpl,
//  ReadService,
//  Update,
//}
//import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
//import com.digitalasset.canton.metrics.{LedgerApiServerHistograms, LedgerApiServerMetrics}
//import com.digitalasset.canton.platform.LedgerApiServer
//import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
//import com.digitalasset.canton.platform.indexer.ha.HaConfig
//import com.digitalasset.canton.platform.indexer.{Indexer, IndexerServiceOwner, JdbcIndexer}
//import com.digitalasset.canton.platform.store.DbSupport.DataSourceProperties
//import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
//import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
//import io.opentelemetry.api.trace.Tracer
//import org.apache.pekko.NotUsed
//import org.apache.pekko.actor.ActorSystem
//import org.apache.pekko.stream.Materializer
//import org.apache.pekko.stream.scaladsl.Source
//
//import java.util.concurrent.Executors
//import scala.concurrent.duration.Duration
//import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
//import scala.io.StdIn
//
//class IndexerBenchmark extends NamedLogging {
//
//  override val loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root
//
//  private val directEc = DirectExecutionContext(noTracingLogger)
//
//  def run(
//      createUpdates: () => Future[Source[(Offset, Update), NotUsed]],
//      config: Config,
//      dataSourceProperties: DataSourceProperties,
//      highAvailability: HaConfig,
//  ): Future[Unit] = {
//    withNewTraceContext { implicit traceContext =>
//      val system = ActorSystem("IndexerBenchmark")
//      implicit val materializer: Materializer = Materializer(system)
//      implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
//
//      val indexerExecutor = Executors.newWorkStealingPool()
//      val indexerExecutionContext = ExecutionContext.fromExecutor(indexerExecutor)
//      val tracer: Tracer = NoReportingTracerProvider.tracer
//
//      println("Generating state updates...")
//      val updates = Await.result(createUpdates(), Duration(10, "minute"))
//
//      println("Creating read service and indexer...")
//      val readService = createReadService(updates)
//      val metrics = new LedgerApiServerMetrics(
//        new LedgerApiServerHistograms(MetricName("noop"))(new HistogramInventory),
//        NoOpMetricsFactory,
//      )
//      val resource = for {
//        servicesExecutionContext <- ResourceOwner
//          .forExecutorService(() => Executors.newWorkStealingPool())
//          .map(ExecutionContext.fromExecutorService)
//          .acquire()
//        (inMemoryState, inMemoryStateUpdaterFlow) <-
//          LedgerApiServer
//            .createInMemoryStateAndUpdater(
//              CommandProgressTracker.NoOp,
//              config.indexServiceConfig,
//              256,
//              metrics,
//              indexerExecutionContext,
//              tracer,
//              loggerFactory,
//            )
//            .acquire()
//        indexerFactory = new JdbcIndexer.Factory(
//          config.participantId,
//          config.dataSource,
//          config.indexerConfig,
//          Set.empty,
//          readService,
//          metrics,
//          inMemoryState,
//          inMemoryStateUpdaterFlow,
//          servicesExecutionContext,
//          tracer,
//          loggerFactory,
//          dataSourceProperties,
//          highAvailability,
//          None,
//        )
//        _ = println("Setting up the index database...")
//        indexer <- indexer(config, indexerExecutionContext, indexerFactory)
//        _ = println("Starting the indexing...")
//        startTime = System.nanoTime()
//        handle <- indexer.acquire()
//        _ <- Resource.fromFuture(handle)
//        stopTime = System.nanoTime()
//        _ = println("Indexing done.")
//        _ <- Resource.fromFuture(system.terminate())
//        _ = indexerExecutor.shutdown()
//      } yield {
//        val result = new IndexerBenchmarkResult(
//          config,
//          metrics,
//          startTime,
//          stopTime,
//        )
//
//        println(result.banner)
//
//        // Note: this allows the user to inspect the contents of an ephemeral database
//        if (config.waitForUserInput) {
//          println(
//            s"Index database is still running at ${config.dataSource.jdbcUrl}."
//          )
//          StdIn.readLine("Press <enter> to terminate this process.").discard
//        }
//
//        if (result.failure) throw new RuntimeException("Indexer Benchmark failure.")
//        ()
//      }
//      resource.asFuture
//    }
//  }
//
//  private def indexer(
//      config: Config,
//      indexerExecutionContext: ExecutionContextExecutor,
//      indexerFactory: JdbcIndexer.Factory,
//  )(implicit
//      traceContext: TraceContext,
//      rc: ResourceContext,
//  ): resources.Resource[ResourceContext, Indexer] =
//    Await
//      .result(
//        IndexerServiceOwner
//          .migrateOnly(config.dataSource.jdbcUrl, loggerFactory)(rc.executionContext, traceContext)
//          .map(_ => indexerFactory.initialized(logger))(indexerExecutionContext),
//        Duration(5, "minute"),
//      )
//      .acquire()
//
//  private[this] def createReadService(
//      updates: Source[(Offset, Update), NotUsed]
//  ): ReadService = {
//    new ReadService with InternalStateServiceProviderImpl {
//      override def stateUpdates(
//          beginAfter: Option[Offset]
//      )(implicit traceContext: TraceContext): Source[(Offset, Update), NotUsed] = {
//        assert(beginAfter.isEmpty, s"beginAfter is $beginAfter")
//        updates
//      }
//
//      override def currentHealth(): HealthStatus = Healthy
//    }
//  }
//
//  def runAndExit(
//      config: Config,
//      dataSourceProperties: DataSourceProperties,
//      highAvailability: HaConfig,
//      updates: () => Future[Source[(Offset, Update), NotUsed]],
//  ): Unit = {
//    val result: Future[Unit] = new IndexerBenchmark()
//      .run(updates, config, dataSourceProperties, highAvailability)
//      .recover { case ex =>
//        logger.error("Error running benchmark", ex)(TraceContext.empty)
//        sys.exit(1)
//      }(directEc)
//
//    Await.result(result, Duration(100, "hour"))
//    println("Done.")
//    // We call sys.exit because some actor system or thread pool is still running, preventing a normal shutdown.
//    sys.exit(0)
//  }
//}
