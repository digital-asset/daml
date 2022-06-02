// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.{Configuration, LedgerInitialConditions, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ReadService, Update}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.indexer.{Indexer, JdbcIndexer, StandaloneIndexerServer}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.LfValueTranslationCache
import com.daml.resources
import com.daml.testing.postgresql.PostgresResource

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.io.StdIn

class IndexerBenchmark() {

  /** Same as [[IndexerBenchmark.run]], but overrides the JDBC url to point to an ephemeral Postgres database.
    *
    * Using an uncontrolled local database does not give good performance results, but is useful for development
    * and functional tests.
    */
  def runWithEphemeralPostgres(
      createUpdates: () => Future[Source[(Offset, Update), NotUsed]],
      config: Config,
  ): Future[Unit] =
    PostgresResource
      .owner()
      .use(db => {
        println(s"Running the indexer benchmark against the ephemeral Postgres database ${db.url}")
        run(
          createUpdates,
          config.copy(dataSource = ParticipantDataSourceConfig(db.url)),
        )
      })(ExecutionContext.parasitic)

  def run(
      createUpdates: () => Future[Source[(Offset, Update), NotUsed]],
      config: Config,
  ): Future[Unit] = {
    newLoggingContext { implicit loggingContext =>
      val metrics = new Metrics(new MetricRegistry)
      metrics.registry.registerAll(new JvmMetricSet)

      val system = ActorSystem("IndexerBenchmark")
      implicit val materializer: Materializer = Materializer(system)
      implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

      val indexerExecutor = Executors.newWorkStealingPool()
      val indexerExecutionContext = ExecutionContext.fromExecutor(indexerExecutor)

      println("Generating state updates...")
      val updates = Await.result(createUpdates(), Duration(10, "minute"))

      println("Creating read service and indexer...")
      val readService = createReadService(updates)
      val indexerFactory: JdbcIndexer.Factory = new JdbcIndexer.Factory(
        config.participantId,
        config.dataSource,
        config.indexerConfig,
        readService,
        metrics,
        LfValueTranslationCache.Cache.none,
      )

      val resource = for {
        _ <- metricsResource(config, metrics)
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
        val result = new IndexerBenchmarkResult(config, metrics, startTime, stopTime)

        println(result.banner)

        // Note: this allows the user to inpsect the contents of an ephemeral database
        if (config.waitForUserInput) {
          println(
            s"Index database is still running at ${config.dataSource.jdbcUrl}."
          )
          StdIn.readLine("Press <enter> to terminate this process.")
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
      loggingContext: LoggingContext,
      rc: ResourceContext,
  ): resources.Resource[ResourceContext, Indexer] =
    Await
      .result(
        StandaloneIndexerServer
          .migrateOnly(config.dataSource.jdbcUrl)
          .map(_ => indexerFactory.initialized())(indexerExecutionContext),
        Duration(5, "minute"),
      )
      .acquire()

  private def metricsResource(config: Config, metrics: Metrics)(implicit rc: ResourceContext) =
    config.metricsReporter.fold(Resource.unit)(reporter =>
      ResourceOwner
        .forCloseable(() => reporter.register(metrics.registry))
        .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
        .acquire()
    )

  private[this] def createReadService(
      updates: Source[(Offset, Update), NotUsed]
  ): ReadService = {
    val initialConditions = LedgerInitialConditions(
      IndexerBenchmark.LedgerId,
      Configuration(
        generation = 0,
        timeModel = LedgerTimeModel.reasonableDefault,
        maxDeduplicationDuration = java.time.Duration.ofDays(1),
      ),
      Time.Timestamp.Epoch,
    )

    new ReadService {
      override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = {
        Source.single(initialConditions)
      }

      override def stateUpdates(
          beginAfter: Option[Offset]
      )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] = {
        assert(beginAfter.isEmpty, s"beginAfter is $beginAfter")
        updates
      }

      override def currentHealth(): HealthStatus = Healthy
    }
  }
}

object IndexerBenchmark {
  val LedgerId = "IndexerBenchmarkLedger"

  def runAndExit(
      config: Config,
      updates: () => Future[Source[(Offset, Update), NotUsed]],
  ): Unit = {
    val result: Future[Unit] =
      (if (config.dataSource.jdbcUrl.isEmpty) {
         new IndexerBenchmark().runWithEphemeralPostgres(updates, config)
       } else {
         new IndexerBenchmark().run(updates, config)
       }).recover { case ex =>
        println(s"Error: ${ex.getMessage}")
        sys.exit(1)
      }(scala.concurrent.ExecutionContext.Implicits.global)

    Await.result(result, Duration(100, "hour"))
    println("Done.")
    // TODO: some actor system or thread pool is still running, preventing a shutdown
    sys.exit(0)
  }
}
