// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import java.util.concurrent.{Executors, TimeUnit}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Snapshot}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1.{
  Configuration,
  LedgerInitialConditions,
  Offset,
  ReadService,
  TimeModel,
  Update,
}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.JdbcIndexer
import com.daml.platform.store.LfValueTranslationCache
import com.daml.testing.postgresql.PostgresResource

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.StdIn

class IndexerBenchmark() {

  /** Same as [[IndexerBenchmark.run]], but overrides the JDBC url to point to an ephemeral Postgres database.
    *
    * Using an uncontrolled local database does not give good performance results, but is useful for development
    * and functional tests.
    */
  def runWithEphemeralPostgres(
      createUpdates: String => Future[Iterator[(Offset, Update)]],
      config: Config,
  ): Unit = {
    implicit val context: ExecutionContext = DirectExecutionContext
    PostgresResource
      .owner()
      .use(db => {
        println(s"Running the indexer benchmark against the ephemeral Postgres database ${db.url}")
        run(createUpdates, config.copy(indexerConfig = config.indexerConfig.copy(jdbcUrl = db.url)))
        println(s"Run finished")
        Future.unit
      })
    ()
  }

  def run(
      createUpdates: String => Future[Iterator[(Offset, Update)]],
      config: Config,
  ): Unit = {
    val metricRegistry = new MetricRegistry
    val metrics = new Metrics(metricRegistry)
    metrics.registry.registerAll(new JvmMetricSet)

    val system = ActorSystem("IndexerBenchmark")
    implicit val materializer: Materializer = Materializer(system)
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

    val indexerE = Executors.newWorkStealingPool()
    val indexerEC = ExecutionContext.fromExecutor(indexerE)

    newLoggingContext { implicit loggingContext =>
      println("Generating state updates...")
      val updates = Await.result(createUpdates(config.updateSource), Duration(5, "minute"))

      println("Creating read service and indexer...")
      val readService = createReadService(updates, config)
      val indexerFactory = new JdbcIndexer.Factory(
        ServerRole.Indexer,
        config.indexerConfig,
        readService,
        indexerEC,
        metrics,
        LfValueTranslationCache.Cache.none,
      )

      val resource = for {
        _ <- config.metricsReporter.fold(Resource.unit)(reporter =>
          ResourceOwner
            .forCloseable(() => reporter.register(metrics.registry))
            .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
            .acquire()
        )

        _ = println("Setting up the index database...")
        indexer <- Await
          .result(indexerFactory.migrateSchema(false), Duration(100, "hour"))
          .acquire()

        _ = println("Starting the indexing...")
        startTime = System.nanoTime()
        handle <- indexer.subscription(readService).acquire()

        _ <- Resource.fromFuture(handle.completed())
        stopTime = System.nanoTime()
        _ = println("Indexing done.")
      } yield {
        val duration: Double = (stopTime - startTime).toDouble / 1000000000.0
        val updates: Long = metrics.daml.parallelIndexer.updates.getCount
        println(
          s"""
             |--------------------------------------------------------------------------------
             |Indexer benchmark results
             |--------------------------------------------------------------------------------
             |
             |Input:
             |  source:   ${config.updateSource}
             |  count:    ${config.updateCount}
             |  jdbcUrl:  ${config.indexerConfig.jdbcUrl}
             |
             |Indexer parameters:
             |  inputMappingParallelism:   ${config.indexerConfig.inputMappingParallelism}
             |  ingestionParallelism:      ${config.indexerConfig.ingestionParallelism}
             |  submissionBatchSize:       ${config.indexerConfig.submissionBatchSize}
             |  batchWithinMillis:         ${config.indexerConfig.batchWithinMillis}
             |  tailingRateLimitPerSecond: ${config.indexerConfig.tailingRateLimitPerSecond}
             |
             |Result:
             |  duration:    $duration
             |  updates:     $updates
             |  updates/sec: ${updates / duration}
             |
             |Other metrics:
             |  inputMapping.batchSize:     ${histogramToString(
            metrics.daml.parallelIndexer.inputMapping.batchSize.getSnapshot
          )}
             |  inputMapping.duration:      ${histogramToString(
            metrics.daml.parallelIndexer.inputMapping.duration.getSnapshot
          )}
             |  inputMapping.duration.rate: ${metrics.daml.parallelIndexer.inputMapping.duration.getMeanRate}
             |  ingestion.duration:         ${histogramToString(
            metrics.daml.parallelIndexer.ingestion.duration.getSnapshot
          )}
             |  ingestion.duration.rate:    ${metrics.daml.parallelIndexer.ingestion.duration.getMeanRate}
             |
             |""".stripMargin
        )

        // Note: this allows the user to inpsect the contents of an ephemeral database
        if (config.waitForUserInput) {
          println(s"Index database is still running at ${config.indexerConfig.jdbcUrl}.")
          StdIn.readLine("Press <enter> to terminate this process.")
        }
      }
      Await.result(resource.asFuture, Duration(100, "hour"))
      system.terminate()
      ()
    }
  }

  private[this] def createReadService(
      updates: Iterator[(Offset, Update)],
      config: Config,
  ): ReadService = {
    val initialConditions = LedgerInitialConditions(
      IndexerBenchmark.LedgerId,
      Configuration(
        generation = 0,
        timeModel = TimeModel.reasonableDefault,
        maxDeduplicationTime = java.time.Duration.ofDays(1),
      ),
      Time.Timestamp.Epoch,
    )

    new ReadService {
      override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = {
        Source.single(initialConditions)
      }
      override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
        assert(beginAfter.isEmpty)
        config.updateCount match {
          case None =>
            Source.fromIterator(() => updates)
          case Some(updateCount) =>
            Source
              .cycle(() => {
                println("(Re)starting the stream of updates")
                updates
              }) // TODO append-only: cycling the exact same updates is probably not a good idea
              .take(updateCount)
        }
      }
      override def currentHealth(): HealthStatus = HealthStatus.healthy
    }
  }

  private[this] def histogramToString(data: Snapshot): String = {
    s"[min: ${data.getMin}, median: ${data.getMedian}, max: ${data.getMax}]"
  }
}

object IndexerBenchmark {
  val LedgerId = "IndexerBenchmarkLedger"

  def runAndExit(
      args: Array[String],
      updates: String => Future[Iterator[(Offset, Update)]],
  ): Unit = {
    val config: Config = Config.parse(args).getOrElse {
      sys.exit(1)
    }
    IndexerBenchmark.runAndExit(config, updates)
  }

  def runAndExit(
      config: Config,
      updates: String => Future[Iterator[(Offset, Update)]],
  ): Unit = {
    if (config.indexerConfig.jdbcUrl.isEmpty) {
      new IndexerBenchmark().runWithEphemeralPostgres(updates, config)
    } else {
      new IndexerBenchmark().run(updates, config)
    }
  }
}
