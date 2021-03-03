// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.kvutils.export.{
  LedgerDataImporter,
  ProtobufBasedLedgerDataImporter,
  WriteSet,
}
import com.daml.ledger.participant.state.v1.{Offset, ParticipantId, ReadService, Update}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{Indexer, IndexerConfig, IndexerStartupMode, JdbcIndexer}
import com.daml.platform.store.dao.events.LfValueTranslation

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

class IntegrityChecker[LogResult](
    commitStrategySupportBuilder: Metrics => CommitStrategySupport[LogResult]
) {
  private val metricRegistry = new MetricRegistry
  private val metrics = new Metrics(metricRegistry)
  private val commitStrategySupport = commitStrategySupportBuilder(metrics)
  private val writeSetComparison = commitStrategySupport.writeSetComparison

  import IntegrityChecker._

  def run(
      importer: LedgerDataImporter,
      config: Config,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): Future[Unit] = {

    if (config.indexOnly)
      println("Running indexing only".white)
    else
      println("Running full integrity check".white)

    if (config.indexOnly || !config.performByteComparison) {
      println("Skipping byte-for-byte comparison.".yellow)
      println()
    }

    val expectedReadServiceFactory = commitStrategySupport.newReadServiceFactory()
    val actualReadServiceFactory = commitStrategySupport.newReadServiceFactory()
    val stateUpdates = new ReadServiceStateUpdateComparison(
      expectedReadServiceFactory.createReadService,
      actualReadServiceFactory.createReadService,
    )

    checkIntegrity(
      config,
      importer,
      expectedReadServiceFactory,
      actualReadServiceFactory,
      stateUpdates,
      metrics,
    ).andThen {
      case _ if config.reportMetrics =>
        reportDetailedMetrics(metricRegistry)
    }
  }

  private def checkIntegrity(
      config: Config,
      importer: LedgerDataImporter,
      expectedReadServiceFactory: ReplayingReadServiceFactory,
      actualReadServiceFactory: ReplayingReadServiceFactory,
      stateUpdates: StateUpdateComparison,
      metrics: Metrics,
  )(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): Future[Unit] =
    for {
      _ <- processSubmissions(
        importer,
        expectedReadServiceFactory,
        actualReadServiceFactory,
        config,
      )
      _ <- compareStateUpdates(config, stateUpdates)
      _ <- indexStateUpdates(
        config = config,
        metrics = metrics,
        readService =
          if (config.indexOnly)
            expectedReadServiceFactory.createReadService
          else
            actualReadServiceFactory.createReadService,
      )
    } yield ()

  private[integritycheck] def compareStateUpdates(
      config: Config,
      stateUpdates: StateUpdateComparison,
  ): Future[Unit] =
    if (!config.indexOnly)
      stateUpdates.compare()
    else
      Future.unit

  private def indexStateUpdates(
      config: Config,
      metrics: Metrics,
      readService: ReplayingReadService,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

    // Start the indexer consuming the recorded state updates
    println(s"Starting to index ${readService.updateCount()} updates.".white)
    newLoggingContext { implicit loggingContext =>
      val feedHandleResourceOwner = for {
        indexer <- migrateAndStartIndexer(
          createIndexerConfig(config),
          readService,
          metrics,
          LfValueTranslation.Cache.none,
        )
        feedHandle <- indexer.subscription(readService)
      } yield (feedHandle, System.nanoTime())

      // Wait for the indexer to finish consuming the state updates.
      // This works because ReplayingReadService.stateUpdates() closes the update stream
      // when it is done streaming the recorded updates, and IndexFeedHandle.complete()
      // completes when it finishes consuming the state update stream.
      // Any failure (e.g., during the decoding of the recorded state updates, or
      // during the indexing of a state update) will result in a failed Future.
      feedHandleResourceOwner.use { case (feedHandle, startTime) =>
        Future.successful(startTime).zip(feedHandle.completed())
      }
    }.transform {
      case Success((startTime, _)) =>
        Success {
          println("Successfully indexed all updates.".green)
          val durationSeconds = Duration
            .fromNanos(System.nanoTime() - startTime)
            .toMillis
            .toDouble / 1000.0
          val updatesPerSecond = readService.updateCount() / durationSeconds
          println()
          println(s"Indexing duration: $durationSeconds seconds ($updatesPerSecond updates/second)")
        }
      case Failure(exception) =>
        val message =
          s"""Failure indexing updates: $exception
             |Indexer metrics:
             |  stateUpdateProcessing:  ${metrics.daml.indexer.stateUpdateProcessing.getCount}
             |  lastReceivedRecordTime: ${metrics.daml.indexer.lastReceivedRecordTime.getValue()}
             |  lastReceivedOffset:     ${metrics.daml.indexer.lastReceivedOffset.getValue()}
             |""".stripMargin
        Failure(new IndexingFailureException(message))
    }
  }

  private def processSubmissions(
      importer: LedgerDataImporter,
      expectedReadServiceFactory: ReplayingReadServiceFactory,
      actualReadServiceFactory: ReplayingReadServiceFactory,
      config: Config,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    println("Processing the ledger export.".white)

    Source(importer.read())
      .mapAsync(1) { case (submissionInfo, expectedWriteSet) =>
        println(
          "Read submission"
            + s" correlationId=${submissionInfo.correlationId}"
            + s" submissionEnvelopeSize=${submissionInfo.submissionEnvelope.size}"
            + s" writeSetSize=${expectedWriteSet.size}"
        )
        expectedWriteSet.foreach { case (key, value) =>
          val result = writeSetComparison.checkEntryIsReadable(key, value)
          result.left.foreach { message =>
            throw new UnreadableWriteSetException(message)
          }
        }
        expectedReadServiceFactory.appendBlock(expectedWriteSet)
        if (!config.indexOnly) {
          commitStrategySupport.commit(submissionInfo) map { actualWriteSet =>
            val orderedActualWriteSet =
              if (config.sortWriteSet)
                actualWriteSet.sortBy(_._1)
              else
                actualWriteSet
            actualReadServiceFactory.appendBlock(orderedActualWriteSet)

            if (config.performByteComparison)
              writeSetComparison.compareWriteSets(expectedWriteSet, orderedActualWriteSet) match {
                case None =>
                  println("OK".green)
                case Some(message) =>
                  throw new ComparisonFailureException(message)
              }
          }
        } else {
          Future.unit
        }
      }
      .runWith(Sink.fold(0)((n, _) => n + 1))
      .map { counter =>
        println(s"Processed $counter submissions.".green)
        println()
      }
  }

  private def migrateAndStartIndexer(
      config: IndexerConfig,
      readService: ReadService,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit
      resourceContext: ResourceContext,
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[Indexer] =
    for {
      servicesExecutionContext <- ResourceOwner
        .forExecutorService(() => Executors.newWorkStealingPool())
        .map(ExecutionContext.fromExecutorService)
      indexerFactory = new JdbcIndexer.Factory(
        ServerRole.Indexer,
        config,
        readService,
        servicesExecutionContext,
        metrics,
        lfValueTranslationCache,
      )
      migrating <- ResourceOwner.forFuture(() =>
        indexerFactory.migrateSchema(allowExistingSchema = false)
      )
      migrated <- migrating
    } yield migrated
}

object IntegrityChecker {
  type CommitStrategySupportFactory[LogResult] =
    (Metrics, ExecutionContext) => CommitStrategySupport[LogResult]

  abstract class CheckFailedException(message: String) extends RuntimeException(message)

  final class UnreadableWriteSetException(message: String) extends CheckFailedException(message)

  final class ComparisonFailureException(lines: String*)
      extends CheckFailedException(("FAIL" +: lines).mkString(System.lineSeparator))

  final class IndexingFailureException(message: String) extends CheckFailedException(message)

  def runAndExit[LogResult](
      args: Array[String],
      commitStrategySupportFactory: CommitStrategySupportFactory[LogResult],
      writeSetToUpdates: Option[(WriteSet, Long) => Iterable[(Offset, Update)]],
  ): Unit = {
    val config = Config.parse(args).getOrElse {
      sys.exit(1)
    }
    runAndExit(config, commitStrategySupportFactory, writeSetToUpdates)
  }

  def runAndExit[LogResult](
      config: Config,
      commitStrategySupportFactory: CommitStrategySupportFactory[LogResult],
      writeSetToUpdates: Option[(WriteSet, Long) => Iterable[(Offset, Update)]],
  ): Unit = {
    println(s"Verifying integrity of ${config.exportFilePath}...")

    val actorSystem: ActorSystem = ActorSystem("integrity-checker")
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    implicit val materializer: Materializer = Materializer(actorSystem)

    val importer = ProtobufBasedLedgerDataImporter(config.exportFilePath)

    if (config.indexerPerfTest)
      IndexerPerfTest.run(importer, config, writeSetToUpdates.get, executionContext)

    new IntegrityChecker(commitStrategySupportFactory(_, executionContext))
      .run(importer, config)
      .onComplete {
        case Success(_) =>
          sys.exit(0)
        case Failure(exception: CheckFailedException) =>
          println(exception.getMessage.red)
          sys.exit(1)
        case Failure(exception) =>
          exception.printStackTrace()
          sys.exit(1)
      }(DirectExecutionContext)
  }

  private[integritycheck] def createIndexerConfig(config: Config): IndexerConfig =
    IndexerConfig(
      participantId = ParticipantId.assertFromString("IntegrityCheckerParticipant"),
      jdbcUrl = jdbcUrl(config),
      startupMode = IndexerStartupMode.MigrateAndStart,
    )

  private[integritycheck] def jdbcUrl(config: Config): String =
    config.jdbcUrl.getOrElse(defaultJdbcUrl(config.exportFileName))

  private[integritycheck] def defaultJdbcUrl(exportFileName: String): String =
    s"jdbc:h2:mem:$exportFileName;db_close_delay=-1;db_close_on_exit=false"

  def reportDetailedMetrics(metricRegistry: MetricRegistry): Unit = {
    val reporter = ConsoleReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build
    reporter.report()
  }
}
