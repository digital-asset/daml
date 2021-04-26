package com.daml.ledger.participant.state.kvutils.tools.reindexfromstate

import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.OffsetBuilder
import com.daml.ledger.participant.state.kvutils.`export`.LedgerDataImporter
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.IndexingFailureException
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.{CommitStrategySupport, Config, color}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{Indexer, IndexerConfig, IndexerStartupMode, JdbcIndexer}
import com.daml.platform.store.LfValueTranslationCache

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class UpdatesIndexer[LogResult](commitStrategySupportBuilder: Metrics => CommitStrategySupport[LogResult]) {
  private val metricRegistry = new MetricRegistry
  private val metrics = new Metrics(metricRegistry)
  private val commitStrategySupport = commitStrategySupportBuilder(metrics)

  def run(
           importer: LedgerDataImporter,
           config: Config,
         )(implicit executionContext: ExecutionContext, materializer: Materializer): Future[Unit] =
    indexUpdatesFromState(config, importer)

  private def indexUpdatesFromState(
                                     config: Config,
                                     importer: LedgerDataImporter,
                                   )(implicit
                                     executionContext: ExecutionContext,
                                     materializer: Materializer,
                                   ): Future[Unit] =
    for {
      _ <- processSubmissions(importer)
      snapshotState = commitStrategySupport.currentState()
      reindexer = new StateToUpdateMapping(snapshotState.mutableState())
      updates = reindexer.generateUpdates()
      offsetPlusUpdates = updates.zipWithIndex.map { case (update, index) =>
        (OffsetBuilder.fromLong(index.toLong), update)
      }
      readService = createReadServiceReplayingUpdates(offsetPlusUpdates)
      _ <- indexStateItems(config, 0, readService)
    } yield ()

  private def createReadServiceReplayingUpdates(offsetPlusUpdates: Iterable[(Offset, Update)]): ReadService =
    new ReadService {
      override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
        Source.single(LedgerInitialConditions(
          "FakeId",
          LedgerReader.DefaultConfiguration,
          Time.Timestamp.Epoch,
        ))

      override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
        Source.fromIterator(() => offsetPlusUpdates.toIterator)

      override def currentHealth(): HealthStatus = HealthStatus.healthy
    }

  private def indexStateItems(
                               config: Config,
                               updateCount: Int,
                               readService: ReadService,
                             )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

    // Start the indexer consuming the recorded state updates
    println(s"Starting to re-index $updateCount updates.".white)
    newLoggingContext { implicit loggingContext =>
      val feedHandleResourceOwner = for {
        indexer <- migrateAndStartIndexer(
          createIndexerConfig(config),
          readService,
          metrics,
          LfValueTranslationCache.Cache.none,
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
          println("Successfully re-indexed all updates.".green)
          val durationSeconds = Duration
            .fromNanos(System.nanoTime() - startTime)
            .toMillis
            .toDouble / 1000.0
          val updatesPerSecond = updateCount / durationSeconds
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
        commitStrategySupport.commit(submissionInfo)
      }
      .runWith(Sink.fold(0)((n, _) => n + 1))
      .map { counter =>
        println(s"Processed $counter submissions.".green)
        println()
      }
  }

  // FIXME(miklos): Reuse from integritycheck package.
  private def migrateAndStartIndexer(
                                      config: IndexerConfig,
                                      readService: ReadService,
                                      metrics: Metrics,
                                      lfValueTranslationCache: LfValueTranslationCache.Cache,
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

  private def createIndexerConfig(config: Config): IndexerConfig =
    IndexerConfig(
      participantId = ParticipantId.assertFromString("IntegrityCheckerParticipant"),
      jdbcUrl = jdbcUrl(config),
      startupMode = IndexerStartupMode.MigrateAndStart,
    )

  private def jdbcUrl(config: Config): String =
    config.jdbcUrl.getOrElse(defaultJdbcUrl(config.exportFileName))

  private def defaultJdbcUrl(exportFileName: String): String =
    s"jdbc:h2:mem:$exportFileName;db_close_delay=-1;db_close_on_exit=false"

}
