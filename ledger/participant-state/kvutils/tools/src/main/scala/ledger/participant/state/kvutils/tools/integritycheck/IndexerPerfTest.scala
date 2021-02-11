package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{Materializer, OverflowStrategy}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.`export`.{ProtobufBasedLedgerDataImporter, WriteSet}
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.PerfSupport._
import com.daml.ledger.participant.state.v1.{LedgerInitialConditions, Offset, ParticipantId, ReadService, Update}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, JdbcIndexer}
import com.daml.platform.store.dao.events.LfValueTranslation

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object IndexerPerfTest {
  def run[LogResult](importer: ProtobufBasedLedgerDataImporter,
                     config: Config,
                     writeSetToUpdates: (WriteSet, Long) => Iterable[(Offset, Update)],
                     defaultExecutionContext: ExecutionContext)
                    (implicit materializer: Materializer): Future[Unit] = {
    val ReadServiceMappingParallelism = 6
    val ReadServiceBatchSize = 100L
    val ReadServiceBufferSize = 20
    val InitSubmissionSize = 118

    val workerE = Executors.newFixedThreadPool(ReadServiceMappingParallelism)
    val workerEC = ExecutionContext.fromExecutorService(workerE)


    val (initIterator, mainIterator) = importer.read()
      .iterator
      .map(_._2)
      .zipWithIndex
      .span(_._2 < InitSubmissionSize)

    log("Create streamed LedgerReader...")
    val readServiceMappingCounter = OneTenHundredCounter()
    val submissionCounter = OneTenHundredCounter()

    val initLedgerReader = {
      val stream: Source[(Offset, Update), NotUsed] = Source.fromIterator(() => initIterator)
        .flatMapConcat {
          case (ws, index) =>
            Source(writeSetToUpdates(ws, index.toLong).toList)
        }

      new ReadService {
        override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = throw new UnsupportedOperationException
        override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = stream
        override def currentHealth(): HealthStatus = HealthStatus.healthy
      }
    }

    val importBackedStreamingReadService = {
      val stream: Source[(Offset, Update), NotUsed] = Source.fromIterator(() => mainIterator)
        .batch(ReadServiceBatchSize, mutable.ArrayBuffer.apply(_))(_ += _) //TODO arraybuffer with fix the size
        .buffer(ReadServiceBufferSize, OverflowStrategy.backpressure)
        .mapAsync(ReadServiceMappingParallelism)(runOnWorkerWithMetrics(workerEC, readServiceMappingCounter) {
          _.iterator
            .flatMap { case (ws, index) => writeSetToUpdates(ws, index.toLong) }
            .to
        })
        .flatMapConcat(Source.apply)
        .map {
          in =>

            submissionCounter.add(1)
            in
        }

      new ReadService {
        override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = throw new UnsupportedOperationException

        override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = stream

        override def currentHealth(): HealthStatus = HealthStatus.healthy
      }
    }

    val indexerE = Executors.newWorkStealingPool()
    val indexerEC = ExecutionContext.fromExecutor(indexerE)
    val resourceContext: ResourceContext = ResourceContext(defaultExecutionContext)
    log("Create indexer...")
    val indexer = {
      val initLedgerReadService = new ReadService {
        override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = Source.single(
          LedgerInitialConditions(
            "LedgerId",
            LedgerReader.DefaultConfiguration,
            Timestamp.Epoch,
          )
        )
        override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = throw new UnsupportedOperationException
        override def currentHealth(): HealthStatus = throw new UnsupportedOperationException
      }
      val metricRegistry = new MetricRegistry
      val metrics = new Metrics(metricRegistry)
      val indexerConfig =     IndexerConfig(
        participantId = ParticipantId.assertFromString("IntegrityCheckerParticipant"),
        jdbcUrl = config.jdbcUrl.get,
        startupMode = IndexerStartupMode.MigrateAndStart,
      )
      val indexerFactory = newLoggingContext { implicit loggingContext =>
        new JdbcIndexer.Factory(
          ServerRole.Indexer,
          indexerConfig,
          initLedgerReadService,
          indexerEC,
          metrics,
          LfValueTranslation.Cache.none,
        )
      }
      indexerFactory.migrateSchema(allowExistingSchema = false)(resourceContext).waitforit
    }

    log(s"start initialisation ($InitSubmissionSize submissions) .............................................")
    val result = Try {
      indexer.use {
        indexer => Future.successful {
          indexer.subscription(initLedgerReader).use(_.completed())(resourceContext).waitforit

          log("start reporting...")
          @volatile var progress = "Not started yet"
          @volatile var submissionsProcessed = 0L
          val stopOneSecReporter = everyMillis(1000, 1000, runAfterShutdown = true) {
            val (c, c10, c100) = submissionCounter.retrieveAndReset
            submissionsProcessed += c
            val (_, readServiceNanos, _) = readServiceMappingCounter.retrieveAndReset
            progress = s"progress: ${(submissionsProcessed / 2).toString.padRight(9)} trades ${
              (100 * submissionsProcessed / 16000).toString.padRight(2)
            }%"
            println(
              s"$now $progress ${
                (c / 2).toString.padRight(6)
              } trade/s 10: ${
                (c10 / 20).toString.padRight(6)
              } trade/s 100: ${
                (c100 / 200).toString.padRight(6)
              } trade/s read-service-cpu: ${
                (readServiceNanos / 100000000L).toString.padRight(4)
              }%")
          }

          log("start ingestion >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

          indexer.subscription(importBackedStreamingReadService).use(_.completed())(resourceContext).waitforit

          log(s"finished ingestion >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          log(s"release reasources...")
          stopOneSecReporter()
        }
      }(resourceContext).waitforit
    }

//    val result = Try(importBackedStreamingReadService.stateUpdates(None)
//      .run()
//      .waitforit
//    )

    workerEC.shutdownNow()
    indexerE.shutdownNow()
    if (result.isSuccess) {
      log(
        s"ALL DONE")
      System.exit(0)
    } else {
      log(s"FAILED")
      result.get
      ()
    }

    Future.successful(())
  }
}
