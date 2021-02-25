package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.`export`.{ProtobufBasedLedgerDataImporter, WriteSet}
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.PerfSupport._
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.poc.PerfSupport.OneTenHundredCounter
import com.daml.platform.indexer.poc.StaticMetrics
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, JdbcIndexer}
import com.daml.platform.store.dao.events.LfValueTranslation

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object IndexerPerfTest {
  def run[LogResult](importer: ProtobufBasedLedgerDataImporter,
                     config: Config,
                     writeSetToUpdates: (WriteSet, Long) => Iterable[(Offset, Update)],
                     defaultExecutionContext: ExecutionContext)
                    (implicit materializer: Materializer): Future[Unit] = {
    println(
      s"""Config
         |  exportFilePath ${config.exportFilePath}
         |  performByteComparison ${config.performByteComparison}
         |  sortWriteSet ${config.sortWriteSet}
         |  indexOnly ${config.indexOnly}
         |  reportMetrics ${config.reportMetrics}
         |  jdbcUrl ${config.jdbcUrl}
         |  indexerPerfTest ${config.indexerPerfTest}
         |  deserMappingPar ${config.deserMappingPar}
         |  deserMappingBatchSize ${config.deserMappingBatchSize}
         |  inputMappingParallelism ${config.inputMappingParallelism}
         |  ingestionParallelism ${config.ingestionParallelism}
         |  submissionBatchSize ${config.submissionBatchSize}
         |  tailingRateLimitPerSecond ${config.tailingRateLimitPerSecond}
         |  batchWithinMillis ${config.batchWithinMillis}
         |  streamExport ${config.streamExport}
         |  cycleRun ${config.cycleRun}
         |  initSubmissionSize ${config.initSubmissionSize}
         |  runStageUntil ${config.runStageUntil}
         |""".stripMargin)

    val ReadServiceMappingParallelism = config.deserMappingPar
    val ReadServiceBatchSize = config.deserMappingBatchSize
    val InitSubmissionSize = config.initSubmissionSize

    val workerE = Executors.newFixedThreadPool(ReadServiceMappingParallelism)
    val workerEC = ExecutionContext.fromExecutorService(workerE)

    val importIterator =
      if (config.streamExport) {
        @volatile var rollingForwardImporter: ProtobufBasedLedgerDataImporter = importer
        () => {
          rollingForwardImporter.close()
          rollingForwardImporter = ProtobufBasedLedgerDataImporter(config.exportFilePath)
          rollingForwardImporter.read().iterator.map(_._2).zipWithIndex.drop(InitSubmissionSize)
        }
      } else {
        log("Start loading export...")
        val loaded = importer.read().map(_._2).zipWithIndex.drop(InitSubmissionSize).toVector
        log("Export loaded")
        () => loaded.iterator
      }

    //    val (initIterator, mainIterator) = importer.read()
    //      .iterator
    //      .map(_._2)
    //      .zipWithIndex
    //      .span(_._2 < InitSubmissionSize)
    //

    log("Create streamed LedgerReader...")
    val readServiceMappingCounter = OneTenHundredCounter()
    val submissionCounter = OneTenHundredCounter()
    val initLedgerReader = {
      //      val stream: Source[(Offset, Update), NotUsed] = Source.fromIterator(() => initIterator)
      //        .flatMapConcat {
      //          case (ws, index) =>
      //            Source(writeSetToUpdates(ws, index.toLong).toList)
      //        }

      new ReadService {
        override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = throw new UnsupportedOperationException

        override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = Source.empty

        override def currentHealth(): HealthStatus = HealthStatus.healthy
      }
    }

    val importBackedStreamingReadService = {
      val stream: Source[(Offset, Update), NotUsed] =
        (if (config.cycleRun) Source.cycle(importIterator) else Source.fromIterator(importIterator))
          .groupedWithin(ReadServiceBatchSize, FiniteDuration(50, "millis"))
          .mapAsync(ReadServiceMappingParallelism)(runOnWorkerWithMetrics(workerEC, readServiceMappingCounter) {
            batch => {
              //            println(s"${batch.size}   ${batch.view.map(_._1.view.map(_._2.size).sum).sum}")
              batch.iterator
                .flatMap { case (ws, index) => writeSetToUpdates(ws, index.toLong) }
                .toVector
            }
          })
          .flatMapConcat(Source.apply)
          .map {
            in =>
              submissionCounter.add(1L)
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
      val indexerConfig = IndexerConfig(
        participantId = ParticipantId.assertFromString("IntegrityCheckerParticipant"),
        jdbcUrl = config.jdbcUrl.get,
        startupMode = IndexerStartupMode.MigrateAndStart,
        inputMappingParallelism = config.inputMappingParallelism,
        ingestionParallelism = config.ingestionParallelism,
        submissionBatchSize = config.submissionBatchSize,
        tailingRateLimitPerSecond = config.tailingRateLimitPerSecond,
        batchWithinMillis = config.batchWithinMillis,
        runStageUntil = config.runStageUntil
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
        indexer =>
          Future.successful {
            indexer.subscription(initLedgerReader).use(_.completed())(resourceContext).waitforit

            submissionCounter.retrieveAndReset
            readServiceMappingCounter.retrieveAndReset
            StaticMetrics.batchCounter.retrieveAndReset
            StaticMetrics.ingestionCPU.retrieveAndReset
            StaticMetrics.seqMappingCPU.retrieveAndReset
            StaticMetrics.mappingCPU.retrieveAndReset
            StaticMetrics.dbCallHistrogram.retrieveAndReset
            log("start reporting...")
            @volatile var progress = "Not started yet"
            @volatile var submissionsProcessed = 0L
            val stopOneSecReporter = everyMillis(1000, 1000, runAfterShutdown = true) {
              val (c, c10, c100) = submissionCounter.retrieveAndReset
              submissionsProcessed += c
              val (_, readServiceNanos, _) = readServiceMappingCounter.retrieveAndReset
              val (_, mappingNanos, _) = StaticMetrics.mappingCPU.retrieveAndReset
              val (_, seqMappingNanos, _) = StaticMetrics.seqMappingCPU.retrieveAndReset
              val (_, ingestionNanos, _) = StaticMetrics.ingestionCPU.retrieveAndReset
              val averageBatchSize = StaticMetrics.batchCounter.retrieveAverage
              val dbCallHist = StaticMetrics.dbCallHistrogram.retrieveAndReset
              progress = s"progress: ${(submissionsProcessed / 2).toString.padRight(7)} trades"
              println(
                s"$now $progress ${
                  (c / 2).toString.padRight(4)
                } trade/s 10: ${
                  (c10 / 20).toString.padRight(4)
                } trade/s 100: ${
                  (c100 / 200).toString.padRight(4)
                } trade/s averageBatchSize: ${
                  averageBatchSize.getOrElse("-").toString.padRight(4)
                } read-service-cpu: ${
                  (readServiceNanos / 100000000L).toString.padRight(4)
                }% mapping-cpu: ${
                  (mappingNanos / 100000000L).toString.padRight(4)
                }% seq-mapping-cpu: ${
                  (seqMappingNanos / 100000000L).toString.padRight(4)
                }% ingesting-cpu: ${
                  (ingestionNanos / 100000000L).toString.padRight(4)
                }% db call histogram [0, 0.1ms, 1ms, 10ms, 100ms, 1s, 10s] ${
                  dbCallHist
                    .map(micro => micro.toString.padRight(5))
                    .mkString("[", ", ", "]")
                }")
            }

            log("start ingestion >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            indexer.subscription(importBackedStreamingReadService).use(_.completed())(resourceContext).waitforit

            log(s"finished ingestion >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            log(s"release reasources...")
            stopOneSecReporter()
          }
      }(resourceContext).waitforit
    }

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
