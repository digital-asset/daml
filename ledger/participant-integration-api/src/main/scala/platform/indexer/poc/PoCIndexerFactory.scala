// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.poc

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import com.codahale.metrics.Gauge
import com.daml.ledger.participant.state.v1.{Offset, ParticipantId, ReadService, Update}
import com.daml.ledger.resources.{Resource, ResourceOwner}
import com.daml.metrics.Metrics
import com.daml.platform.indexer.poc.AsyncSupport._
import com.daml.platform.indexer.poc.PerfSupport._
import com.daml.platform.indexer.poc.StaticMetrics._
import com.daml.platform.indexer.{Indexer, SubscriptionIndexFeedHandle}
import com.daml.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object PoCIndexerFactory {

  // TODO migrate code for mutable initialisation
  def apply(
      jdbcUrl: String,
      participantId: ParticipantId,
      translation: LfValueTranslation,
      compressionStrategy: CompressionStrategy,
      mat: Materializer,
      inputMappingParallelism: Int,
      ingestionParallelism: Int,
      submissionBatchSize: Long,
      tailingRateLimitPerSecond: Int,
      batchWithinMillis: Long,
      runStageUntil: Int,
      metrics: Metrics,
  ): ResourceOwner[Indexer] = {
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        Some(metrics.daml.pocIndexer.inputMappingExecutor -> metrics.registry),
      )
      postgresDaoPool <- asyncResourcePool(
        () => JDBCPostgresDAO(jdbcUrl),
        size = ingestionParallelism + 1,
        Some(metrics.daml.pocIndexer.ingestionExecutor -> metrics.registry),
      )
    } yield {
      val batchCounterMetric = CountedCounter()
      val batchCounterGauge: Gauge[Int] = () =>
        batchCounterMetric.retrieveAverage.getOrElse(0L).toInt
      metrics.registry.register(metrics.daml.pocIndexer.batchSize, batchCounterGauge)
      val ingest: Long => Source[(Offset, Update), NotUsed] => Source[Unit, NotUsed] =
        initialSeqId =>
          source =>
            BatchingParallelIngestionPipe(
              submissionBatchSize = submissionBatchSize,
              batchWithinMillis = batchWithinMillis,
              inputMappingParallelism = inputMappingParallelism,
              inputMapper =
                inputMapperExecutor.execute((input: Iterable[((Offset, Update), Long)]) =>
                  withMetrics(mappingCPU) {
                    val (data, started) = input.unzip
                    val averageStartTime = started.view.map(_ / input.size).sum
                    batchCounter.add(input.size.toLong)
                    batchCounterMetric.add(input.size.toLong)
                    RunningDBBatch.inputMapper(
                      UpdateToDBDTOV1(
                        participantId = participantId,
                        translation = translation,
                        compressionStrategy = compressionStrategy,
                      ),
                      averageStartTime,
                    )(data)
                  }
                ),
              seqMapperZero = RunningDBBatch.seqMapperZero(initialSeqId),
              seqMapper = (prev: RunningDBBatch, curr: RunningDBBatch) =>
                withMetrics(seqMappingCPU) {
                  val nowNanos = System.nanoTime()
                  metrics.daml.pocIndexer.inputMappingStageDuration.update(
                    (nowNanos - curr.averageStartTime) / curr.batchSize,
                    TimeUnit.NANOSECONDS,
                  )
                  RunningDBBatch.seqMapper(prev, curr).copy(averageStartTime = nowNanos)
                },
              ingestingParallelism = ingestionParallelism,
              ingester = postgresDaoPool.execute[RunningDBBatch, RunningDBBatch](
                (batch: RunningDBBatch, dao: PostgresDAO) =>
                  withMetrics(ingestionCPU, dbCallHistrogram) {
                    dao.insertBatch(batch.batch)
                    metrics.daml.pocIndexer.indexerSubmissionThroughput.inc(batch.batchSize.toLong)
                    batch
                  }
              ),
              tailer = (prev: RunningDBBatch, curr: RunningDBBatch) => {
                val nowNanos = System.nanoTime()
                metrics.daml.pocIndexer.ingestionStageDuration.update(
                  (nowNanos - curr.averageStartTime) / curr.batchSize,
                  TimeUnit.NANOSECONDS,
                )
                RunningDBBatch.tailer(prev, curr)
              },
              tailingRateLimitPerSecond = tailingRateLimitPerSecond,
              ingestTail = postgresDaoPool.execute[RunningDBBatch, RunningDBBatch](
                (batch: RunningDBBatch, dao: PostgresDAO) => {
                  dao.updateParams(
                    ledgerEnd = batch.lastOffset,
                    eventSeqId = batch.lastSeqEventId,
                    configuration = batch.lastConfig,
                  )
                  batch
                }
              ),
              runStageUntil = runStageUntil,
            )(
              instrumentedBufferedSource(
                original = source,
                counter = metrics.daml.pocIndexer.indexerInputBufferLength,
                size = 200,
              ).map(_ -> System.nanoTime())
            ).map(_ => ())

      def subscribe(readService: ReadService): Future[Source[Unit, NotUsed]] =
        postgresDaoPool
          .execute(_.initialize)
          .map { case (offset, eventSeqId) =>
            ingest(eventSeqId.getOrElse(0L))(readService.stateUpdates(beginAfter = offset))
          }(scala.concurrent.ExecutionContext.global)

      indexerFluffAround(subscribe)(mat)
    }
  }

  def indexerFluffAround(
      ingestionPipeOn: ReadService => Future[Source[Unit, NotUsed]]
  )(implicit mat: Materializer): Indexer =
    readService =>
      ResourceOwner { implicit context =>
        implicit val ec: ExecutionContext = context.executionContext
        Resource {
          ingestionPipeOn(readService).map { pipe =>
            val (killSwitch, completionFuture) = pipe
              .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
              .toMat(Sink.ignore)(Keep.both)
              .run()
            new SubscriptionIndexFeedHandle(killSwitch, completionFuture.map(_ => ()))
          }
        } { handle =>
          handle.killSwitch.shutdown()
          handle.completed.recover { case NonFatal(_) =>
            ()
          }
        }
      }
}
