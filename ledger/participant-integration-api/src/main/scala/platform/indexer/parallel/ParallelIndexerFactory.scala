// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer, UniqueKillSwitch}
import com.codahale.metrics.Gauge
import com.daml.ledger.participant.state.v1.{Offset, ParticipantId, ReadService, Update}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.Metrics
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.indexer.parallel.PerfSupport._
import com.daml.platform.indexer.{IndexFeedHandle, Indexer}
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.resources

import scala.concurrent.Future
import scala.util.control.NonFatal

object ParallelIndexerFactory {

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
      metrics: Metrics,
  ): ResourceOwner[Indexer] = {
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        Some(metrics.daml.parallelIndexer.inputMappingExecutor -> metrics.registry),
      )
      postgresDaoPool <- asyncResourcePool(
        () => JDBCPostgresDAO(jdbcUrl),
        size = ingestionParallelism + 1,
        Some(metrics.daml.parallelIndexer.ingestionExecutor -> metrics.registry),
      )
    } yield {
      // TODO move this custom gauge implementation to metrics module, and remove hack for instantiation
      val batchCounterMetric = AverageCounter()
      val batchCounterGauge: Gauge[Int] = () =>
        batchCounterMetric.retrieveAverage.getOrElse(0L).toInt
      metrics.registry.remove(
        metrics.daml.parallelIndexer.batchSize
      ) // to allow to run this code multiple times in one JVM
      metrics.registry.register(metrics.daml.parallelIndexer.batchSize, batchCounterGauge)
      val ingest: Long => Source[(Offset, Update), NotUsed] => Source[Unit, NotUsed] =
        initialSeqId =>
          source =>
            BatchingParallelIngestionPipe(
              submissionBatchSize = submissionBatchSize,
              batchWithinMillis = batchWithinMillis,
              inputMappingParallelism = inputMappingParallelism,
              inputMapper =
                inputMapperExecutor.execute((input: Iterable[((Offset, Update), Long)]) => {
                  val (data, started) = input.unzip
                  val averageStartTime = started.view.map(_ / input.size).sum
                  batchCounterMetric.add(input.size.toLong)
                  RunningDBBatch.inputMapper(
                    UpdateToDBDTOV1(
                      participantId = participantId,
                      translation = translation,
                      compressionStrategy = compressionStrategy,
                    ),
                    averageStartTime,
                  )(data)
                }),
              seqMapperZero = RunningDBBatch.seqMapperZero(initialSeqId),
              seqMapper = (prev: RunningDBBatch, curr: RunningDBBatch) => {
                val nowNanos = System.nanoTime()
                metrics.daml.parallelIndexer.inputMappingStageDuration.update(
                  (nowNanos - curr.averageStartTime) / curr.batchSize,
                  TimeUnit.NANOSECONDS,
                )
                RunningDBBatch.seqMapper(prev, curr).copy(averageStartTime = nowNanos)
              },
              ingestingParallelism = ingestionParallelism,
              ingester = postgresDaoPool.execute[RunningDBBatch, RunningDBBatch](
                (batch: RunningDBBatch, dao: PostgresDAO) => {
                  dao.insertBatch(batch.batch)
                  metrics.daml.parallelIndexer.indexerSubmissionThroughput
                    .inc(batch.batchSize.toLong)
                  batch
                }
              ),
              tailer = (prev: RunningDBBatch, curr: RunningDBBatch) => {
                val nowNanos = System.nanoTime()
                metrics.daml.parallelIndexer.ingestionStageDuration.update(
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
            )(
              instrumentedBufferedSource(
                original = source,
                counter = metrics.daml.parallelIndexer.indexerInputBufferLength,
                size = 200, // TODO maybe make it configurable
              ).map(_ -> System.nanoTime())
            ).map(_ => ())

      def subscribe(readService: ReadService): Future[Source[Unit, NotUsed]] =
        postgresDaoPool
          .execute(_.initialize)
          .map { case (offset, eventSeqId) =>
            ingest(eventSeqId.getOrElse(0L))(readService.stateUpdates(beginAfter = offset))
          }(scala.concurrent.ExecutionContext.global)

      toIndexer(subscribe)(mat)
    }
  }

  def toIndexer(
      ingestionPipeOn: ReadService => Future[Source[Unit, NotUsed]]
  )(implicit mat: Materializer): Indexer =
    readService =>
      new ResourceOwner[IndexFeedHandle] {
        override def acquire()(implicit
            context: ResourceContext
        ): resources.Resource[ResourceContext, IndexFeedHandle] = {
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

  class SubscriptionIndexFeedHandle(
      val killSwitch: KillSwitch,
      override val completed: Future[Unit],
  ) extends IndexFeedHandle
}
