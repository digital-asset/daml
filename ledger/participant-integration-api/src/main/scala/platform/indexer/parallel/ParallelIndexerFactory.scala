// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer, UniqueKillSwitch}
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

  // TODO append-only: migrate code for mutable initialisation
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
        Some(metrics.daml.parallelIndexer.inputMapping.executor -> metrics.registry),
      )
      postgresDaoPool <- asyncResourcePool(
        () => JDBCPostgresDAO(jdbcUrl),
        size = ingestionParallelism + 1,
        Some(metrics.daml.parallelIndexer.ingestion.executor -> metrics.registry),
      )
    } yield {
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
                  metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)
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
                metrics.daml.parallelIndexer.inputMapping.duration.update(
                  (nowNanos - curr.averageStartTime) / curr.batchSize,
                  TimeUnit.NANOSECONDS,
                )
                RunningDBBatch.seqMapper(prev, curr).copy(averageStartTime = nowNanos)
              },
              ingestingParallelism = ingestionParallelism,
              ingester = postgresDaoPool.execute[RunningDBBatch, RunningDBBatch](
                (batch: RunningDBBatch, dao: PostgresDAO) => {
                  dao.insertBatch(batch.batch)
                  metrics.daml.parallelIndexer.updates.inc(batch.batchSize.toLong)
                  batch
                }
              ),
              tailer = (prev: RunningDBBatch, curr: RunningDBBatch) => {
                val nowNanos = System.nanoTime()
                metrics.daml.parallelIndexer.ingestion.duration.update(
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
                counter = metrics.daml.parallelIndexer.inputBufferLength,
                size = 200, // TODO append-only: maybe make it configurable
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
