// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.sql.Connection
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.participant.state.v1.{Offset, ParticipantId, ReadService, Update}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.metrics.{InstrumentedSource, Metrics}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.indexer.{IndexFeedHandle, Indexer}
import com.daml.platform.store.{DbType, backend}
import com.daml.platform.store.appendonlydao.{DbDispatcher, JdbcLedgerDao}
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.{DBDTOV1, StorageBackend}
import com.daml.resources

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object ParallelIndexerFactory {

  def apply[DB_BATCH](
      jdbcUrl: String,
      storageBackend: StorageBackend[DB_BATCH],
      participantId: ParticipantId,
      translation: LfValueTranslation,
      compressionStrategy: CompressionStrategy,
      mat: Materializer,
      inputMappingParallelism: Int,
      batchingParallelism: Int,
      ingestionParallelism: Int,
      submissionBatchSize: Long,
      tailingRateLimitPerSecond: Int,
      batchWithinMillis: Long,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[Indexer] = {
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        "input-mapping-pool",
        Some(metrics.daml.parallelIndexer.inputMapping.executor -> metrics.registry),
      )
      batcherExecutor <- asyncPool(
        batchingParallelism,
        "batching-pool",
        Some(metrics.daml.parallelIndexer.batching.executor -> metrics.registry),
      )
      dbDispatcher <- DbDispatcher
        .owner( // TODO append-only: do we need to wire health status here somehow?
          serverRole = ServerRole.Indexer,
          jdbcUrl = jdbcUrl,
          connectionPoolSize = ingestionParallelism + 1, // + 1 for the tailing ledger_end updates
          connectionTimeout = FiniteDuration(
            250,
            "millis",
          ), // TODO append-only: verify why would we need here a timeout, in this case (amount of parallelism aligned) it should not happen by design
          metrics = metrics,
          connectionAsyncCommitMode = DbType.AsynchronousCommit,
        )
      toDbDto = backend.UpdateToDBDTOV1(
        participantId = participantId,
        translation = translation,
        compressionStrategy = compressionStrategy,
      )
    } yield {
      val ingest: Long => Source[(Offset, Update), NotUsed] => Source[Unit, NotUsed] =
        initialSeqId =>
          source =>
            BatchingParallelIngestionPipe(
              submissionBatchSize = submissionBatchSize,
              batchWithinMillis = batchWithinMillis,
              inputMappingParallelism = inputMappingParallelism,
              inputMapper = inputMapperExecutor.execute(inputMapper(metrics, toDbDto)),
              seqMapperZero = seqMapperZero(initialSeqId),
              seqMapper = seqMapper(metrics),
              batchingParallelism = batchingParallelism,
              batcher = batcherExecutor.execute(batcher(storageBackend.batch, metrics)),
              ingestingParallelism = ingestionParallelism,
              ingester = ingester(storageBackend.insertBatch, dbDispatcher, metrics),
              tailer = tailer(storageBackend.batch(Vector.empty)),
              tailingRateLimitPerSecond = tailingRateLimitPerSecond,
              ingestTail = ingestTail[DB_BATCH](storageBackend.updateParams, dbDispatcher, metrics),
            )(
              InstrumentedSource
                .bufferedSource(
                  original = source,
                  counter = metrics.daml.parallelIndexer.inputBufferLength,
                  size = 200, // TODO append-only: maybe make it configurable
                )
                .map(_ -> System.nanoTime())
            ).map(_ => ())

      def subscribe(readService: ReadService): Future[Source[Unit, NotUsed]] =
        dbDispatcher
          .executeSql(metrics.daml.parallelIndexer.initialization)(storageBackend.initialize)
          .map(initialized =>
            ingest(initialized.lastEventSeqId.getOrElse(0L))(
              readService.stateUpdates(beginAfter = initialized.lastOffset)
            )
          )(scala.concurrent.ExecutionContext.global)

      toIndexer(subscribe)(mat)
    }
  }

  /** Batch wraps around a T-typed batch, enriching it with processing relevant information.
    *
    * @param lastOffset The latest offset available in the batch. Needed for tail ingestion.
    * @param lastSeqEventId The latest sequential-event-id in the batch, or if none present there, then the latest from before. Needed for tail ingestion.
    * @param lastConfig The latest successful accepted configuration in the batch, if available.
    * @param batch The batch of variable type.
    * @param batchSize Size of the batch measured in number of updates. Needed for metrics population.
    * @param averageStartTime The nanosecond timestamp of the start of the previous processing stage. Needed for metrics population: how much time is spend by a particular update in a certain stage.
    */
  case class Batch[+T](
      lastOffset: Offset,
      lastSeqEventId: Long,
      lastConfig: Option[Array[Byte]],
      batch: T,
      batchSize: Int,
      averageStartTime: Long,
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => Update => Iterator[DBDTOV1],
  ): Iterable[((Offset, Update), Long)] => Batch[Vector[DBDTOV1]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)
    val batch = input.iterator.flatMap { case ((offset, update), _) =>
      toDbDto(offset)(update)
    }.toVector
    Batch(
      lastOffset = input.last._1._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastConfig = batch.reverseIterator.collectFirst {
        case c: DBDTOV1.ConfigurationEntry if c.typ == JdbcLedgerDao.acceptType => c.configuration
      },
      batch = batch,
      batchSize = input.size,
      averageStartTime = input.view.map(_._2 / input.size).sum,
    )
  }

  def seqMapperZero(initialSeqId: Long): Batch[Vector[DBDTOV1]] =
    Batch(
      lastOffset = null,
      lastSeqEventId = initialSeqId, // this is the only property of interest in the zero element
      lastConfig = None,
      batch = Vector.empty,
      batchSize = 0,
      averageStartTime = 0,
    )

  def seqMapper(metrics: Metrics)(
      previous: Batch[Vector[DBDTOV1]],
      current: Batch[Vector[DBDTOV1]],
  ): Batch[Vector[DBDTOV1]] = {
    var eventSeqId = previous.lastSeqEventId
    val batchWithSeqIds = current.batch.map {
      case dbDto: DBDTOV1.EventCreate =>
        eventSeqId += 1
        dbDto.copy(event_sequential_id = eventSeqId)

      case dbDto: DBDTOV1.EventExercise =>
        eventSeqId += 1
        dbDto.copy(event_sequential_id = eventSeqId)

      case dbDto: DBDTOV1.EventDivulgence =>
        eventSeqId += 1
        dbDto.copy(event_sequential_id = eventSeqId)

      case notEvent => notEvent
    }
    val nowNanos = System.nanoTime()
    metrics.daml.parallelIndexer.inputMapping.duration.update(
      (nowNanos - current.averageStartTime) / current.batchSize,
      TimeUnit.NANOSECONDS,
    )
    current.copy(
      lastSeqEventId = eventSeqId,
      batch = batchWithSeqIds,
      averageStartTime = nowNanos, // setting start time to the start of the next stage
    )
  }

  def batcher[DB_BATCH](
      batchF: Vector[DBDTOV1] => DB_BATCH,
      metrics: Metrics,
  ): Batch[Vector[DBDTOV1]] => Batch[DB_BATCH] = { inBatch =>
    val dbBatch = batchF(inBatch.batch)
    val nowNanos = System.nanoTime()
    metrics.daml.parallelIndexer.batching.duration.update(
      (nowNanos - inBatch.averageStartTime) / inBatch.batchSize,
      TimeUnit.NANOSECONDS,
    )
    inBatch.copy(
      batch = dbBatch,
      averageStartTime = nowNanos,
    )
  }

  def ingester[DB_BATCH](
      ingestFunction: (Connection, DB_BATCH) => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      dbDispatcher.executeSql(metrics.daml.parallelIndexer.ingestion) { connection =>
        metrics.daml.parallelIndexer.updates.inc(batch.batchSize.toLong)
        ingestFunction(connection, batch.batch)
        val nowNanos = System.nanoTime()
        metrics.daml.parallelIndexer.ingestion.duration.update(
          (nowNanos - batch.averageStartTime) / batch.batchSize,
          TimeUnit.NANOSECONDS,
        )
        batch
      }

  def tailer[DB_BATCH](
      zeroDbBatch: DB_BATCH
  ): (Batch[DB_BATCH], Batch[DB_BATCH]) => Batch[DB_BATCH] =
    (prev, curr) =>
      Batch[DB_BATCH](
        lastOffset = curr.lastOffset,
        lastSeqEventId = curr.lastSeqEventId,
        lastConfig = curr.lastConfig.orElse(prev.lastConfig),
        batch = zeroDbBatch, // not used anymore
        batchSize = 0, // not used anymore
        averageStartTime = 0, // not used anymore
      )

  def ingestTail[DB_BATCH](
      ingestTailFunction: (Connection, StorageBackend.Params) => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
        ingestTailFunction(
          connection,
          StorageBackend.Params(
            ledgerEnd = batch.lastOffset,
            eventSeqId = batch.lastSeqEventId,
            configuration = batch.lastConfig,
          ),
        )
        metrics.daml.indexer.ledgerEndSequentialId.updateValue(batch.lastSeqEventId)
        batch
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
