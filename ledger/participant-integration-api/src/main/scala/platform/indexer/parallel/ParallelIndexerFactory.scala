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
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.indexer.{IndexFeedHandle, Indexer}
import com.daml.platform.store.{DbType, backend}
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.{DBDTOV1, StorageBackend}
import com.daml.resources

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object ParallelIndexerFactory {

  private val logger = ContextualizedLogger.get(this.getClass)

  def apply[DB_BATCH](
      jdbcUrl: String,
      storageBackend: StorageBackend[DB_BATCH],
      participantId: ParticipantId,
      translation: LfValueTranslation,
      compressionStrategy: CompressionStrategy,
      mat: Materializer,
      maxInputBufferSize: Int,
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
        .owner(
          serverRole = ServerRole.Indexer,
          jdbcUrl = jdbcUrl,
          connectionPoolSize = ingestionParallelism + 1, // + 1 for the tailing ledger_end updates
          connectionTimeout = FiniteDuration(
            250,
            "millis",
          ), // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
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
                  size = maxInputBufferSize,
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
    * @param batch The batch of variable type.
    * @param batchSize Size of the batch measured in number of updates. Needed for metrics population.
    * @param averageStartTime The nanosecond timestamp of the start of the previous processing stage. Needed for metrics population: how much time is spend by a particular update in a certain stage.
    */
  case class Batch[+T](
      lastOffset: Offset,
      lastSeqEventId: Long,
      batch: T,
      batchSize: Int,
      averageStartTime: Long,
      offsets: Vector[Offset],
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => Update => Iterator[DBDTOV1],
  )(implicit
      loggingContext: LoggingContext
  ): Iterable[((Offset, Update), Long)] => Batch[Vector[DBDTOV1]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)
    input.foreach { case ((offset, update), _) =>
      LoggingContext.withEnrichedLoggingContext(
        IndexerLoggingContext.loggingContextFor(offset, update)
      ) { implicit loggingContext =>
        logger.info("Storing transaction")
      }
    }
    val batch = input.iterator.flatMap { case ((offset, update), _) =>
      toDbDto(offset)(update)
    }.toVector
    Batch(
      lastOffset = input.last._1._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      batch = batch,
      batchSize = input.size,
      averageStartTime = input.view.map(_._2 / input.size).sum,
      offsets = input.view.map(_._1._1).toVector,
    )
  }

  def seqMapperZero(initialSeqId: Long): Batch[Vector[DBDTOV1]] =
    Batch(
      lastOffset = null,
      lastSeqEventId = initialSeqId, // this is the only property of interest in the zero element
      batch = Vector.empty,
      batchSize = 0,
      averageStartTime = 0,
      offsets = Vector.empty,
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
      LoggingContext.withEnrichedLoggingContext(
        "updateOffsets" -> batch.offsets.view.map(_.toHexString).mkString("[", ", ", "]")
      ) { implicit loggingContext =>
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
      }

  def tailer[DB_BATCH](
      zeroDbBatch: DB_BATCH
  ): (Batch[DB_BATCH], Batch[DB_BATCH]) => Batch[DB_BATCH] =
    (_, curr) =>
      Batch[DB_BATCH](
        lastOffset = curr.lastOffset,
        lastSeqEventId = curr.lastSeqEventId,
        batch = zeroDbBatch, // not used anymore
        batchSize = 0, // not used anymore
        averageStartTime = 0, // not used anymore
        offsets = Vector.empty, // not used anymore
      )

  def ingestTail[DB_BATCH](
      ingestTailFunction: (Connection, StorageBackend.Params) => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      LoggingContext.withEnrichedLoggingContext(
        "updateOffset" -> batch.lastOffset.toHexString
      ) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
          ingestTailFunction(
            connection,
            StorageBackend.Params(
              ledgerEnd = batch.lastOffset,
              eventSeqId = batch.lastSeqEventId,
            ),
          )
          metrics.daml.indexer.ledgerEndSequentialId.updateValue(batch.lastSeqEventId)
          logger.info("Ledger end updated")
          batch
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
