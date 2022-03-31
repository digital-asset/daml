// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.indexer.ha.Handle
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend._
import com.daml.platform.store.interning.{InternizingStringInterningView, StringInterning}

import java.sql.Connection
import scala.concurrent.Future

private[platform] case class ParallelIndexerSubscription[DB_BATCH](
    ingestionStorageBackend: IngestionStorageBackend[DB_BATCH],
    parameterStorageBackend: ParameterStorageBackend,
    participantId: Ref.ParticipantId,
    translation: LfValueTranslation,
    compressionStrategy: CompressionStrategy,
    maxInputBufferSize: Int,
    inputMappingParallelism: Int,
    batchingParallelism: Int,
    ingestionParallelism: Int,
    submissionBatchSize: Long,
    batchWithinMillis: Long,
    metrics: Metrics,
) {
  import ParallelIndexerSubscription._

  def apply(
      inputMapperExecutor: Executor,
      batcherExecutor: Executor,
      dbDispatcher: DbDispatcher,
      stringInterningView: StringInterning with InternizingStringInterningView,
      indexedUpdatesConsumer: Sink[(Offset, state.Update), NotUsed],
      materializer: Materializer,
  )(implicit loggingContext: LoggingContext): InitializeParallelIngestion.Initialized => Handle = {
    initialized =>
      val (killSwitch, completionFuture) = BatchingParallelIngestionPipe(
        submissionBatchSize = submissionBatchSize,
        batchWithinMillis = batchWithinMillis,
        inputMappingParallelism = inputMappingParallelism,
        inputMapper = inputMapperExecutor.execute(
          inputMapper(
            metrics,
            UpdateToDbDto(
              participantId = participantId,
              translation = translation,
              compressionStrategy = compressionStrategy,
            ),
            UpdateToMeteringDbDto(),
          )
        ),
        seqMapperZero =
          seqMapperZero(initialized.initialEventSeqId, initialized.initialStringInterningId),
        seqMapper = seqMapper(
          dtos => stringInterningView.internize(DbDtoToStringsForInterning(dtos)),
          metrics,
        ),
        batchingParallelism = batchingParallelism,
        batcher = batcherExecutor.execute(
          batcher(ingestionStorageBackend.batch(_, stringInterningView))
        ),
        ingestingParallelism = ingestionParallelism,
        ingester = ingester(ingestionStorageBackend.insertBatch, dbDispatcher, metrics),
        tailer = tailer(ingestionStorageBackend.batch(Vector.empty, stringInterningView)),
        ingestTail =
          ingestTail[DB_BATCH](parameterStorageBackend.updateLedgerEnd, dbDispatcher, metrics),
      )(
        InstrumentedSource
          .bufferedSource(
            original = initialized.readServiceSource,
            counter = metrics.daml.parallelIndexer.inputBufferLength,
            size = maxInputBufferSize,
          )
      )
        .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
        .watchTermination()(Keep.both)
        .toMat(indexedUpdatesConsumer)(Keep.left)
        .run()(materializer)
      Handle(completionFuture.map(_ => ())(materializer.executionContext), killSwitch)
  }
}

object ParallelIndexerSubscription {
  type WithUpdatesBatch[T] = (Iterable[(Offset, state.Update)], T)

  private val logger = ContextualizedLogger.get(this.getClass)

  /** Batch wraps around a T-typed batch, enriching it with processing relevant information.
    *
    * @param lastOffset The latest offset available in the batch. Needed for tail ingestion.
    * @param lastSeqEventId The latest sequential-event-id in the batch, or if none present there, then the latest from before. Needed for tail ingestion.
    * @param lastStringInterningId The latest string interning id in the batch, or if none present there, then the latest from before. Needed for tail ingestion.
    * @param lastRecordTime The latest record time in the batch, in milliseconds since Epoch. Needed for metrics population.
    * @param batch The batch of variable type.
    * @param batchSize Size of the batch measured in number of updates. Needed for metrics population.
    */
  case class Batch[+T](
      lastOffset: Offset,
      lastSeqEventId: Long,
      lastStringInterningId: Int,
      lastRecordTime: Long,
      batch: T,
      batchSize: Int,
      offsets: Vector[Offset],
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => state.Update => Iterator[DbDto],
      toMeteringDbDto: Iterable[(Offset, state.Update)] => Vector[DbDto.TransactionMetering],
  )(implicit
      loggingContext: LoggingContext
  ): Iterable[(Offset, state.Update)] => WithUpdatesBatch[Batch[Vector[DbDto]]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)
    input.foreach { case (offset, update) =>
      withEnrichedLoggingContext("offset" -> offset, "update" -> update) {
        implicit loggingContext =>
          logger.info(s"Storing ${update.description}")
      }
    }

    val mainBatch = input.iterator.flatMap { case (offset, update) =>
      toDbDto(offset)(update)
    }.toVector

    val meteringBatch = toMeteringDbDto(input)

    val batch = mainBatch ++ meteringBatch

    input -> Batch(
      lastOffset = input.last._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastStringInterningId = 0, // will be filled later in the sequential step
      lastRecordTime = input.last._2.recordTime.toInstant.toEpochMilli,
      batch = batch,
      batchSize = input.size,
      offsets = input.view.map(_._1).toVector,
    )
  }

  def seqMapperZero(
      initialSeqId: Long,
      initialStringInterningId: Int,
  ): WithUpdatesBatch[Batch[Vector[DbDto]]] =
    Iterable((null, null)) -> Batch(
      lastOffset = null,
      lastSeqEventId = initialSeqId, // this is property of interest in the zero element
      lastStringInterningId =
        initialStringInterningId, // this is property of interest in the zero element
      lastRecordTime = 0,
      batch = Vector.empty,
      batchSize = 0,
      offsets = Vector.empty,
    )

  def seqMapper(
      internize: Iterable[DbDto] => Iterable[(Int, String)],
      metrics: Metrics,
  )(
      previous: WithUpdatesBatch[Batch[Vector[DbDto]]],
      current: WithUpdatesBatch[Batch[Vector[DbDto]]],
  ): WithUpdatesBatch[Batch[Vector[DbDto]]] = {
    Timed.value(
      metrics.daml.parallelIndexer.seqMapping.duration, {
        var eventSeqId = previous._2.lastSeqEventId
        val batchWithSeqIds = current._2.batch
          .map {
            case dbDto: DbDto.EventCreate =>
              eventSeqId += 1
              dbDto.copy(event_sequential_id = eventSeqId)

            case dbDto: DbDto.EventExercise =>
              eventSeqId += 1
              dbDto.copy(event_sequential_id = eventSeqId)

            case dbDto: DbDto.EventDivulgence =>
              eventSeqId += 1
              dbDto.copy(event_sequential_id = eventSeqId)

            case dbDto: DbDto.CreateFilter =>
              // we do not increase the event_seq_id here, because all the CreateFilter DbDto-s must have the same eventSeqId as the preceding EventCreate
              dbDto.copy(event_sequential_id = eventSeqId)

            case unChanged => unChanged
          }

        val (newLastStringInterningId, dbDtosWithStringInterning) =
          internize(batchWithSeqIds)
            .map(DbDto.StringInterningDto.from) match {
            case noNewEntries if noNewEntries.isEmpty =>
              previous._2.lastStringInterningId -> batchWithSeqIds
            case newEntries => newEntries.last.internalId -> (batchWithSeqIds ++ newEntries)
          }

        current._1 -> current._2.copy(
          lastSeqEventId = eventSeqId,
          lastStringInterningId = newLastStringInterningId,
          batch = dbDtosWithStringInterning,
        )
      },
    )
  }

  def batcher[DB_BATCH](
      batchF: Vector[DbDto] => DB_BATCH
  ): WithUpdatesBatch[Batch[Vector[DbDto]]] => WithUpdatesBatch[Batch[DB_BATCH]] = { inBatch =>
    val dbBatch = batchF(inBatch._2.batch)
    inBatch.copy(_2 =
      inBatch._2.copy(
        batch = dbBatch
      )
    )
  }

  def ingester[DB_BATCH](
      ingestFunction: (Connection, DB_BATCH) => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit
      loggingContext: LoggingContext
  ): WithUpdatesBatch[Batch[DB_BATCH]] => Future[WithUpdatesBatch[Batch[DB_BATCH]]] =
    batch =>
      withEnrichedLoggingContext("updateOffsets" -> batch._2.offsets) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.parallelIndexer.ingestion) { connection =>
          metrics.daml.parallelIndexer.updates.inc(batch._2.batchSize.toLong)
          ingestFunction(connection, batch._2.batch)
          batch
        }
      }

  def tailer[DB_BATCH](
      zeroDbBatch: DB_BATCH
  ): (WithUpdatesBatch[Batch[DB_BATCH]], WithUpdatesBatch[Batch[DB_BATCH]]) => WithUpdatesBatch[
    Batch[DB_BATCH]
  ] = { case ((prevUpdates, _), (currUpdates, currentBatch)) =>
    prevUpdates ++ currUpdates -> Batch[DB_BATCH](
      lastOffset = currentBatch.lastOffset,
      lastSeqEventId = currentBatch.lastSeqEventId,
      lastStringInterningId = currentBatch.lastStringInterningId,
      lastRecordTime = currentBatch.lastRecordTime,
      batch = zeroDbBatch, // not used anymore
      batchSize = 0, // not used anymore
      offsets = Vector.empty, // not used anymore
    )
  }

  def ledgerEndFrom(batch: Batch[_]): LedgerEnd =
    LedgerEnd(
      lastOffset = batch.lastOffset,
      lastEventSeqId = batch.lastSeqEventId,
      lastStringInterningId = batch.lastStringInterningId,
    )

  def ingestTail[DB_BATCH](
      ingestTailFunction: LedgerEnd => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit
      loggingContext: LoggingContext
  ): WithUpdatesBatch[Batch[DB_BATCH]] => Future[Iterable[(Offset, state.Update)]] =
    batch =>
      withEnrichedLoggingContext("updateOffset" -> batch._2.lastOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
          ingestTailFunction(ledgerEndFrom(batch._2))(connection)
          metrics.daml.indexer.ledgerEndSequentialId.updateValue(batch._2.lastSeqEventId)
          metrics.daml.indexer.lastReceivedRecordTime.updateValue(batch._2.lastRecordTime)
          metrics.daml.indexer.lastReceivedOffset.updateValue(batch._2.lastOffset.toHexString)
          logger.info("Ledger end updated")
          batch._1
        }
      }
}
