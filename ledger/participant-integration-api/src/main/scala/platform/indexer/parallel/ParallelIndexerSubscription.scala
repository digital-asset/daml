// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.metrics.InstrumentedGraph._
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.index.InMemoryStateUpdater
import com.daml.platform.indexer.ha.Handle
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend._
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.interning.{InternizingStringInterningView, StringInterning}
import java.sql.Connection
import scala.util.chaining._

import com.daml.metrics.api.MetricsContext

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
    maxOutputBatchedBufferSize: Int,
    maxTailerBatchSize: Int,
    metrics: Metrics,
    inMemoryStateUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
    stringInterningView: StringInterning with InternizingStringInterningView,
) {
  import ParallelIndexerSubscription._
  private implicit val metricsContext: MetricsContext = MetricsContext(
    "participant_id" -> participantId
  )
  def apply(
      inputMapperExecutor: Executor,
      batcherExecutor: Executor,
      dbDispatcher: DbDispatcher,
      materializer: Materializer,
  )(implicit loggingContext: LoggingContext): InitializeParallelIngestion.Initialized => Handle = {
    initialized =>
      val (killSwitch, completionFuture) = BatchingParallelIngestionPipe(
        submissionBatchSize = submissionBatchSize,
        inputMappingParallelism = inputMappingParallelism,
        inputMapper = inputMapperExecutor.execute(
          inputMapper(
            metrics,
            UpdateToDbDto(
              participantId = participantId,
              translation = translation,
              compressionStrategy = compressionStrategy,
              metrics,
            ),
            UpdateToMeteringDbDto(metrics = metrics.daml.indexerEvents),
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
        ingester = ingester(
          ingestFunction = ingestionStorageBackend.insertBatch,
          zeroDbBatch = ingestionStorageBackend.batch(Vector.empty, stringInterningView),
          dbDispatcher = dbDispatcher,
          metrics = metrics,
        ),
        maxTailerBatchSize = maxTailerBatchSize,
        ingestTail =
          ingestTail[DB_BATCH](parameterStorageBackend.updateLedgerEnd, dbDispatcher, metrics),
      )(
        initialized.readServiceSource
          .buffered(metrics.daml.parallelIndexer.inputBufferLength, maxInputBufferSize)
      )
        .map(batch => batch.offsetsUpdates -> batch.lastSeqEventId)
        .buffered(
          counter = metrics.daml.parallelIndexer.outputBatchedBufferLength,
          size = maxOutputBatchedBufferSize,
        )
        .via(inMemoryStateUpdaterFlow)
        .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
        .toMat(Sink.ignore)(Keep.both)
        .run()(materializer)
      Handle(completionFuture.map(_ => ())(materializer.executionContext), killSwitch)
  }
}

object ParallelIndexerSubscription {

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
      offsetsUpdates: Vector[(Offset, state.Update)],
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => state.Update => Iterator[DbDto],
      toMeteringDbDto: Iterable[(Offset, state.Update)] => Vector[DbDto.TransactionMetering],
  )(implicit
      loggingContext: LoggingContext
  ): Iterable[(Offset, state.Update)] => Batch[Vector[DbDto]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)(MetricsContext.Empty)
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

    Batch(
      lastOffset = input.last._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastStringInterningId = 0, // will be filled later in the sequential step
      lastRecordTime = input.last._2.recordTime.toInstant.toEpochMilli,
      batch = batch,
      batchSize = input.size,
      offsetsUpdates = input.toVector,
    )
  }

  def seqMapperZero(
      initialSeqId: Long,
      initialStringInterningId: Int,
  ): Batch[Vector[DbDto]] =
    Batch(
      lastOffset = null,
      lastSeqEventId = initialSeqId, // this is property of interest in the zero element
      lastStringInterningId =
        initialStringInterningId, // this is property of interest in the zero element
      lastRecordTime = 0,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )

  def seqMapper(
      internize: Iterable[DbDto] => Iterable[(Int, String)],
      metrics: Metrics,
  )(
      previous: Batch[Vector[DbDto]],
      current: Batch[Vector[DbDto]],
  ): Batch[Vector[DbDto]] = {
    Timed.value(
      metrics.daml.parallelIndexer.seqMapping.duration, {
        var eventSeqId = previous.lastSeqEventId
        var lastTransactionMetaEventSeqId = eventSeqId
        val batchWithSeqIds = current.batch.map {
          case dbDto: DbDto.EventCreate =>
            eventSeqId += 1
            dbDto.copy(event_sequential_id = eventSeqId)

          case dbDto: DbDto.EventExercise =>
            eventSeqId += 1
            dbDto.copy(event_sequential_id = eventSeqId)

          case dbDto: DbDto.EventDivulgence =>
            eventSeqId += 1
            dbDto.copy(event_sequential_id = eventSeqId)

          case dbDto: DbDto.IdFilterCreateStakeholder =>
            // we do not increase the event_seq_id here, because all the IdFilterCreateStakeholder DbDto-s must have the same eventSeqId as the preceding EventCreate
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterCreateNonStakeholderInformee =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterConsumingStakeholder =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterConsumingNonStakeholderInformee =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterNonConsumingInformee =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.TransactionMeta =>
            dbDto
              .copy(
                event_sequential_id_first = lastTransactionMetaEventSeqId + 1,
                event_sequential_id_last = eventSeqId,
              )
              .tap(_ => lastTransactionMetaEventSeqId = eventSeqId)
          case unChanged => unChanged
        }

        val (newLastStringInterningId, dbDtosWithStringInterning) =
          internize(batchWithSeqIds)
            .map(DbDto.StringInterningDto.from) match {
            case noNewEntries if noNewEntries.isEmpty =>
              previous.lastStringInterningId -> batchWithSeqIds
            case newEntries => newEntries.last.internalId -> (batchWithSeqIds ++ newEntries)
          }

        current.copy(
          lastSeqEventId = eventSeqId,
          lastStringInterningId = newLastStringInterningId,
          batch = dbDtosWithStringInterning,
        )
      },
    )
  }

  def batcher[DB_BATCH](
      batchF: Vector[DbDto] => DB_BATCH
  ): Batch[Vector[DbDto]] => Batch[DB_BATCH] = { inBatch =>
    val dbBatch = batchF(inBatch.batch)
    inBatch.copy(
      batch = dbBatch
    )
  }

  def ingester[DB_BATCH](
      ingestFunction: (Connection, DB_BATCH) => Unit,
      zeroDbBatch: DB_BATCH,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      withEnrichedLoggingContext("updateOffsets" -> batch.offsetsUpdates.map(_._1)) {
        implicit loggingContext =>
          dbDispatcher.executeSql(metrics.daml.parallelIndexer.ingestion) { connection =>
            metrics.daml.parallelIndexer.updates.inc(batch.batchSize.toLong)(MetricsContext.Empty)
            ingestFunction(connection, batch.batch)
            cleanUnusedBatch(zeroDbBatch)(batch)
          }
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
  ): Vector[Batch[DB_BATCH]] => Future[Vector[Batch[DB_BATCH]]] = batchOfBatches =>
    batchOfBatches.lastOption match {
      case Some(lastBatch) =>
        withEnrichedLoggingContext("updateOffset" -> lastBatch.lastOffset) {
          implicit loggingContext =>
            dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
              ingestTailFunction(ledgerEndFrom(lastBatch))(connection)
              metrics.daml.indexer.ledgerEndSequentialId
                .updateValue(lastBatch.lastSeqEventId)
              metrics.daml.indexer.lastReceivedRecordTime
                .updateValue(lastBatch.lastRecordTime)
              metrics.daml.indexer.lastReceivedOffset
                .updateValue(lastBatch.lastOffset.toHexString)
              logger.info("Ledger end updated in IndexDB")
              batchOfBatches
            }
        }
      case None =>
        val message = "Unexpectedly encountered a zero-sized batch in ingestTail"
        logger.error(message)
        Future.failed(new IllegalStateException(message))
    }

  private def cleanUnusedBatch[DB_BATCH](
      zeroDbBatch: DB_BATCH
  ): Batch[DB_BATCH] => Batch[DB_BATCH] =
    _.copy(
      batch = zeroDbBatch, // not used anymore
      batchSize = 0, // not used anymore
    )
}
