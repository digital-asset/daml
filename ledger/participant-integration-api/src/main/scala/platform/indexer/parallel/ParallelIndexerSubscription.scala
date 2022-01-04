// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.ledger.participant.state.v2.Update.TransactionAccepted
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics}
import com.daml.platform.indexer.ha.Handle
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend._
import com.daml.platform.store.interning.{InternizingStringInterningView, StringInterning}

import java.sql.Connection
import java.util.concurrent.TimeUnit
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
    tailingRateLimitPerSecond: Int,
    batchWithinMillis: Long,
    metrics: Metrics,
) {
  import ParallelIndexerSubscription._

  def apply(
      inputMapperExecutor: Executor,
      batcherExecutor: Executor,
      dbDispatcher: DbDispatcher,
      stringInterningView: StringInterning with InternizingStringInterningView,
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
          batcher(ingestionStorageBackend.batch(_, stringInterningView), metrics)
        ),
        ingestingParallelism = ingestionParallelism,
        ingester = ingester(ingestionStorageBackend.insertBatch, dbDispatcher, metrics),
        tailer = tailer(ingestionStorageBackend.batch(Vector.empty, stringInterningView)),
        tailingRateLimitPerSecond = tailingRateLimitPerSecond,
        ingestTail =
          ingestTail[DB_BATCH](parameterStorageBackend.updateLedgerEnd, dbDispatcher, metrics),
      )(
        InstrumentedSource
          .bufferedSource(
            original = initialized.readServiceSource,
            counter = metrics.daml.parallelIndexer.inputBufferLength,
            size = maxInputBufferSize,
          )
          .map(_ -> System.nanoTime())
      )
        .map(_ => ())
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
    * @param averageStartTime The nanosecond timestamp of the start of the previous processing stage. Needed for metrics population: how much time is spend by a particular update in a certain stage.
    */
  case class Batch[+T](
      lastOffset: Offset,
      lastSeqEventId: Long,
      lastStringInterningId: Int,
      lastRecordTime: Long,
      batch: T,
      batchSize: Int,
      averageStartTime: Long,
      offsets: Vector[Offset],
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => state.Update => Iterator[DbDto],
  )(implicit
      loggingContext: LoggingContext
  ): Iterable[((Offset, state.Update), Long)] => Batch[Vector[DbDto]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)
    input.foreach { case ((offset, update), _) =>
      withEnrichedLoggingContext("offset" -> offset, "update" -> update) {
        implicit loggingContext =>
          logger.info(s"Storing ${update.description}")
      }
    }

    val nonMetering = input.iterator.flatMap { case ((offset, update), _) =>
      toDbDto(offset)(update)
    }.toVector

    val metering = input.iterator
      .collect({ case ((offset, ta: TransactionAccepted), time) =>
        (offset, ta.optCompletionInfo, time)
      })
      .collect({
        case (offset, Some(CompletionInfo(_, applicationId, _, _, _, Some(statistics))), time) =>
          (offset, applicationId, statistics, time)
      })
      .foldLeft(Map.empty[Ref.ApplicationId, DbDto.TransactionMetering]) {
        case (map, (offset, applicationId, statistics, time)) =>
          val update = map.get(applicationId) match {
            case None =>
              DbDto.TransactionMetering(
                applicationId,
                statistics.committed.actions + statistics.rolledBack.actions,
                time,
                time,
                offset.toHexString,
                offset.toHexString,
              )
            case Some(m) =>
              m.copy(
                action_count =
                  m.action_count + statistics.committed.actions + statistics.rolledBack.actions,
                to_timestamp = time,
                to_ledger_offset = offset.toHexString,
              )
          }
          map + (applicationId -> update)
      }

    val batch = nonMetering ++ metering.values.toList.sortBy(_.application_id)

    Batch(
      lastOffset = input.last._1._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastStringInterningId = 0, // will be filled later in the sequential step
      lastRecordTime = input.last._1._2.recordTime.toInstant.toEpochMilli,
      batch = batch,
      batchSize = input.size,
      averageStartTime = input.view.map(_._2 / input.size).sum,
      offsets = input.view.map(_._1._1).toVector,
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
      averageStartTime = 0,
      offsets = Vector.empty,
    )

  def seqMapper(
      internize: Iterable[DbDto] => Iterable[(Int, String)],
      metrics: Metrics,
  )(
      previous: Batch[Vector[DbDto]],
      current: Batch[Vector[DbDto]],
  ): Batch[Vector[DbDto]] = {
    var eventSeqId = previous.lastSeqEventId
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

      case dbDto: DbDto.CreateFilter =>
        // we do not increase the event_seq_id here, because all the CreateFilter DbDto-s must have the same eventSeqId as the preceding EventCreate
        dbDto.copy(event_sequential_id = eventSeqId)

      case unChanged => unChanged
    }

    val (newLastStringInterningId, dbDtosWithStringInterning) =
      internize(batchWithSeqIds)
        .map(DbDto.StringInterningDto.from) match {
        case noNewEntries if noNewEntries.isEmpty =>
          previous.lastStringInterningId -> batchWithSeqIds
        case newEntries => newEntries.last.internalId -> (batchWithSeqIds ++ newEntries)
      }

    val nowNanos = System.nanoTime()
    metrics.daml.parallelIndexer.inputMapping.duration.update(
      (nowNanos - current.averageStartTime) / current.batchSize,
      TimeUnit.NANOSECONDS,
    )
    current.copy(
      lastSeqEventId = eventSeqId,
      lastStringInterningId = newLastStringInterningId,
      batch = dbDtosWithStringInterning,
      averageStartTime = nowNanos, // setting start time to the start of the next stage
    )
  }

  def batcher[DB_BATCH](
      batchF: Vector[DbDto] => DB_BATCH,
      metrics: Metrics,
  ): Batch[Vector[DbDto]] => Batch[DB_BATCH] = { inBatch =>
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
      withEnrichedLoggingContext("updateOffsets" -> batch.offsets) { implicit loggingContext =>
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
        lastStringInterningId = curr.lastStringInterningId,
        lastRecordTime = curr.lastRecordTime,
        batch = zeroDbBatch, // not used anymore
        batchSize = 0, // not used anymore
        averageStartTime = 0, // not used anymore
        offsets = Vector.empty, // not used anymore
      )

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
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      withEnrichedLoggingContext("updateOffset" -> batch.lastOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
          ingestTailFunction(ledgerEndFrom(batch))(connection)
          metrics.daml.indexer.ledgerEndSequentialId.updateValue(batch.lastSeqEventId)
          metrics.daml.indexer.lastReceivedRecordTime.updateValue(batch.lastRecordTime)
          metrics.daml.indexer.lastReceivedOffset.updateValue(batch.lastOffset.toHexString)
          logger.info("Ledger end updated")
          batch
        }
      }
}
