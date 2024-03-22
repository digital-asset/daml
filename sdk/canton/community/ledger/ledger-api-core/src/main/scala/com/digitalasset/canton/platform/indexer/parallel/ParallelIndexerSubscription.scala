// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.lf.data.Ref
import com.daml.metrics.InstrumentedGraph.*
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.Update
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.index.InMemoryStateUpdater
import com.digitalasset.canton.platform.indexer.ha.Handle
import com.digitalasset.canton.platform.indexer.parallel.AsyncSupport.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.*
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.digitalasset.canton.platform.store.interning.{
  InternizingStringInterningView,
  StringInterning,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}

import java.sql.Connection
import scala.concurrent.Future
import scala.util.chaining.*

private[platform] final case class ParallelIndexerSubscription[DB_BATCH](
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
    stringInterningView: StringInterning & InternizingStringInterningView,
    tracer: Tracer,
    loggerFactory: NamedLoggerFactory,
    multiDomainEnabled: Boolean,
) extends NamedLogging
    with Spanning {
  import ParallelIndexerSubscription.*
  private implicit val metricsContext: MetricsContext = MetricsContext(
    "participant_id" -> participantId
  )
  private def mapInSpan(
      mapper: Offset => Traced[Update] => Iterator[DbDto]
  )(offset: Offset)(update: Traced[Update]): Iterator[DbDto] = {
    withSpan("Indexer.mapInput")(_ => _ => mapper(offset)(update))(update.traceContext, tracer)
  }

  def apply(
      inputMapperExecutor: Executor,
      batcherExecutor: Executor,
      dbDispatcher: DbDispatcher,
      materializer: Materializer,
  )(implicit traceContext: TraceContext): InitializeParallelIngestion.Initialized => Handle = {
    initialized =>
      val (killSwitch, completionFuture) = BatchingParallelIngestionPipe(
        submissionBatchSize = submissionBatchSize,
        inputMappingParallelism = inputMappingParallelism,
        inputMapper = inputMapperExecutor.execute(
          inputMapper(
            metrics,
            mapInSpan(
              UpdateToDbDto(
                participantId = participantId,
                translation = translation,
                compressionStrategy = compressionStrategy,
                metrics = metrics,
                multiDomainEnabled = multiDomainEnabled,
              )
            ),
            UpdateToMeteringDbDto(metrics = metrics.daml.indexerEvents),
            logger,
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
        ingestTail = ingestTail[DB_BATCH](
          parameterStorageBackend.updateLedgerEnd,
          dbDispatcher,
          metrics,
          logger,
        ),
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

  /** Batch wraps around a T-typed batch, enriching it with processing relevant information.
    *
    * @param lastOffset The latest offset available in the batch. Needed for tail ingestion.
    * @param lastSeqEventId The latest sequential-event-id in the batch, or if none present there, then the latest from before. Needed for tail ingestion.
    * @param lastStringInterningId The latest string interning id in the batch, or if none present there, then the latest from before. Needed for tail ingestion.
    * @param lastRecordTime The latest record time in the batch, in milliseconds since Epoch. Needed for metrics population.
    * @param lastTraceContext The latest trace context contained in the batch. Needed for logging.
    * @param batch The batch of variable type.
    * @param batchSize Size of the batch measured in number of updates. Needed for metrics population.
    */
  final case class Batch[+T](
      lastOffset: Offset,
      lastSeqEventId: Long,
      lastStringInterningId: Int,
      lastRecordTime: Long,
      lastTraceContext: TraceContext,
      batch: T,
      batchSize: Int,
      offsetsUpdates: Vector[(Offset, Traced[Update])],
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => Traced[Update] => Iterator[DbDto],
      toMeteringDbDto: Iterable[(Offset, Traced[Update])] => Vector[DbDto.TransactionMetering],
      logger: TracedLogger,
  ): Iterable[(Offset, Traced[Update])] => Batch[Vector[DbDto]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)(MetricsContext.Empty)

    val mainBatch = input.iterator.flatMap { case (offset, update) =>
      val prefix = update.value match {
        case _: Update.TransactionAccepted => "Phase 7: "
        case _ => ""
      }
      logger.info(
        s"${prefix}Storing at offset=${offset.bytes.toHexString} ${update.value}"
      )(update.traceContext)
      toDbDto(offset)(update)

    }.toVector

    val meteringBatch = toMeteringDbDto(input)

    val batch = mainBatch ++ meteringBatch

    // TODO(i11665): Replace with NonEmpty after sorting out the dependencies
    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    val last = input.last

    Batch(
      lastOffset = last._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastStringInterningId = 0, // will be filled later in the sequential step
      lastRecordTime = last._2.value.recordTime.toInstant.toEpochMilli,
      lastTraceContext = last._2.traceContext,
      batch = batch,
      batchSize = input.size,
      offsetsUpdates = input.toVector,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
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
      lastTraceContext = TraceContext.empty,
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
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var eventSeqId = previous.lastSeqEventId
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
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

          case dbDto: DbDto.EventUnassign =>
            eventSeqId += 1
            dbDto.copy(event_sequential_id = eventSeqId)

          case dbDto: DbDto.EventAssign =>
            eventSeqId += 1
            dbDto.copy(event_sequential_id = eventSeqId)

          // we do not increase the event_seq_id here, because all the DbDto-s must have the same eventSeqId as the preceding Event
          case dbDto: DbDto.IdFilterCreateStakeholder =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterCreateNonStakeholderInformee =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterConsumingStakeholder =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterConsumingNonStakeholderInformee =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterNonConsumingInformee =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterUnassignStakeholder =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.IdFilterAssignStakeholder =>
            dbDto.copy(event_sequential_id = eventSeqId)
          case dbDto: DbDto.TransactionMeta =>
            dbDto
              .copy(
                event_sequential_id_first = lastTransactionMetaEventSeqId + 1,
                event_sequential_id_last = eventSeqId,
              )
              .tap(_ => lastTransactionMetaEventSeqId = eventSeqId)

          case unChanged: DbDto.CommandCompletion => unChanged
          case unChanged: DbDto.ConfigurationEntry => unChanged
          case unChanged: DbDto.Package => unChanged
          case unChanged: DbDto.PackageEntry => unChanged
          case unChanged: DbDto.PartyEntry => unChanged
          case unChanged: DbDto.StringInterningDto => unChanged
          case unChanged: DbDto.TransactionMetering => unChanged
        }

        val (newLastStringInterningId, dbDtosWithStringInterning) =
          internize(batchWithSeqIds)
            .map(DbDto.StringInterningDto.from)
            .pipe(newEntries =>
              newEntries.lastOption.fold(previous.lastStringInterningId -> batchWithSeqIds)(last =>
                last.internalId -> (batchWithSeqIds ++ newEntries)
              )
            )

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
  )(implicit traceContext: TraceContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      LoggingContextWithTrace.withNewLoggingContext(
        "updateOffsets" -> batch.offsetsUpdates.map(_._1)
      ) { implicit loggingContext =>
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
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Vector[Batch[DB_BATCH]] => Future[Vector[Batch[DB_BATCH]]] = batchOfBatches =>
    batchOfBatches.lastOption match {
      case Some(lastBatch) =>
        LoggingContextWithTrace.withNewLoggingContext("updateOffset" -> lastBatch.lastOffset) {
          implicit loggingContext =>
            dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
              ingestTailFunction(ledgerEndFrom(lastBatch))(connection)
              metrics.daml.indexer.ledgerEndSequentialId
                .updateValue(lastBatch.lastSeqEventId)
              metrics.daml.indexer.lastReceivedRecordTime
                .updateValue(lastBatch.lastRecordTime)
              logger.debug(
                s"Ledger end updated in IndexDB, ${loggingContext.serializeFiltered("updateOffset")}."
              )(lastBatch.lastTraceContext)
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
