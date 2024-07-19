// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.metrics.InstrumentedGraph.*
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, Update}
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
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
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.daml.lf.data.Ref
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
    postProcessingParallelism: Int,
    excludedPackageIds: Set[Ref.PackageId],
    metrics: LedgerApiServerMetrics,
    inMemoryStateUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
    stringInterningView: StringInterning & InternizingStringInterningView,
    reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
    postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
    tracer: Tracer,
    loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with Spanning {
  import ParallelIndexerSubscription.*

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
      clock: Clock,
  )(implicit traceContext: TraceContext): InitializeParallelIngestion.Initialized => Handle = {
    initialized =>
      import MetricsContext.Implicits.empty
      val storeLedgerEndF = storeLedgerEnd(
        parameterStorageBackend.updateLedgerEnd,
        dbDispatcher,
        metrics,
        logger,
      )
      val storePostProcessingEndF = storePostProcessingEnd(
        parameterStorageBackend.updatePostProcessingEnd,
        dbDispatcher,
        metrics,
        logger,
      )
      val (killSwitch, completionFuture) = initialized.readServiceSource
        .buffered(metrics.parallelIndexer.inputBufferLength, maxInputBufferSize)
        .via(
          BatchingParallelIngestionPipe(
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
                  )
                ),
                UpdateToMeteringDbDto(
                  metrics = metrics.indexerEvents,
                  excludedPackageIds = excludedPackageIds,
                ),
                logger,
              )
            ),
            seqMapperZero = seqMapperZero(
              initialized.initialEventSeqId,
              initialized.initialStringInterningId,
              initialized.initialLastPublicationTime,
            ),
            seqMapper = seqMapper(
              dtos => stringInterningView.internize(DbDtoToStringsForInterning(dtos)),
              metrics,
              clock,
              logger,
            ),
            batchingParallelism = batchingParallelism,
            batcher = batcherExecutor.execute(
              batcher(ingestionStorageBackend.batch(_, stringInterningView))
            ),
            ingestingParallelism = ingestionParallelism,
            ingester = ingester(
              ingestFunction = ingestionStorageBackend.insertBatch,
              reassignmentOffsetPersistence = reassignmentOffsetPersistence,
              zeroDbBatch = ingestionStorageBackend.batch(Vector.empty, stringInterningView),
              dbDispatcher = dbDispatcher,
              logger = logger,
              metrics = metrics,
            ),
            maxTailerBatchSize = maxTailerBatchSize,
            ingestTail = ingestTail[DB_BATCH](
              storeLedgerEndF,
              logger,
            ),
          )
        )
// TODO(i18695): FIXME on big bang rollout
//        .async
//        .mapAsync(postProcessingParallelism)(
//          Future.successful
//          postProcess(
//            postProcessor,
//            logger,
//          )
//        )
        .batch(maxTailerBatchSize.toLong, Vector(_))(_ :+ _)
        .mapAsync(1)(
          ingestPostProcessEnd[DB_BATCH](
            storePostProcessingEndF,
            logger,
          )
        )
        .mapConcat(
          _.map(batch => (batch.offsetsUpdates, batch.lastSeqEventId, batch.publicationTime))
        )
        .buffered(
          counter = metrics.parallelIndexer.outputBatchedBufferLength,
          size = maxOutputBatchedBufferSize,
        )
        .via(inMemoryStateUpdaterFlow)
        .map { updates =>
          updates.foreach { case (_, Traced(update)) =>
            discard(update.persisted.trySuccess(()))
          }
        }
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
      publicationTime: CantonTimestamp,
      lastTraceContext: TraceContext,
      batch: T,
      batchSize: Int,
      offsetsUpdates: Vector[(Offset, Traced[Update])],
  )

  def inputMapper(
      metrics: LedgerApiServerMetrics,
      toDbDto: Offset => Traced[Update] => Iterator[DbDto],
      toMeteringDbDto: Iterable[(Offset, Traced[Update])] => Vector[DbDto.TransactionMetering],
      logger: TracedLogger,
  ): Iterable[(Offset, Traced[Update])] => Batch[Vector[DbDto]] = { input =>
    metrics.parallelIndexer.inputMapping.batchSize.update(input.size)(MetricsContext.Empty)

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

    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    val last = input.last

    Batch(
      lastOffset = last._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastStringInterningId = 0, // will be filled later in the sequential step
      lastRecordTime = last._2.value.recordTime.toInstant.toEpochMilli,
      publicationTime = CantonTimestamp.MinValue, // will be filled later in the equential step
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
      initialPublicationTime: CantonTimestamp,
  ): Batch[Vector[DbDto]] =
    Batch(
      lastOffset = Offset.beforeBegin,
      lastSeqEventId = initialSeqId, // this is property of interest in the zero element
      lastStringInterningId =
        initialStringInterningId, // this is property of interest in the zero element
      lastRecordTime = 0,
      publicationTime =
        initialPublicationTime, // this is property of interest in the zero element: sets the lower bound for publication time
      lastTraceContext = TraceContext.empty,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )

  def seqMapper(
      internize: Iterable[DbDto] => Iterable[(Int, String)],
      metrics: LedgerApiServerMetrics,
      clock: Clock,
      logger: TracedLogger,
  )(
      previous: Batch[Vector[DbDto]],
      current: Batch[Vector[DbDto]],
  ): Batch[Vector[DbDto]] = {
    Timed.value(
      metrics.parallelIndexer.seqMapping.duration, {
        discard(
          current.offsetsUpdates.foldLeft(previous.lastOffset) { case (prev, (curr, _)) =>
            assert(prev < curr, s"Monotonic Offset violation detected from $prev to $curr")
            curr
          }
        )
        val publicationTime = {
          val now = clock.monotonicTime()
          val next = Ordering[CantonTimestamp].max(
            now,
            previous.publicationTime,
          )
          if (now < next) {
            implicit val batchTraceContext: TraceContext = TraceContext.ofBatch(
              current.offsetsUpdates.iterator.map(_._2)
            )(logger)
            logger.info(
              s"Local participant clock at $now is before a previous publication time $next. Has the clock been reset, e.g., during participant failover?"
            )
          }
          next
        }

        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var eventSeqId = previous.lastSeqEventId
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var lastTransactionMetaEventSeqId = eventSeqId
        val batchWithSeqIdsAndPublicationTime = current.batch.map {
          case dbDto: DbDto.EventCreate =>
            eventSeqId += 1
            dbDto.copy(event_sequential_id = eventSeqId)

          case dbDto: DbDto.EventExercise =>
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
                publication_time = publicationTime.underlying.micros,
              )
              .tap(_ => lastTransactionMetaEventSeqId = eventSeqId)

          case dbDto: DbDto.CommandCompletion =>
            dbDto.copy(publication_time = publicationTime.underlying.micros)

          case unChanged: DbDto.PartyEntry => unChanged
          case unChanged: DbDto.StringInterningDto => unChanged
          case unChanged: DbDto.TransactionMetering => unChanged
          case unChanged: DbDto.SequencerIndexMoved => unChanged
        }

        val (newLastStringInterningId, dbDtosWithStringInterning) =
          internize(batchWithSeqIdsAndPublicationTime)
            .map(DbDto.StringInterningDto.from)
            .pipe(newEntries =>
              newEntries.lastOption.fold(
                previous.lastStringInterningId -> batchWithSeqIdsAndPublicationTime
              )(last => last.internalId -> (batchWithSeqIdsAndPublicationTime ++ newEntries))
            )

        current.copy(
          lastSeqEventId = eventSeqId,
          lastStringInterningId = newLastStringInterningId,
          publicationTime = publicationTime,
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
      reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
      zeroDbBatch: DB_BATCH,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batch =>
      LoggingContextWithTrace.withNewLoggingContext(
        "updateOffsets" -> batch.offsetsUpdates.map(_._1)
      ) { implicit loggingContext =>
        val batchTraceContext: TraceContext = TraceContext.ofBatch(
          batch.offsetsUpdates.iterator.map(_._2)
        )(logger)
        reassignmentOffsetPersistence
          .persist(
            batch.offsetsUpdates.map { case (offset, tracedUpdate) =>
              tracedUpdate.value -> offset
            },
            logger,
          )(batchTraceContext)
          .flatMap(_ =>
            dbDispatcher.executeSql(metrics.parallelIndexer.ingestion) { connection =>
              metrics.parallelIndexer.updates.inc(batch.batchSize.toLong)(MetricsContext.Empty)
              ingestFunction(connection, batch.batch)
              cleanUnusedBatch(zeroDbBatch)(batch)
            }
          )(directExecutionContext)
      }
  }

  def ledgerEndFrom(batch: Batch[_]): LedgerEnd =
    LedgerEnd(
      lastOffset = batch.lastOffset,
      lastEventSeqId = batch.lastSeqEventId,
      lastStringInterningId = batch.lastStringInterningId,
      lastPublicationTime = batch.publicationTime,
    )

  def ledgerEndDomainIndexFrom(
      domainIndexes: Vector[(DomainId, DomainIndex)]
  ): Map[DomainId, DomainIndex] =
    domainIndexes.groupMapReduce(_._1)(_._2)(_ max _)

  def ingestTail[DB_BATCH](
      storeLedgerEnd: (LedgerEnd, Map[DomainId, DomainIndex]) => Future[Unit],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Vector[Batch[DB_BATCH]] => Future[Vector[Batch[DB_BATCH]]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batchOfBatches =>
      batchOfBatches.lastOption match {
        case Some(lastBatch) =>
          storeLedgerEnd(
            ledgerEndFrom(lastBatch),
            ledgerEndDomainIndexFrom(
              batchOfBatches
                .flatMap(_.offsetsUpdates)
                .flatMap(_._2.value.domainIndexOpt)
            ),
          ).map(_ => batchOfBatches)(directExecutionContext)

        case None =>
          val message = "Unexpectedly encountered a zero-sized batch in ingestTail"
          logger.error(message)
          Future.failed(new IllegalStateException(message))
      }
  }

  private def storeLedgerEnd(
      storeTailFunction: (LedgerEnd, Map[DomainId, DomainIndex]) => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): (LedgerEnd, Map[DomainId, DomainIndex]) => Future[Unit] =
    (ledgerEnd, ledgerEndDomainIndexes) =>
      LoggingContextWithTrace.withNewLoggingContext("updateOffset" -> ledgerEnd.lastOffset) {
        implicit loggingContext =>
          def domainIndexesLog: String = ledgerEndDomainIndexes.toVector
            .sortBy(_._1.toProtoPrimitive)
            .map { case (domainId, domainIndex) =>
              s"${domainId.toProtoPrimitive.take(20).mkString} -> $domainIndex"
            }
            .mkString("domain-indexes: [", ", ", "]")

          dbDispatcher.executeSql(metrics.parallelIndexer.tailIngestion) { connection =>
            storeTailFunction(ledgerEnd, ledgerEndDomainIndexes)(connection)
            metrics.indexer.ledgerEndSequentialId
              .updateValue(ledgerEnd.lastEventSeqId)
            logger.debug(
              s"Ledger end updated in IndexDB $domainIndexesLog ${loggingContext
                  .serializeFiltered("updateOffset")}."
            )(loggingContext.traceContext)
          }
      }

  def postProcess[DB_BATCH](
      processor: (Vector[PostPublishData], TraceContext) => Future[Unit],
      logger: TracedLogger,
  ): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batch => {
      val batchTraceContext: TraceContext = TraceContext.ofBatch(
        batch.offsetsUpdates.iterator.map(_._2)
      )(logger)
      val postPublishData = batch.offsetsUpdates.flatMap { case (offset, tracedUpdate) =>
        PostPublishData.from(tracedUpdate, offset, batch.publicationTime)
      }
      processor(postPublishData, batchTraceContext).map(_ => batch)(directExecutionContext)
    }
  }

  def ingestPostProcessEnd[DB_BATCH](
      storePostProcessingEnd: Offset => Future[Unit],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Vector[Batch[DB_BATCH]] => Future[Vector[Batch[DB_BATCH]]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batchOfBatches =>
      batchOfBatches.lastOption match {
        case Some(lastBatch) =>
          storePostProcessingEnd(lastBatch.lastOffset)
            .map(_ => batchOfBatches)(directExecutionContext)
        case None =>
          val message = "Unexpectedly encountered a zero-sized batch in ingestPostProcessEnd"
          logger.error(message)
          Future.failed(new IllegalStateException(message))
      }
  }

  def storePostProcessingEnd(
      storePostProcessEndFunction: Offset => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Offset => Future[Unit] = offset =>
    LoggingContextWithTrace.withNewLoggingContext("updateOffset" -> offset) {
      implicit loggingContext =>
        dbDispatcher.executeSql(metrics.parallelIndexer.postProcessingEndIngestion) { connection =>
          storePostProcessEndFunction(offset)(connection)
          logger.debug(
            s"Post Processing end updated in IndexDB, ${loggingContext.serializeFiltered("updateOffset")}."
          )(loggingContext.traceContext)
        }
    }

  private def cleanUnusedBatch[DB_BATCH](
      zeroDbBatch: DB_BATCH
  ): Batch[DB_BATCH] => Batch[DB_BATCH] =
    _.copy(
      batch = zeroDbBatch, // not used anymore
      batchSize = 0, // not used anymore
    )
}

trait ReassignmentOffsetPersistence {
  def persist(
      updates: Seq[(Update, Offset)],
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): Future[Unit]
}

object NoOpReassignmentOffsetPersistence extends ReassignmentOffsetPersistence {
  override def persist(
      updates: Seq[(Update, Offset)],
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful(())
}
