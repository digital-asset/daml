// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.metrics.InstrumentedGraph.*
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.CommitRepair
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, Update}
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.InMemoryState
import com.digitalasset.canton.platform.index.InMemoryStateUpdater
import com.digitalasset.canton.platform.indexer.ha.Handle
import com.digitalasset.canton.platform.indexer.parallel.AsyncSupport.*
import com.digitalasset.canton.platform.store.backend.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.PekkoUtil.{Commit, FutureQueue, PekkoSourceQueueToFutureQueue}
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer, OverflowStrategy}
import org.apache.pekko.{Done, NotUsed}

import java.sql.Connection
import java.util.concurrent.atomic.AtomicReference
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
    inMemoryState: InMemoryState,
    reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
    postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
    tracer: Tracer,
    loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with Spanning {
  import ParallelIndexerSubscription.*

  private def mapInSpan(
      mapper: AbsoluteOffset => Update => Iterator[DbDto]
  )(offset: AbsoluteOffset)(update: Update): Iterator[DbDto] =
    withSpan("Indexer.mapInput")(_ => _ => mapper(offset)(update))(update.traceContext, tracer)

  def apply(
      inputMapperExecutor: Executor,
      batcherExecutor: Executor,
      dbDispatcher: DbDispatcher,
      materializer: Materializer,
      initialLedgerEnd: Option[LedgerEnd],
      commit: Commit,
      clock: Clock,
      repairMode: Boolean,
  )(implicit
      traceContext: TraceContext
  ): (Handle, Future[Done] => FutureQueue[(Long, Update)]) = {
    import MetricsContext.Implicits.empty
    val aggregatedLedgerEndForRepair
        : AtomicReference[Option[(LedgerEnd, Map[DomainId, DomainIndex])]] =
      // the LedgerEnd necessarily will be updated as successful repair has at least a CommitRepair Update, which carries the LedgerEnd forward
      new AtomicReference(None)
    val storeLedgerEndF = storeLedgerEnd(
      parameterStorageBackend.updateLedgerEnd,
      dbDispatcher,
      metrics,
      logger,
    )
    val storePostProcessingEndF = storePostProcessingEnd(
      offset => parameterStorageBackend.updatePostProcessingEnd(Some(offset)),
      dbDispatcher,
      metrics,
      logger,
    )

    val ((sourceQueue, uniqueKillSwitch), completionFuture) = Source
      .queue[(Long, Update)](
        bufferSize = maxInputBufferSize,
        overflowStrategy = OverflowStrategy.backpressure,
        maxConcurrentOffers = 1, // This queue is fed by the RecoveringQueue which is sequential
      )
      .takeWhile(
        // The queue is only consumed until the first CommitRepair:
        //   - in case Repair Mode, it is invalid to use this queue after committed,
        //   - in case normal indexing, CommitRepair is invalid.
        // The queue and stream processing completes after the CommitRepair.
        !_._2.isInstanceOf[CommitRepair],
        // the stream also must hold the CommitRepair event itself
        inclusive = true,
      )
      .map { case (longOffset, update) => AbsoluteOffset.tryFromLong(longOffset) -> update }
      .via(monotonicOffsetValidator)
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
                metrics = metrics.indexer,
                excludedPackageIds = excludedPackageIds,
              ),
              logger,
            )
          ),
          seqMapperZero = seqMapperZero(initialLedgerEnd),
          seqMapper = seqMapper(
            dtos =>
              inMemoryState.stringInterningView
                .internize(DbDtoToStringsForInterning(dtos)),
            metrics,
            clock,
            logger,
          ),
          batchingParallelism = batchingParallelism,
          batcher = batcherExecutor.execute(
            batcher(
              ingestionStorageBackend.batch(
                _,
                inMemoryState.stringInterningView,
              )
            )
          ),
          ingestingParallelism = ingestionParallelism,
          ingester = ingester(
            ingestFunction = ingestionStorageBackend.insertBatch,
            reassignmentOffsetPersistence = reassignmentOffsetPersistence,
            zeroDbBatch = ingestionStorageBackend.batch(
              Vector.empty,
              inMemoryState.stringInterningView,
            ),
            dbDispatcher = dbDispatcher,
            logger = logger,
            metrics = metrics,
          ),
          maxTailerBatchSize = maxTailerBatchSize,
          ingestTail =
            if (repairMode) { (batchOfBatches: Vector[Batch[DB_BATCH]]) =>
              aggregateLedgerEndForRepair[DB_BATCH](aggregatedLedgerEndForRepair)(batchOfBatches)
              Future.successful(batchOfBatches)
            } else
              ingestTail[DB_BATCH](
                storeLedgerEndF,
                logger,
              ),
        )
      )
      .async
      .mapAsync(postProcessingParallelism)(
        postProcess(
          postProcessor,
          logger,
        )
      )
      .batch(maxTailerBatchSize.toLong, Vector(_))(_ :+ _)
      .via(
        if (repairMode) {
          // no need to aggregate the postProcessingEnd for repair: this will be done implicitly by aggregating the ledger end
          Flow.apply
        } else {
          Flow.apply.mapAsync(1)(
            ingestPostProcessEnd[DB_BATCH](
              storePostProcessingEndF,
              logger,
            )
          )
        }
      )
      .mapConcat(
        _.map(batch => (batch.offsetsUpdates, batch.ledgerEnd))
      )
      .buffered(
        counter = metrics.indexer.outputBatchedBufferLength,
        size = maxOutputBatchedBufferSize,
      )
      .via(inMemoryStateUpdaterFlow(repairMode))
      .via(
        if (repairMode) {
          Flow.apply.mapAsync(1)(
            commitRepair(
              storeLedgerEndF,
              storePostProcessingEndF,
              ledgerEnd =>
                InMemoryStateUpdater.updateLedgerEnd(
                  inMemoryState = inMemoryState,
                  ledgerEnd = ledgerEnd,
                  logger,
                ),
              aggregatedLedgerEndForRepair,
              logger,
            )
          )
        } else {
          Flow.apply
        }
      )
      .map { updates =>
        updates.lastOption.foreach { case (offset, _) =>
          commit(offset.unwrap)
        }
        updates.foreach { case (_, update) =>
          discard(update.persisted.trySuccess(()))
        }
      }
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.ignore)(Keep.both)
      .run()(materializer)
    (
      Handle(completionFuture.map(_ => ())(materializer.executionContext), uniqueKillSwitch),
      sourceDone =>
        new PekkoSourceQueueToFutureQueue(
          sourceQueue = sourceQueue,
          sourceDone = sourceDone,
          loggerFactory = loggerFactory,
        ),
    )
  }
}

object ParallelIndexerSubscription {

  /** Batch wraps around a T-typed batch, enriching it with processing relevant information.
    *
    * @param ledgerEnd The LedgerEnd for the batch. Needed for tail ingestion.
    * @param lastTraceContext The latest trace context contained in the batch. Needed for logging.
    * @param batch The batch of variable type.
    * @param batchSize Size of the batch measured in number of updates. Needed for metrics population.
    */
  final case class Batch[+T](
      ledgerEnd: LedgerEnd,
      lastTraceContext: TraceContext,
      batch: T,
      batchSize: Int,
      offsetsUpdates: Vector[(AbsoluteOffset, Update)],
  )

  val ZeroLedgerEnd: LedgerEnd = LedgerEnd(
    lastOffset = AbsoluteOffset.MaxValue, // it will not be used, will be overridden
    lastEventSeqId =
      0L, // this is a property of interest in the zero element, we start the sequential ids at 1L
    lastStringInterningId =
      0, // this is a property of interest in the zero element, we start the string interning ids at 1
    lastPublicationTime =
      CantonTimestamp.MinValue, // this is a property of interest in the zero element: sets the lower bound for publication time, we start at MinValue
  )

  def monotonicOffsetValidator[T]: Flow[(AbsoluteOffset, T), (AbsoluteOffset, T), NotUsed] =
    Flow[(AbsoluteOffset, T)].statefulMap[Option[AbsoluteOffset], (AbsoluteOffset, T)](() => None)(
      { case (prevO, (curr, upd)) =>
        assert(
          prevO < Some(curr),
          s"Monotonic Offset violation detected from ${prevO.getOrElse("participant begin")} to $curr",
        )
        (Some(curr), (curr, upd))
      },
      _ => None,
    )

  def inputMapper(
      metrics: LedgerApiServerMetrics,
      toDbDto: AbsoluteOffset => Update => Iterator[DbDto],
      toMeteringDbDto: Iterable[(AbsoluteOffset, Update)] => Vector[
        DbDto.TransactionMetering
      ],
      logger: TracedLogger,
  ): Iterable[(AbsoluteOffset, Update)] => Batch[Vector[DbDto]] = { input =>
    metrics.indexer.inputMapping.batchSize.update(input.size)(MetricsContext.Empty)

    val mainBatch = input.iterator.flatMap { case (offset, update) =>
      val prefix = update match {
        case _: Update.TransactionAccepted => "Phase 7: "
        case _ => ""
      }
      logger.info(
        s"${prefix}Storing at offset=${offset.unwrap} $update"
      )(update.traceContext)
      toDbDto(offset)(update)

    }.toVector

    val meteringBatch = toMeteringDbDto(input)

    val batch = mainBatch ++ meteringBatch

    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    val last = input.last

    Batch(
      ledgerEnd = ZeroLedgerEnd.copy(
        lastOffset = last._1
        // the rest will be filled later in the sequential step
      ),
      lastTraceContext = last._2.traceContext,
      batch = batch,
      batchSize = input.size,
      offsetsUpdates = input.toVector,
    )
  }

  def seqMapperZero(
      initialLedgerEndO: Option[LedgerEnd]
  ): Batch[Vector[DbDto]] =
    Batch(
      ledgerEnd = initialLedgerEndO.getOrElse(ZeroLedgerEnd),
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
  ): Batch[Vector[DbDto]] =
    Timed.value(
      metrics.indexer.seqMapping.duration, {
        val publicationTime = {
          val now = clock.monotonicTime()
          val next = Ordering[CantonTimestamp].max(
            now,
            previous.ledgerEnd.lastPublicationTime,
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
        var eventSeqId = previous.ledgerEnd.lastEventSeqId
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

          case dbDto: DbDto.EventPartyToParticipant =>
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
                previous.ledgerEnd.lastStringInterningId -> batchWithSeqIdsAndPublicationTime
              )(last => last.internalId -> (batchWithSeqIdsAndPublicationTime ++ newEntries))
            )

        current.copy(
          ledgerEnd = current.ledgerEnd.copy(
            lastEventSeqId = eventSeqId,
            lastStringInterningId = newLastStringInterningId,
            lastPublicationTime = publicationTime,
          ),
          batch = dbDtosWithStringInterning,
        )
      },
    )

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
              tracedUpdate -> Offset.fromAbsoluteOffset(offset)
            },
            logger,
          )(batchTraceContext)
          .flatMap(_ =>
            dbDispatcher.executeSql(metrics.indexer.ingestion) { connection =>
              metrics.indexer.updates.inc(batch.batchSize.toLong)(MetricsContext.Empty)
              ingestFunction(connection, batch.batch)
              cleanUnusedBatch(zeroDbBatch)(batch)
            }
          )(directExecutionContext)
      }
  }

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
            lastBatch.ledgerEnd,
            ledgerEndDomainIndexFrom(
              batchOfBatches
                .flatMap(_.offsetsUpdates)
                .flatMap(_._2.domainIndexOpt)
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
      LoggingContextWithTrace.withNewLoggingContext(
        "updateOffset" -> Offset.fromAbsoluteOffset(ledgerEnd.lastOffset)
      ) { implicit loggingContext =>
        def domainIndexesLog: String = ledgerEndDomainIndexes.toVector
          .sortBy(_._1.toProtoPrimitive)
          .map { case (domainId, domainIndex) =>
            s"${domainId.toProtoPrimitive.take(20).mkString} -> $domainIndex"
          }
          .mkString("domain-indexes: [", ", ", "]")

        dbDispatcher.executeSql(metrics.indexer.tailIngestion) { connection =>
          storeTailFunction(ledgerEnd, ledgerEndDomainIndexes)(connection)
          metrics.indexer.ledgerEndSequentialId
            .updateValue(ledgerEnd.lastEventSeqId)
          logger.debug(
            s"Ledger end updated in IndexDB $domainIndexesLog ${loggingContext
                .serializeFiltered("updateOffset")}."
          )(loggingContext.traceContext)
        }
      }

  def aggregateLedgerEndForRepair[DB_BATCH](
      aggregatedLedgerEnd: AtomicReference[Option[(LedgerEnd, Map[DomainId, DomainIndex])]]
  ): Vector[Batch[DB_BATCH]] => Unit =
    batchOfBatches =>
      batchOfBatches.lastOption.foreach(lastBatch =>
        discard(aggregatedLedgerEnd.updateAndGet { aggregated =>
          val oldDomainIndexes =
            aggregated.fold(Vector.empty[(DomainId, DomainIndex)])(_._2.toVector)
          // this will also have at the end the offset bump for the CommitRepair Update as well, we accept this for sake of simplicity
          val newLedgerEnd = lastBatch.ledgerEnd
          val domainIndexesForBatchOfBatches = batchOfBatches
            .flatMap(_.offsetsUpdates)
            .flatMap(_._2.domainIndexOpt)
          val newDomainIndexes =
            ledgerEndDomainIndexFrom(oldDomainIndexes ++ domainIndexesForBatchOfBatches)
          Some((newLedgerEnd, newDomainIndexes))
        })
      )

  def postProcess[DB_BATCH](
      processor: (Vector[PostPublishData], TraceContext) => Future[Unit],
      logger: TracedLogger,
  ): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batch => {
      val batchTraceContext: TraceContext = TraceContext.ofBatch(
        batch.offsetsUpdates.iterator.map(_._2)
      )(logger)
      val postPublishData = batch.offsetsUpdates.flatMap { case (offset, update) =>
        PostPublishData.from(
          update,
          offset,
          batch.ledgerEnd.lastPublicationTime,
        )
      }
      processor(postPublishData, batchTraceContext).map(_ => batch)(directExecutionContext)
    }
  }

  def ingestPostProcessEnd[DB_BATCH](
      storePostProcessingEnd: AbsoluteOffset => Future[Unit],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Vector[Batch[DB_BATCH]] => Future[Vector[Batch[DB_BATCH]]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batchOfBatches =>
      batchOfBatches.lastOption match {
        case Some(lastBatch) =>
          storePostProcessingEnd(lastBatch.ledgerEnd.lastOffset)
            .map(_ => batchOfBatches)(directExecutionContext)
        case None =>
          val message = "Unexpectedly encountered a zero-sized batch in ingestPostProcessEnd"
          logger.error(message)
          Future.failed(new IllegalStateException(message))
      }
  }

  def storePostProcessingEnd(
      storePostProcessEndFunction: AbsoluteOffset => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): AbsoluteOffset => Future[Unit] = offset =>
    LoggingContextWithTrace.withNewLoggingContext(
      "updateOffset" -> Offset.fromAbsoluteOffset(offset)
    ) { implicit loggingContext =>
      dbDispatcher.executeSql(metrics.indexer.postProcessingEndIngestion) { connection =>
        storePostProcessEndFunction(offset)(connection)
        logger.debug(
          s"Post Processing end updated in IndexDB, ${loggingContext.serializeFiltered("updateOffset")}."
        )(loggingContext.traceContext)
      }
    }

  def commitRepair[DB_BATCH](
      storeLedgerEnd: (LedgerEnd, Map[DomainId, DomainIndex]) => Future[Unit],
      storePostProcessingEnd: AbsoluteOffset => Future[Unit],
      updateInMemoryState: LedgerEnd => Unit,
      aggregatedLedgerEnd: AtomicReference[Option[(LedgerEnd, Map[DomainId, DomainIndex])]],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Vector[(AbsoluteOffset, Update)] => Future[
    Vector[(AbsoluteOffset, Update)]
  ] = {
    implicit val ec: DirectExecutionContext = DirectExecutionContext(logger)
    offsetsAndUpdates =>
      offsetsAndUpdates.lastOption match {
        case Some((_, CommitRepair())) =>
          aggregatedLedgerEnd.get() match {
            case Some((ledgerEnd, domainIndexes)) =>
              for {
                // this order is important to respect crash recovery rules
                _ <- storeLedgerEnd(ledgerEnd, domainIndexes)
                _ <- storePostProcessingEnd(ledgerEnd.lastOffset)
                _ = updateInMemoryState(ledgerEnd)
              } yield {
                logger.info("Repair committed, Ledger End stored and updated successfully.")
                offsetsAndUpdates
              }
            case None =>
              val message = "Unexpectedly the Repair committed did not update the Ledger End."
              logger.error(message)
              Future.failed(new IllegalStateException(message))
          }

        case Some(_) =>
          Future.successful(offsetsAndUpdates)

        case None =>
          val message = "Unexpectedly encountered a zero-sized batch in ingestTail"
          logger.error(message)
          Future.failed(new IllegalStateException(message))
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
