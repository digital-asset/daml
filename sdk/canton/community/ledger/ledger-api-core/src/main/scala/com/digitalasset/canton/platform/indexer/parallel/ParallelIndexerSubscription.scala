// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.logging.entries.LoggingEntries
import com.daml.metrics.InstrumentedGraph.*
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.CommitRepair
import com.digitalasset.canton.ledger.participant.state.{
  ParticipantUpdate,
  SynchronizerIndex,
  SynchronizerIndexUpdate,
  Update,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
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
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.PekkoUtil.{Commit, FutureQueue, PekkoSourceQueueToFutureQueue}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer, OverflowStrategy}
import org.apache.pekko.{Done, NotUsed}

import java.sql.Connection
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered
import scala.util.chaining.*

private[platform] final case class ParallelIndexerSubscription[DB_BATCH](
    ingestionStorageBackend: IngestionStorageBackend[DB_BATCH],
    parameterStorageBackend: ParameterStorageBackend,
    contractStorageBackend: ContractStorageBackend,
    participantId: Ref.ParticipantId,
    translation: LfValueTranslation,
    compressionStrategy: CompressionStrategy,
    maxInputBufferSize: Int,
    inputMappingParallelism: Int,
    dbPrepareParallelism: Int,
    batchingParallelism: Int,
    ingestionParallelism: Int,
    submissionBatchSize: Long,
    maxOutputBatchedBufferSize: Int,
    maxTailerBatchSize: Int,
    postProcessingParallelism: Int,
    metrics: LedgerApiServerMetrics,
    inMemoryStateUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
    inMemoryState: InMemoryState,
    reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
    postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
    sequentialPostProcessor: Update => Unit,
    disableMonotonicityChecks: Boolean,
    tracer: Tracer,
    loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with Spanning {
  import ParallelIndexerSubscription.*

  private def mapInSpan(
      mapper: Offset => Update => Iterator[DbDto]
  )(offset: Offset)(update: Update): Iterator[DbDto] =
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
        : AtomicReference[Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]] =
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
    val loadPreviousSynchronizerIndexF = (synchronizerId: SynchronizerId) =>
      dbDispatcher.executeSql(metrics.index.db.getCleanSynchronizerIndex)(
        parameterStorageBackend.cleanSynchronizerIndex(synchronizerId)
      )(LoggingContextWithTrace(loggerFactory))

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
      .map { case (longOffset, update) => Offset.tryFromLong(longOffset) -> update }
      .via(
        if (disableMonotonicityChecks)
          Flow.apply
        else
          monotonicityValidator(
            initialOffset = initialLedgerEnd.map(_.lastOffset),
            loadPreviousState = loadPreviousSynchronizerIndexF,
          )(logger)
      )
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
              EventMetricsUpdater(metrics.indexer.meteredEventsMeter),
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
            inMemoryState.ledgerEndCache,
          ),
          dbPrepareParallelism = dbPrepareParallelism,
          dbPrepare = dbPrepare(
            lastActivations = contractStorageBackend.lastActivations,
            dbDispatcher = dbDispatcher,
            logger = logger,
            metrics = metrics,
          ),
          batchingParallelism = batchingParallelism,
          batcher = batcherExecutor.execute(
            batcher(
              ingestionStorageBackend.batch(
                _,
                inMemoryState.stringInterningView,
              ),
              logger,
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
      .map(sequentialPostProcess(sequentialPostProcessor))
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
        updates
          .collect { case (_, participantUpdate: ParticipantUpdate) =>
            participantUpdate
          }
          .foreach(_.persisted.trySuccess(()).discard)
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
  val EmptyActiveContracts: mutable.LinkedHashMap[(SynchronizerId, ContractId), Long] =
    mutable.LinkedHashMap.empty

  /** Batch wraps around a T-typed batch, enriching it with processing relevant information.
    *
    * @param ledgerEnd
    *   The LedgerEnd for the batch. Needed for tail ingestion.
    * @param batch
    *   The batch of variable type.
    * @param batchSize
    *   Size of the batch measured in number of updates. Needed for metrics population.
    * @param offsetsUpdates
    *   The Updates with Offsets, the source of the batch.
    * @param activeContracts
    *   The active contracts at the head of the ledger - the ones which are not persisted yet. Key
    *   is the Synchronizer ID of activation and the Contract ID, and the value is the
    *   event_sequential_id of the activation.
    * @param missingDeactivatedActivations
    *   The set of deactivations need to be looked up at dbPrepare stage. It is optional as this is
    *   where the lookup-results are stored as well.
    * @param batchTraceContext
    *   The TraceContext constructed for the whole batch.
    */
  final case class Batch[+T](
      ledgerEnd: LedgerEnd,
      batch: T,
      batchSize: Int,
      offsetsUpdates: Vector[(Offset, Update)],
      activeContracts: mutable.LinkedHashMap[(SynchronizerId, ContractId), Long],
      missingDeactivatedActivations: Map[(SynchronizerId, ContractId), Option[Long]],
      batchTraceContext: TraceContext,
  )

  val ZeroLedgerEnd: LedgerEnd = LedgerEnd(
    lastOffset = Offset.MaxValue, // it will not be used, will be overridden
    lastEventSeqId =
      0L, // this is a property of interest in the zero element, we start the sequential ids at 1L
    lastStringInterningId =
      0, // this is a property of interest in the zero element, we start the string interning ids at 1
    lastPublicationTime =
      CantonTimestamp.MinValue, // this is a property of interest in the zero element: sets the lower bound for publication time, we start at MinValue
  )

  def monotonicityValidator(
      initialOffset: Option[Offset],
      loadPreviousState: SynchronizerId => Future[Option[SynchronizerIndex]],
  )(implicit logger: TracedLogger): Flow[(Offset, Update), (Offset, Update), NotUsed] = {
    implicit val directExecutionContext: ExecutionContext = DirectExecutionContext(logger)
    val stateRef = new AtomicReference[Map[SynchronizerId, SynchronizerIndex]](Map.empty)
    val lastOffset = new AtomicReference[Option[Offset]](initialOffset)

    def checkAndUpdateOffset(offset: Offset)(implicit traceContext: TraceContext): Unit = {
      assertMonotonicityCondition(
        lastOffset.get() < Some(offset),
        s"Monotonic Offset violation detected from ${lastOffset.get().getOrElse("participant begin")} to $offset",
      )
      lastOffset.set(Some(offset))
    }

    def checkAndUpdateSynchronizerIndex(offset: Offset)(
        synchronizerId: SynchronizerId,
        synchronizerIndex: SynchronizerIndex,
    )(implicit traceContext: TraceContext): Future[Unit] =
      stateRef
        .get()
        .get(synchronizerId)
        .map(Some(_))
        .map(Future.successful)
        .getOrElse(loadPreviousState(synchronizerId))
        .map { prevSynchronizerIndexO =>
          checkSynchronizerIndex(prevSynchronizerIndexO, synchronizerIndex, offset, synchronizerId)
          stateRef.set(
            stateRef
              .get()
              .updated(
                synchronizerId,
                prevSynchronizerIndexO.map(_ max synchronizerIndex).getOrElse(synchronizerIndex),
              )
          )
        }

    Flow[(Offset, Update)].mapAsync(1) { case (offset, update) =>
      implicit val traceContext: TraceContext = update.traceContext
      checkAndUpdateOffset(offset)

      Option(update)
        .collect { case synchronizerIndexUpdate: SynchronizerIndexUpdate =>
          synchronizerIndexUpdate.synchronizerIndex
        }
        .map(idAndIndex => checkAndUpdateSynchronizerIndex(offset)(idAndIndex._1, idAndIndex._2))
        .getOrElse(Future.unit)
        .map(_ => (offset, update))
    }
  }

  private def checkSynchronizerIndex(
      prevSynchronizerIndex: Option[SynchronizerIndex],
      synchronizerIndex: SynchronizerIndex,
      offset: Offset,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext, tracedLogger: TracedLogger): Unit =
    prevSynchronizerIndex match {
      case None => ()

      case Some(prevIndex) =>
        assertMonotonicityCondition(
          prevIndex.recordTime <= synchronizerIndex.recordTime,
          s"Monotonicity violation detected: record time decreases from ${prevIndex.recordTime} to ${synchronizerIndex.recordTime} at offset $offset and synchronizer $synchronizerId",
        )
        prevIndex.sequencerIndex.zip(synchronizerIndex.sequencerIndex).foreach {
          case (prevSeqIndex, currSeqIndex) =>
            assertMonotonicityCondition(
              prevSeqIndex.sequencerTimestamp < currSeqIndex.sequencerTimestamp,
              s"Monotonicity violation detected: sequencer timestamp did not increase from ${prevSeqIndex.sequencerTimestamp} to ${currSeqIndex.sequencerTimestamp} at offset $offset and synchronizer $synchronizerId",
            )
        }
        prevIndex.repairIndex.zip(synchronizerIndex.repairIndex).foreach {
          case (prevRepairIndex, currRepairIndex) =>
            assertMonotonicityCondition(
              prevRepairIndex <= currRepairIndex,
              s"Monotonicity violation detected: repair index decreases from $prevRepairIndex to $currRepairIndex at offset $offset and synchronizer $synchronizerId",
            )
        }
    }

  private def assertMonotonicityCondition(
      condition: Boolean,
      errorMessage: => String,
  )(implicit traceContext: TraceContext, tracedLogger: TracedLogger): Unit =
    if (!condition)
      ErrorUtil.invalidState(errorMessage)(ErrorLoggingContext.fromTracedLogger(tracedLogger))

  def inputMapper(
      metrics: LedgerApiServerMetrics,
      toDbDto: Offset => Update => Iterator[DbDto],
      eventMetricsUpdater: Iterable[(Offset, Update)] => Unit,
      logger: TracedLogger,
  ): Iterable[(Offset, Update)] => Batch[Vector[DbDto]] = { input =>
    metrics.indexer.inputMapping.batchSize.update(input.size)(MetricsContext.Empty)

    val batch = input.iterator.flatMap { case (offset, update) =>
      val prefix = update match {
        case _: Update.TransactionAccepted => "Phase 7: "
        case _ => ""
      }
      logger.info(
        s"${prefix}Storing at offset=${offset.unwrap} $update"
      )(update.traceContext)
      toDbDto(offset)(update)
    }.toVector

    eventMetricsUpdater(input)

    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    val last = input.last

    Batch(
      ledgerEnd = ZeroLedgerEnd.copy(
        lastOffset = last._1
        // the rest will be filled later in the sequential step
      ),
      batch = batch,
      batchSize = input.size,
      offsetsUpdates = input.toVector,
      activeContracts = EmptyActiveContracts, // will be overridden later
      missingDeactivatedActivations = Map.empty, // will be filled later
      batchTraceContext = TraceContext.ofBatch("indexer_update_batch")(
        input.iterator.map(_._2)
      )(logger),
    )
  }

  def seqMapperZero(
      initialLedgerEndO: Option[LedgerEnd]
  ): Batch[Vector[DbDto]] =
    Batch(
      ledgerEnd = initialLedgerEndO.getOrElse(ZeroLedgerEnd),
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
      activeContracts =
        mutable.LinkedHashMap.empty, // this mutable will propagate forward in sequential mapping
      missingDeactivatedActivations = Map.empty, // will be populated later
      batchTraceContext = TraceContext.empty, // will be populated later
    )

  def seqMapper(
      internize: Iterable[DbDto] => Iterable[(Int, String)],
      metrics: LedgerApiServerMetrics,
      clock: Clock,
      logger: TracedLogger,
      ledgerEndCache: LedgerEndCache,
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
            logger.info(
              s"Local participant clock at $now is before a previous publication time $next. Has the clock been reset, e.g., during participant failover?"
            )(current.batchTraceContext)
          }
          next
        }

        val activeContracts = previous.activeContracts
        val missingDeactivatedActivationsBuilder =
          Map.newBuilder[(SynchronizerId, ContractId), Option[Long]]
        def setActivation(synCon: (SynchronizerId, ContractId), eventSeqId: Long): Unit = {
          if (activeContracts.contains(synCon)) {
            logger.warn(
              s"Double activation at eventSeqId: $eventSeqId. Previous at ${activeContracts.get(synCon)} This should not happen"
            )(current.batchTraceContext)
            activeContracts.remove(synCon).discard // we will add a new now
          }
          activeContracts.addOne(synCon -> eventSeqId)
        }
        def tryToGetDeactivated(synCon: (SynchronizerId, ContractId)): Long =
          activeContracts.get(synCon) match {
            case Some(activationSeqId) =>
              activeContracts.remove(synCon).discard
              activationSeqId
            case None =>
              missingDeactivatedActivationsBuilder.addOne(synCon -> None)
              0 // will be filled later
          }

        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var eventSeqId = previous.ledgerEnd.lastEventSeqId
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var lastTransactionMetaEventSeqId = eventSeqId
        val batchWithSeqIdsAndPublicationTime = current.batch.map {
          case dbDto: DbDto.EventCreate =>
            eventSeqId += 1
            if (dbDto.flat_event_witnesses.nonEmpty) {
              // activation
              setActivation(
                dbDto.synchronizer_id -> dbDto.contract_id,
                eventSeqId,
              )
            }
            dbDto.copy(event_sequential_id = eventSeqId)

          case dbDto: DbDto.EventExercise =>
            eventSeqId += 1
            val deactivated = Option.when(dbDto.consuming && dbDto.flat_event_witnesses.nonEmpty) {
              // deactivation
              tryToGetDeactivated(dbDto.synchronizer_id -> dbDto.contract_id)
            }
            dbDto.copy(
              event_sequential_id = eventSeqId,
              deactivated_event_sequential_id = deactivated,
            )

          case dbDto: DbDto.EventUnassign =>
            eventSeqId += 1
            dbDto.copy(
              event_sequential_id = eventSeqId,
              deactivated_event_sequential_id = Some(
                tryToGetDeactivated(dbDto.source_synchronizer_id -> dbDto.contract_id)
              ),
            )

          case dbDto: DbDto.EventAssign =>
            eventSeqId += 1
            // activation
            setActivation(
              dbDto.target_synchronizer_id -> dbDto.contract_id,
              eventSeqId,
            )
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

        // prune active contracts so, that only activations remain which are not visible on DB yet
        ledgerEndCache().foreach(ledgerEnd =>
          activeContracts.iterator
            .takeWhile(_._2 <= ledgerEnd.lastEventSeqId)
            .map(_._1)
            .toList
            .foreach(activeContracts.remove(_).discard)
        )

        current.copy(
          ledgerEnd = current.ledgerEnd.copy(
            lastEventSeqId = eventSeqId,
            lastStringInterningId = newLastStringInterningId,
            lastPublicationTime = publicationTime,
          ),
          batch = dbDtosWithStringInterning,
          activeContracts = activeContracts,
          missingDeactivatedActivations = missingDeactivatedActivationsBuilder.result(),
        )
      },
    )

  def dbPrepare(
      lastActivations: Iterable[(SynchronizerId, ContractId)] => Connection => Map[
        (SynchronizerId, ContractId),
        Long,
      ],
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  ): Batch[Vector[DbDto]] => Future[Batch[Vector[DbDto]]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batch =>
      val missingActivations = batch.missingDeactivatedActivations.keys
      if (missingActivations.isEmpty) Future.successful(batch)
      else {
        implicit val loggingContextWithTrace: LoggingContextWithTrace =
          new LoggingContextWithTrace(LoggingEntries.empty, batch.batchTraceContext)
        dbDispatcher
          .executeSql(metrics.index.db.lookupLastActivationsDbMetrics)(
            lastActivations(missingActivations)
          )
          .map { results =>
            batch.copy(
              missingDeactivatedActivations = batch.missingDeactivatedActivations.++(
                results.iterator.map { case (synCon, eventSeqId) =>
                  synCon -> Some(eventSeqId)
                }
              )
            )
          }(directExecutionContext)
      }
  }

  def refillMissingDeactivatiedActivations(
      logger: TracedLogger
  )(batch: Batch[Vector[DbDto]]): Batch[Vector[DbDto]] = {
    def deactivationRefFor(
        synchronizerId: SynchronizerId,
        contractId: ContractId,
        marker: => String,
    ): Option[Long] =
      batch.missingDeactivatedActivations.get(synchronizerId -> contractId) match {
        case None =>
          ErrorUtil.invalidState(
            s"Programming error: deactivation reference is missing for $marker for synchronizerId:$synchronizerId contractId:$contractId, but lookup was not even initiated."
          )(ErrorLoggingContext.fromTracedLogger(logger)(batch.batchTraceContext))

        case Some(None) =>
          logger.warn(
            s"Activation is missing for a deactivation for $marker for synchronizerId:$synchronizerId contractId:$contractId."
          )(batch.batchTraceContext)
          None

        case Some(Some(deactivationReference)) => Some(deactivationReference)
      }
    val dbDtosWithDeactivationReferences = batch.batch.map {
      case unassign: DbDto.EventUnassign if unassign.deactivated_event_sequential_id.contains(0L) =>
        unassign.copy(
          deactivated_event_sequential_id = deactivationRefFor(
            unassign.source_synchronizer_id,
            unassign.contract_id,
            s"unassign event with offset:${unassign.event_offset} nodeId:${unassign.node_id}",
          )
        )

      case consumingExercise: DbDto.EventExercise
          if consumingExercise.deactivated_event_sequential_id.contains(0L) =>
        consumingExercise.copy(
          deactivated_event_sequential_id = deactivationRefFor(
            consumingExercise.synchronizer_id,
            consumingExercise.contract_id,
            s"consuming exercise event with offset:${consumingExercise.event_offset} nodeId:${consumingExercise.node_id}",
          )
        )

      case noChange => noChange
    }
    batch.copy(batch = dbDtosWithDeactivationReferences)
  }

  def batcher[DB_BATCH](
      batchF: Vector[DbDto] => DB_BATCH,
      logger: TracedLogger,
  )(inBatch: Batch[Vector[DbDto]]): Batch[DB_BATCH] = {
    val dbBatch = inBatch
      .pipe(refillMissingDeactivatiedActivations(logger))
      .batch
      .pipe(batchF)
    inBatch.copy(batch = dbBatch)
  }

  def ingester[DB_BATCH](
      ingestFunction: (Connection, DB_BATCH) => Unit,
      reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
      zeroDbBatch: DB_BATCH,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  ): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batch =>
      LoggingContextWithTrace.withNewLoggingContext(
        "updateOffsets" -> batch.offsetsUpdates.map(_._1)
      ) { implicit loggingContext =>
        val batchTraceContext: TraceContext = TraceContext.ofBatch("ingest_batch")(
          batch.offsetsUpdates.iterator.map(_._2)
        )(logger)
        reassignmentOffsetPersistence
          .persist(
            batch.offsetsUpdates,
            logger,
          )(batchTraceContext)
          .flatMap(_ =>
            dbDispatcher.executeSql(metrics.indexer.ingestion) { connection =>
              metrics.indexer.updates.inc(batch.batchSize.toLong)(MetricsContext.Empty)
              ingestFunction(connection, batch.batch)
              cleanUnusedBatch(zeroDbBatch)(batch)
            }
          )(directExecutionContext)
      }(batch.batchTraceContext)
  }

  def ledgerEndSynchronizerIndexFrom(
      synchronizerIndexes: Vector[(SynchronizerId, SynchronizerIndex)]
  ): Map[SynchronizerId, SynchronizerIndex] =
    synchronizerIndexes.groupMapReduce(_._1)(_._2)(_ max _)

  def ingestTail[DB_BATCH](
      storeLedgerEnd: (LedgerEnd, Map[SynchronizerId, SynchronizerIndex]) => Future[Unit],
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
            ledgerEndSynchronizerIndexFrom(
              batchOfBatches
                .flatMap(_.offsetsUpdates)
                .collect { case (_, update: SynchronizerIndexUpdate) =>
                  update.synchronizerIndex
                }
            ),
          ).map(_ => batchOfBatches)(directExecutionContext)

        case None =>
          val message = "Unexpectedly encountered a zero-sized batch in ingestTail"
          logger.error(message)
          Future.failed(new IllegalStateException(message))
      }
  }

  private def storeLedgerEnd(
      storeTailFunction: (LedgerEnd, Map[SynchronizerId, SynchronizerIndex]) => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): (LedgerEnd, Map[SynchronizerId, SynchronizerIndex]) => Future[Unit] =
    (ledgerEnd, ledgerEndSynchronizerIndexes) =>
      LoggingContextWithTrace.withNewLoggingContext("updateOffset" -> ledgerEnd.lastOffset) {
        implicit loggingContext =>
          def synchronizerIndexesLog: String = ledgerEndSynchronizerIndexes.toVector
            .sortBy(_._1.toProtoPrimitive)
            .map { case (synchronizerId, synchronizerIndex) =>
              s"${synchronizerId.toProtoPrimitive.take(20).mkString} -> $synchronizerIndex"
            }
            .mkString("synchronizer-indexes: [", ", ", "]")

          dbDispatcher.executeSql(metrics.indexer.tailIngestion) { connection =>
            storeTailFunction(ledgerEnd, ledgerEndSynchronizerIndexes)(connection)
            metrics.indexer.ledgerEndSequentialId
              .updateValue(ledgerEnd.lastEventSeqId)
            logger.debug(
              s"Ledger end updated in IndexDB $synchronizerIndexesLog ${loggingContext
                  .serializeFiltered("updateOffset")}."
            )(loggingContext.traceContext)
          }
      }

  def aggregateLedgerEndForRepair[DB_BATCH](
      aggregatedLedgerEnd: AtomicReference[
        Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]
      ]
  ): Vector[Batch[DB_BATCH]] => Unit =
    batchOfBatches =>
      batchOfBatches.lastOption.foreach(lastBatch =>
        discard(aggregatedLedgerEnd.updateAndGet { aggregated =>
          val oldSynchronizerIndexes =
            aggregated.fold(Vector.empty[(SynchronizerId, SynchronizerIndex)])(_._2.toVector)
          // this will also have at the end the offset bump for the CommitRepair Update as well, we accept this for sake of simplicity
          val newLedgerEnd = lastBatch.ledgerEnd
          val synchronizerIndexesForBatchOfBatches = batchOfBatches
            .flatMap(_.offsetsUpdates)
            .collect { case (_, update: SynchronizerIndexUpdate) =>
              update.synchronizerIndex
            }
          val newSynchronizerIndexes =
            ledgerEndSynchronizerIndexFrom(
              oldSynchronizerIndexes ++ synchronizerIndexesForBatchOfBatches
            )
          Some((newLedgerEnd, newSynchronizerIndexes))
        })
      )

  def postProcess[DB_BATCH](
      processor: (Vector[PostPublishData], TraceContext) => Future[Unit],
      logger: TracedLogger,
  ): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] = {
    val directExecutionContext = DirectExecutionContext(logger)
    batch => {
      val postPublishData = batch.offsetsUpdates.flatMap { case (offset, update) =>
        PostPublishData.from(
          update,
          offset,
          batch.ledgerEnd.lastPublicationTime,
        )
      }
      processor(postPublishData, batch.batchTraceContext).map(_ => batch)(directExecutionContext)
    }
  }

  def sequentialPostProcess[DB_BATCH](
      sequentialPostProcessor: Update => Unit
  ): Batch[DB_BATCH] => Batch[DB_BATCH] = { batch =>
    batch.offsetsUpdates.foreach { case (_, update) =>
      sequentialPostProcessor(update)
    }
    batch
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
          storePostProcessingEnd(lastBatch.ledgerEnd.lastOffset)
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
        dbDispatcher.executeSql(metrics.indexer.postProcessingEndIngestion) { connection =>
          storePostProcessEndFunction(offset)(connection)
          logger.debug(
            s"Post Processing end updated in IndexDB, ${loggingContext.serializeFiltered("updateOffset")}."
          )(loggingContext.traceContext)
        }
    }

  def commitRepair[DB_BATCH](
      storeLedgerEnd: (LedgerEnd, Map[SynchronizerId, SynchronizerIndex]) => Future[Unit],
      storePostProcessingEnd: Offset => Future[Unit],
      updateInMemoryState: LedgerEnd => Unit,
      aggregatedLedgerEnd: AtomicReference[
        Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]
      ],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Vector[(Offset, Update)] => Future[
    Vector[(Offset, Update)]
  ] = {
    implicit val ec: DirectExecutionContext = DirectExecutionContext(logger)
    offsetsAndUpdates =>
      offsetsAndUpdates.lastOption match {
        case Some((_, commitRepair: CommitRepair)) =>
          aggregatedLedgerEnd.get() match {
            case Some((ledgerEnd, synchronizerIndexes)) =>
              for {
                // this order is important to respect crash recovery rules
                _ <- storeLedgerEnd(ledgerEnd, synchronizerIndexes)
                _ <- storePostProcessingEnd(ledgerEnd.lastOffset)
                _ = updateInMemoryState(ledgerEnd)
                _ = commitRepair.persisted.trySuccess(())
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
      activeContracts = EmptyActiveContracts, // not used anymore
      missingDeactivatedActivations = Map.empty, // not used anymore
    )
}

trait ReassignmentOffsetPersistence {
  def persist(
      updates: Seq[(Offset, Update)],
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): Future[Unit]
}

object NoOpReassignmentOffsetPersistence extends ReassignmentOffsetPersistence {
  override def persist(
      updates: Seq[(Offset, Update)],
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful(())
}
