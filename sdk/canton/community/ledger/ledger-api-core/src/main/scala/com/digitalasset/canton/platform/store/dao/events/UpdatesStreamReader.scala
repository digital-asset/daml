// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.Spans
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.api.{TraceIdentifiers, TransactionShape}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.config.UpdatesStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawAcsDeltaEventLegacy,
  RawArchivedEventLegacy,
  RawCreatedEventLegacy,
  RawExercisedEventLegacy,
  RawLedgerEffectsEventLegacy,
}
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSourceLegacy,
  EventPayloadSourceForUpdatesAcsDeltaLegacy,
  EventPayloadSourceForUpdatesLedgerEffectsLegacy,
}
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.events.ReassignmentStreamReader.ReassignmentStreamQueryParams
import com.digitalasset.canton.platform.store.dao.events.TopologyTransactionsStreamReader.TopologyTransactionsStreamQueryParams
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  PaginatingAsyncStream,
}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
  Telemetry,
}
import com.digitalasset.canton.platform.{
  FatContract,
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class UpdatesStreamReader(
    config: UpdatesStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    contractStore: ContractStore,
    metrics: LedgerApiServerMetrics,
    tracer: Tracer,
    topologyTransactionsStreamReader: TopologyTransactionsStreamReader,
    reassignmentStreamReader: ReassignmentStreamReader,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import UpdateReader.*
  import config.*

  private val dbMetrics = metrics.index.db

  private val orderBySequentialEventIdFlat =
    Ordering.by[(Entry[RawAcsDeltaEventLegacy], Option[FatContract]), Long](_._1.eventSequentialId)

  private val orderBySequentialEventIdTree =
    Ordering.by[(Entry[RawLedgerEffectsEventLegacy], Option[FatContract]), Long](
      _._1.eventSequentialId
    )

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val directEC = DirectExecutionContext(logger)

  def streamUpdates(
      queryRange: EventsRange,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val span =
      Telemetry.Updates.createSpan(
        tracer,
        queryRange.startInclusiveOffset,
        queryRange.endInclusiveOffset,
      )(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamUpdates(${queryRange.startInclusiveOffset}, ${queryRange.endInclusiveOffset}, $internalUpdateFormat)"
    )
    doStreamUpdates(queryRange, internalUpdateFormat)
      .wireTap(_ match {
        case (_, getUpdatesResponse) =>
          getUpdatesResponse.update match {
            case GetUpdatesResponse.Update.Transaction(value) =>
              val event = tracing.Event("update", TraceIdentifiers.fromTransaction(value))
              Spans.addEventToSpan(event, span)
            case GetUpdatesResponse.Update.Reassignment(reassignment) =>
              val event =
                tracing.Event("update", TraceIdentifiers.fromReassignment(reassignment))
              Spans.addEventToSpan(event, span)
            case GetUpdatesResponse.Update.TopologyTransaction(topologyTransaction) =>
              val event = tracing
                .Event("update", TraceIdentifiers.fromTopologyTransaction(topologyTransaction))
              Spans.addEventToSpan(event, span)
            case _ => ()
          }
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamUpdates(
      queryRange: EventsRange,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {

    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)
    val deserializationQueriesLimiter =
      new QueueBasedConcurrencyLimiter(transactionsProcessingParallelism, executionContext)
    val txDecomposedFilters: Vector[DecomposedFilter] =
      internalUpdateFormat.includeTransactions
        .map(_.internalEventFormat.templatePartiesFilter)
        .toList
        .flatMap(txFilteringConstraints =>
          FilterUtils.decomposeFilters(txFilteringConstraints).toList
        )
        .toVector
    val numTxDecomposedFilters = txDecomposedFilters.size *
      internalUpdateFormat.includeTransactions.fold(0)(_.transactionShape match {
        // The ids for ledger effects transactions are retrieved from 5 separate id tables: (create stakeholder,
        // create non-stakeholder, exercise consuming stakeholder, exercise consuming non-stakeholder,
        // exercise non-consuming)
        case TransactionShape.LedgerEffects => 5
        // The ids for acs delta transactions are retrieved from 2 separate id tables: (create, archive)
        case TransactionShape.AcsDelta => 2
      })
    val reassignmentsDecomposedFilters: Seq[DecomposedFilter] =
      internalUpdateFormat.includeReassignments
        .map(_.templatePartiesFilter)
        .toList
        .flatMap(reassignmentsFilteringConstraints =>
          FilterUtils.decomposeFilters(reassignmentsFilteringConstraints).toList
        )
    // The ids for reassignments are retrieved from 2 separate id tables: (assign, unassign)
    val numReassignmentsDecomposedFilters = reassignmentsDecomposedFilters.size * 2
    // The ids for topology updates are retrieved from 1 id table: (party_to_participant)
    val numTopologyDecomposedFilters = internalUpdateFormat.includeTopologyEvents
      .flatMap(_.participantAuthorizationFormat)
      .fold(0)(_.parties.fold(1)(_.size))

    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      workingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages,
      numOfDecomposedFilters =
        numTxDecomposedFilters + numReassignmentsDecomposedFilters + numTopologyDecomposedFilters,
      numOfPagesInIdPageBuffer = maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    val sourceOfTransactions = internalUpdateFormat.includeTransactions match {
      case Some(InternalTransactionFormat(internalEventFormat, AcsDelta)) =>
        doStreamTxsAcsDelta(
          queryRange = queryRange,
          internalEventFormat = internalEventFormat,
          txDecomposedFilters = txDecomposedFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          deserializationQueriesLimiter = deserializationQueriesLimiter,
          idPageSizing = idPageSizing,
        )
      case Some(InternalTransactionFormat(internalEventFormat, LedgerEffects)) =>
        doStreamTxsLedgerEffects(
          queryRange = queryRange,
          internalEventFormat = internalEventFormat,
          txDecomposedFilters = txDecomposedFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          deserializationQueriesLimiter = deserializationQueriesLimiter,
          idPageSizing = idPageSizing,
        )
      case None => Source.empty
    }

    val topologyTransactions =
      internalUpdateFormat.includeTopologyEvents.flatMap(_.participantAuthorizationFormat) match {
        case Some(participantAuthorizationFormat) =>
          topologyTransactionsStreamReader
            .streamTopologyTransactions(
              TopologyTransactionsStreamQueryParams(
                queryRange = queryRange,
                payloadQueriesLimiter = payloadQueriesLimiter,
                idPageSizing = idPageSizing,
                participantAuthorizationFormat = participantAuthorizationFormat,
                maxParallelIdQueries = maxParallelIdTopologyEventsQueries,
                maxPagesPerIdPagesBuffer = maxPayloadsPerPayloadsPage,
                maxPayloadsPerPayloadsPage = maxParallelPayloadTopologyEventsQueries,
                maxParallelPayloadQueries = transactionsProcessingParallelism,
              )
            )
            .map { case (offset, topologyTransaction) =>
              offset -> GetUpdatesResponse(
                GetUpdatesResponse.Update.TopologyTransaction(topologyTransaction)
              )
            }
        case None => Source.empty
      }

    val reassignments =
      internalUpdateFormat.includeReassignments match {
        case Some(
              InternalEventFormat(
                reassignmentFilteringConstraints: TemplatePartiesFilter,
                reassignmentEventProjectionProperties: EventProjectionProperties,
              )
            ) =>
          reassignmentStreamReader
            .streamReassignments(
              ReassignmentStreamQueryParams(
                queryRange = queryRange,
                filteringConstraints = reassignmentFilteringConstraints,
                eventProjectionProperties = reassignmentEventProjectionProperties,
                payloadQueriesLimiter = payloadQueriesLimiter,
                deserializationQueriesLimiter = deserializationQueriesLimiter,
                idPageSizing = idPageSizing,
                decomposedFilters =
                  FilterUtils.decomposeFilters(reassignmentFilteringConstraints).toVector,
                maxParallelIdAssignQueries = maxParallelIdAssignQueries,
                maxParallelIdUnassignQueries = maxParallelIdUnassignQueries,
                maxPagesPerIdPagesBuffer = maxPagesPerIdPagesBuffer,
                maxPayloadsPerPayloadsPage = maxPayloadsPerPayloadsPage,
                maxParallelPayloadAssignQueries = maxParallelPayloadAssignQueries,
                maxParallelPayloadUnassignQueries = maxParallelPayloadUnassignQueries,
                deserializationProcessingParallelism = transactionsProcessingParallelism,
              )
            )
            .map { case (offset, reassignment) =>
              offset -> GetUpdatesResponse(
                GetUpdatesResponse.Update.Reassignment(reassignment)
              )
            }
        case None => Source.empty
      }

    sourceOfTransactions
      .mergeSorted(topologyTransactions.map { case (offset, response) =>
        offset -> response
      })(Ordering.by(_._1))
      .mergeSorted(reassignments.map { case (offset, response) =>
        offset -> response
      })(Ordering.by(_._1))

  }

  private def doStreamTxsAcsDelta(
      queryRange: EventsRange,
      internalEventFormat: InternalEventFormat,
      txDecomposedFilters: Vector[DecomposedFilter],
      payloadQueriesLimiter: QueueBasedConcurrencyLimiter,
      deserializationQueriesLimiter: QueueBasedConcurrencyLimiter,
      idPageSizing: IdPageSizing,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val createEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdCreateQueries, executionContext)
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdConsumingQueries, executionContext)
    val txFilteringConstraints = internalEventFormat.templatePartiesFilter

    def fetchIdsSorted(
        txDecomposedFilters: Vector[DecomposedFilter],
        target: EventIdSourceLegacy,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        maxOutputBatchCount: Int,
        metric: DatabaseMetrics,
    ): Source[Iterable[Long], NotUsed] =
      txDecomposedFilters
        .map { filter =>
          fetchIds(queryRange, filter, target, idPageSizing, maxParallelIdQueriesLimiter, metric)
        }
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxOutputBatchCount,
          )
        )

    val idsCreate =
      fetchIdsSorted(
        txDecomposedFilters = txDecomposedFilters,
        target = EventIdSourceLegacy.CreateStakeholder,
        maxParallelIdQueriesLimiter = createEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadCreateQueries + 1,
        metric = dbMetrics.updatesAcsDeltaStream.fetchEventCreateIdsStakeholder,
      )
    val idsConsuming =
      fetchIdsSorted(
        txDecomposedFilters = txDecomposedFilters,
        target = EventIdSourceLegacy.ConsumingStakeholder,
        maxParallelIdQueriesLimiter = consumingEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadConsumingQueries + 1,
        metric = dbMetrics.updatesAcsDeltaStream.fetchEventConsumingIdsStakeholder,
      )

    def getInternalContractIdFromCreated(event: RawAcsDeltaEventLegacy): Long = event match {
      case created: RawCreatedEventLegacy => created.internalContractId
      case _: RawArchivedEventLegacy =>
        throw new IllegalStateException(
          s"archived event should not be used to lookup a contract"
        )
    }

    val payloadsCreate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsCreate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend.fetchEventPayloadsAcsDeltaLegacy(target =
            EventPayloadSourceForUpdatesAcsDeltaLegacy.Create
          )(
            eventSequentialIds = Ids(ids),
            requestingParties = txFilteringConstraints.allFilterParties,
          )(connection),
        maxParallelPayloadQueries = maxParallelPayloadCreateQueries,
        dbMetric = dbMetrics.updatesAcsDeltaStream.fetchEventCreatePayloadsLegacy,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        getInternalContractIdO = Some(getInternalContractIdFromCreated),
      )
    val payloadsConsuming =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsConsuming,
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsAcsDeltaLegacy(target =
              EventPayloadSourceForUpdatesAcsDeltaLegacy.Consuming
            )(
              eventSequentialIds = Ids(ids),
              requestingParties = txFilteringConstraints.allFilterParties,
            )(connection),
        maxParallelPayloadQueries = maxParallelPayloadConsumingQueries,
        dbMetric = dbMetrics.updatesAcsDeltaStream.fetchEventConsumingPayloadsLegacy,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        getInternalContractIdO = None,
      )
    val allSortedPayloads =
      payloadsConsuming.mergeSorted(payloadsCreate)(orderBySequentialEventIdFlat)
    UpdateReader
      .groupContiguous(allSortedPayloads)(by = _._1.updateId)
      .mapAsync(transactionsProcessingParallelism)(rawEvents =>
        deserializationQueriesLimiter.execute(
          deserializeLfValues(rawEvents, internalEventFormat.eventProjectionProperties)
        )
      )
      .mapConcat { (groupOfPayloads: Seq[Entry[Event]]) =>
        val responses = TransactionConversions.toGetTransactionsResponse(
          events = groupOfPayloads,
          transactionShape = AcsDelta,
        )
        responses.map { case (offset, response) => Offset.tryFromLong(offset) -> response }
      }
  }

  private def doStreamTxsLedgerEffects(
      queryRange: EventsRange,
      internalEventFormat: InternalEventFormat,
      txDecomposedFilters: Vector[DecomposedFilter],
      payloadQueriesLimiter: QueueBasedConcurrencyLimiter,
      deserializationQueriesLimiter: QueueBasedConcurrencyLimiter,
      idPageSizing: IdPageSizing,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val createEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdCreateQueries, executionContext)
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdConsumingQueries, executionContext)
    val nonConsumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdNonConsumingQueries, executionContext)
    val txFilteringConstraints = internalEventFormat.templatePartiesFilter

    val idsCreate =
      (txDecomposedFilters.map(filter =>
        fetchIds(
          queryRange = queryRange,
          filter = filter,
          idPageSizing = idPageSizing,
          target = EventIdSourceLegacy.CreateStakeholder,
          maxParallelIdQueriesLimiter = createEventIdQueriesLimiter,
          metric = dbMetrics.updatesLedgerEffectsStream.fetchEventCreateIdsStakeholderLegacy,
        )
      ) ++ txDecomposedFilters.map(filter =>
        fetchIds(
          queryRange = queryRange,
          filter = filter,
          idPageSizing = idPageSizing,
          target = EventIdSourceLegacy.CreateNonStakeholder,
          maxParallelIdQueriesLimiter = createEventIdQueriesLimiter,
          metric = dbMetrics.updatesLedgerEffectsStream.fetchEventCreateIdsNonStakeholderLegacy,
        )
      )).pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadCreateQueries + 1,
        )
      )
    val idsConsuming =
      (txDecomposedFilters.map(filter =>
        fetchIds(
          queryRange = queryRange,
          filter = filter,
          target = EventIdSourceLegacy.ConsumingStakeholder,
          idPageSizing = idPageSizing,
          maxParallelIdQueriesLimiter = consumingEventIdQueriesLimiter,
          metric = dbMetrics.updatesLedgerEffectsStream.fetchEventConsumingIdsStakeholderLegacy,
        )
      ) ++ txDecomposedFilters.map(filter =>
        fetchIds(
          queryRange = queryRange,
          filter = filter,
          target = EventIdSourceLegacy.ConsumingNonStakeholder,
          idPageSizing = idPageSizing,
          maxParallelIdQueriesLimiter = consumingEventIdQueriesLimiter,
          metric = dbMetrics.updatesLedgerEffectsStream.fetchEventConsumingIdsNonStakeholderLegacy,
        )
      )).pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadConsumingQueries + 1,
        )
      )
    val idsNonConsuming = txDecomposedFilters
      .map(filter =>
        fetchIds(
          queryRange = queryRange,
          filter = filter,
          target = EventIdSourceLegacy.NonConsumingInformee,
          idPageSizing = idPageSizing,
          maxParallelIdQueriesLimiter = nonConsumingEventIdQueriesLimiter,
          metric = dbMetrics.updatesLedgerEffectsStream.fetchEventNonConsumingIdsLegacy,
        )
      )
      .pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadNonConsumingQueries + 1,
        )
      )

    def fetchEventPayloadsLedgerEffects(
        target: EventPayloadSourceForUpdatesLedgerEffectsLegacy
    )(ids: Iterable[Long], connection: Connection): Vector[Entry[RawLedgerEffectsEventLegacy]] =
      eventStorageBackend.fetchEventPayloadsLedgerEffectsLegacy(
        target = target
      )(
        eventSequentialIds = Ids(ids),
        requestingParties = txFilteringConstraints.allFilterParties,
      )(connection)

    def getInternalContractIdFromCreated(event: RawLedgerEffectsEventLegacy): Long = event match {
      case created: RawCreatedEventLegacy => created.internalContractId
      case _: RawExercisedEventLegacy =>
        throw new IllegalStateException(
          s"exercised event should not be used to lookup a contract"
        )
    }

    val payloadsCreate = fetchPayloads(
      queryRange = queryRange,
      ids = idsCreate,
      fetchEvents =
        fetchEventPayloadsLedgerEffects(EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create),
      maxParallelPayloadQueries = maxParallelPayloadCreateQueries,
      dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventCreatePayloads,
      payloadQueriesLimiter = payloadQueriesLimiter,
      contractStore = contractStore,
      getInternalContractIdO = Some(getInternalContractIdFromCreated),
    )
    val payloadsConsuming = fetchPayloads(
      queryRange = queryRange,
      ids = idsConsuming,
      fetchEvents =
        fetchEventPayloadsLedgerEffects(EventPayloadSourceForUpdatesLedgerEffectsLegacy.Consuming),
      maxParallelPayloadQueries = maxParallelPayloadConsumingQueries,
      dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventConsumingPayloads,
      payloadQueriesLimiter = payloadQueriesLimiter,
      contractStore = contractStore,
      getInternalContractIdO = None,
    )
    val payloadsNonConsuming = fetchPayloads(
      queryRange = queryRange,
      ids = idsNonConsuming,
      fetchEvents = fetchEventPayloadsLedgerEffects(
        EventPayloadSourceForUpdatesLedgerEffectsLegacy.NonConsuming
      ),
      maxParallelPayloadQueries = maxParallelPayloadNonConsumingQueries,
      dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventNonConsumingPayloads,
      payloadQueriesLimiter = payloadQueriesLimiter,
      contractStore = contractStore,
      getInternalContractIdO = None,
    )
    val allSortedPayloads = payloadsConsuming
      .mergeSorted(payloadsCreate)(orderBySequentialEventIdTree)
      .mergeSorted(payloadsNonConsuming)(orderBySequentialEventIdTree)
    UpdateReader
      .groupContiguous(allSortedPayloads)(by = _._1.updateId)
      .mapAsync(transactionsProcessingParallelism)(rawEvents =>
        deserializationQueriesLimiter.execute(
          deserializeLfValuesTree(rawEvents, internalEventFormat.eventProjectionProperties)
        )
      )
      .mapConcat { events =>
        val responses =
          TransactionConversions.toGetTransactionsResponse(
            events = events,
            transactionShape = LedgerEffects,
          )
        responses.map { case (offset, response) => Offset.tryFromLong(offset) -> response }
      }
  }

  private def fetchIds(
      queryRange: EventsRange,
      filter: DecomposedFilter,
      target: EventIdSourceLegacy,
      idPageSizing: IdPageSizing,
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      metric: DatabaseMetrics,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[Long, NotUsed] =
    paginatingAsyncStream.streamIdsFromSeekPaginationWithoutIdFilter(
      idStreamName = s"Update IDs for $target $filter",
      idPageSizing = idPageSizing,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
      initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
      initialEndInclusive = queryRange.endInclusiveEventSeqId,
    )(
      eventStorageBackend.updateStreamingQueries.fetchEventIdsLegacy(
        target = target
      )(
        stakeholderO = filter.party,
        templateIdO = filter.templateId,
      )
    )(
      executeIdQuery = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metric)(f)
          }
        }
    )

  private def mergeSortAndBatch(
      maxOutputBatchSize: Int,
      maxOutputBatchCount: Int,
  )(sourcesOfIds: Vector[Source[Long, NotUsed]]): Source[Iterable[Long], NotUsed] =
    EventIdsUtils
      .sortAndDeduplicateIds(sourcesOfIds)
      .batchN(
        maxBatchSize = maxOutputBatchSize,
        maxBatchCount = maxOutputBatchCount,
      )

  private def fetchPayloads[T](
      queryRange: EventsRange,
      ids: Source[Iterable[Long], NotUsed],
      fetchEvents: (Iterable[Long], Connection) => Vector[Entry[T]],
      maxParallelPayloadQueries: Int,
      dbMetric: DatabaseMetrics,
      payloadQueriesLimiter: ConcurrencyLimiter,
      contractStore: ContractStore,
      getInternalContractIdO: Option[T => Long],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Entry[T], Option[FatContract]), NotUsed] = {
    // Pekko requires for this buffer's size to be a power of two.
    val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
    ids
      .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
      .mapAsync(maxParallelPayloadQueries)(ids =>
        payloadQueriesLimiter.execute {
          globalPayloadQueriesLimiter.execute {
            queryValidRange.withRangeNotPruned(
              minOffsetInclusive = queryRange.startInclusiveOffset,
              maxOffsetInclusive = queryRange.endInclusiveOffset,
              errorPruning = (prunedOffset: Offset) =>
                s"Updates request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
              errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                s"Updates request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                    .fold(0L)(_.unwrap)}",
            ) {
              dbDispatcher
                .executeSql(dbMetric) { connection =>
                  fetchEvents(ids, connection)
                }
                .flatMap(events =>
                  getInternalContractIdO match {
                    case Some(getInternalContractId) =>
                      val internalContractIds =
                        events.map(entry => getInternalContractId(entry.event))
                      for {
                        contractsM <- contractStore
                          .lookupBatchedNonCached(internalContractIds)
                          .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
                      } yield events.map { entry =>
                        entry -> contractsM
                          .get(getInternalContractId(entry.event))
                          .map(_.inst)
                      }
                    case None =>
                      Future.successful(events.map(_ -> None))
                  }
                )
            }
          }
        }
      )
      .mapConcat(identity)
  }

  private def deserializeLfValuesTree(
      rawEvents: Vector[(Entry[RawLedgerEffectsEventLegacy], Option[FatContract])],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[Seq[Entry[Event]]] =
    Timed.future(
      future = Future.delegate {
        implicit val executionContext: ExecutionContext =
          directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
        MonadUtil.sequentialTraverse(rawEvents)(
          UpdateReader
            .deserializeRawLedgerEffectsEvent(eventProjectionProperties, lfValueTranslation)
        )
      },
      timer = dbMetrics.updatesLedgerEffectsStream.translationTimer,
    )

  private def deserializeLfValues(
      rawEvents: Vector[(Entry[RawAcsDeltaEventLegacy], Option[FatContract])],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[Seq[Entry[Event]]] =
    Timed.future(
      future = Future.delegate {
        implicit val executionContext: ExecutionContext =
          directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
        MonadUtil.sequentialTraverse(rawEvents)(
          UpdateReader.deserializeRawAcsDeltaEvent(eventProjectionProperties, lfValueTranslation)
        )
      },
      timer = dbMetrics.updatesAcsDeltaStream.translationTimer,
    )

}
