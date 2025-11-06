// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.metrics.DatabaseMetrics
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.Spans
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.api.{TraceIdentifiers, TransactionShape}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.config.UpdatesStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{RawEvent, RawThinEvent}
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSource,
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, PersistentEventType}
import com.digitalasset.canton.platform.store.dao.events.TopologyTransactionsStreamReader.TopologyTransactionsStreamQueryParams
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
  Telemetry,
}
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.ExecutionContext
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
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import UpdateReader.*
  import config.*

  private val dbMetrics = metrics.index.db

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

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
    val txFilters: Set[DecomposedFilter] =
      internalUpdateFormat.includeTransactions
        .map(_.internalEventFormat.templatePartiesFilter)
        .toList
        .flatMap(txFilteringConstraints =>
          FilterUtils.decomposeFilters(txFilteringConstraints).toList
        )
        .toSet
    val reassignmentsFilters: Set[DecomposedFilter] =
      internalUpdateFormat.includeReassignments
        .map(_.templatePartiesFilter)
        .toList
        .flatMap(reassignmentsFilteringConstraints =>
          FilterUtils.decomposeFilters(reassignmentsFilteringConstraints).toList
        )
        .toSet
    val txAndReassignmentFilters =
      txFilters.filter(reassignmentsFilters)
    val justTxFilters =
      txFilters.filterNot(txAndReassignmentFilters)
    val justReassignmentFilters =
      reassignmentsFilters.filterNot(txAndReassignmentFilters)
    val numTxAndReassignmentFilters = txAndReassignmentFilters.size *
      internalUpdateFormat.includeTransactions.fold(0)(_.transactionShape match {
        // The ids for ledger effects transactions are retrieved from 5 separate id tables: (activate stakeholder,
        // activate witness, deactivate stakeholder, deactivate witness, various witnessed)
        case TransactionShape.LedgerEffects => 5
        // The ids for acs delta transactions are retrieved from 2 separate id tables: (activate stakeholder, deactivate stakeholder)
        case TransactionShape.AcsDelta => 2
      })
    val numJustTxFilters = justTxFilters.size *
      internalUpdateFormat.includeTransactions.fold(0)(_.transactionShape match {
        // The ids for ledger effects transactions are retrieved from 5 separate id tables: (activate stakeholder,
        // activate witness, deactivate stakeholder, deactivate witness, various witnessed)
        case TransactionShape.LedgerEffects => 5
        // The ids for acs delta transactions are retrieved from 2 separate id tables: (activate stakeholder, deactivate stakeholder)
        case TransactionShape.AcsDelta => 2
      })
    // The ids for reassignments are retrieved from 2 separate id tables: (assign stakeholder, unassign stakeholder)
    val numJustReassignmentsFilters = justReassignmentFilters.size * 2
    // The ids for topology updates are retrieved from 1 id table: (party_to_participant)
    val numTopologyDecomposedFilters = internalUpdateFormat.includeTopologyEvents
      .flatMap(_.participantAuthorizationFormat)
      .fold(0)(_.parties.fold(1)(_.size))

    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      workingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages,
      numOfDecomposedFilters = numTxAndReassignmentFilters +
        numJustTxFilters +
        numJustReassignmentsFilters +
        numTopologyDecomposedFilters,
      numOfPagesInIdPageBuffer = maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    val sourceOfTransactionsAndReassignments = internalUpdateFormat.includeTransactions match {
      case Some(InternalTransactionFormat(internalEventFormat, AcsDelta)) =>
        doStreamAcsDelta(
          queryRange = queryRange,
          txInternalEventFormat = Some(internalEventFormat),
          reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments,
          txAndReassignmentFilters = txAndReassignmentFilters,
          justTxFilters = justTxFilters,
          justReassignmentFilters = justReassignmentFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          idPageSizing = idPageSizing,
        )
      case Some(InternalTransactionFormat(internalEventFormat, LedgerEffects)) =>
        doStreamLedgerEffects(
          queryRange = queryRange,
          txInternalEventFormat = Some(internalEventFormat),
          reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments,
          txAndReassignmentFilters = txAndReassignmentFilters,
          justTxFilters = justTxFilters,
          justReassignmentFilters = justReassignmentFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          idPageSizing = idPageSizing,
        )
      case None if internalUpdateFormat.includeReassignments.isDefined =>
        doStreamAcsDelta(
          queryRange = queryRange,
          txInternalEventFormat = None,
          reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments,
          txAndReassignmentFilters = txAndReassignmentFilters,
          justTxFilters = justTxFilters,
          justReassignmentFilters = justReassignmentFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
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

    UpdateReader
      .groupContiguous(sourceOfTransactionsAndReassignments)(_.offset)
      .mapAsync(transactionsProcessingParallelism) { rawEvents =>
        deserializationQueriesLimiter.execute(
          UpdateReader.toApiUpdate(
            reassignmentEventProjectionProperties = internalUpdateFormat.includeReassignments.map(
              _.eventProjectionProperties
            ),
            transactionEventProjectionProperties = internalUpdateFormat.includeTransactions.map(
              _.internalEventFormat.eventProjectionProperties
            ),
            lfValueTranslation = lfValueTranslation,
          )(rawEvents)(
            convertReassignment = reassignment =>
              Offset.tryFromLong(reassignment.offset) -> GetUpdatesResponse(
                GetUpdatesResponse.Update.Reassignment(reassignment)
              ),
            convertTransaction = transaction =>
              Offset.tryFromLong(transaction.offset) -> GetUpdatesResponse(
                GetUpdatesResponse.Update.Transaction(transaction)
              ),
          )
        )
      }
      .mapConcat(identity)
      .mergeSorted(topologyTransactions)(Ordering.by(_._1))
  }

  private def doStreamAcsDelta(
      queryRange: EventsRange,
      txInternalEventFormat: Option[InternalEventFormat],
      reassignmentInternalEventFormat: Option[InternalEventFormat],
      txAndReassignmentFilters: Set[DecomposedFilter],
      justTxFilters: Set[DecomposedFilter],
      justReassignmentFilters: Set[DecomposedFilter],
      payloadQueriesLimiter: QueueBasedConcurrencyLimiter,
      idPageSizing: IdPageSizing,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawEvent, NotUsed] = {
    val activateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdActivateQueries, executionContext)
    val deactivateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdDeactivateQueries, executionContext)

    val idsActivate =
      fetchIdsSorted(
        txDecomposedFilters = txAndReassignmentFilters.iterator
          .map(_ -> Set.empty[PersistentEventType])
          .++(
            justTxFilters.iterator.map(_ -> Set[PersistentEventType](PersistentEventType.Create))
          )
          .++(
            justReassignmentFilters.iterator
              .map(_ -> Set[PersistentEventType](PersistentEventType.Assign))
          )
          .toVector,
        target = EventIdSource.ActivateStakeholder,
        maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadActivateQueries + 1,
        metricNonFiltered = dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholder,
        metricFilteredLast =
          dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholderFilteredRange,
        metricFilteredIds =
          dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholderFilteredIds,
        queryRange = queryRange,
        idPageSizing = idPageSizing,
      )
    val idsDeactivate =
      fetchIdsSorted(
        txDecomposedFilters = txAndReassignmentFilters.iterator
          .map(_ -> Set.empty[PersistentEventType])
          .++(
            justTxFilters.iterator.map(
              _ -> Set[PersistentEventType](PersistentEventType.ConsumingExercise)
            )
          )
          .++(
            justReassignmentFilters.iterator
              .map(_ -> Set[PersistentEventType](PersistentEventType.Unassign))
          )
          .toVector,
        target = EventIdSource.DeactivateStakeholder,
        maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadDeactivateQueries + 1,
        metricNonFiltered = dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholder,
        metricFilteredLast =
          dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholderFilteredRange,
        metricFilteredIds =
          dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholderFilteredIds,
        queryRange = queryRange,
        idPageSizing = idPageSizing,
      )

    val payloadsActivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsActivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend.fetchEventPayloadsAcsDelta(
            EventPayloadSourceForUpdatesAcsDelta.Activate
          )(
            eventSequentialIds = Ids(ids),
            requestingPartiesForTx =
              txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            requestingPartiesForReassignment =
              reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
          )(connection),
        maxParallelPayloadQueries = maxParallelPayloadActivateQueries,
        dbMetric = dbMetrics.updatesAcsDeltaStream.fetchEventActivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
      )
    val payloadsDeactivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsDeactivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend.fetchEventPayloadsAcsDelta(
            EventPayloadSourceForUpdatesAcsDelta.Deactivate
          )(
            eventSequentialIds = Ids(ids),
            requestingPartiesForTx =
              txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            requestingPartiesForReassignment =
              reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
          )(connection),
        maxParallelPayloadQueries = maxParallelPayloadActivateQueries,
        dbMetric = dbMetrics.updatesAcsDeltaStream.fetchEventDeactivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
      )

    payloadsActivate
      .mergeSorted(payloadsDeactivate)(Ordering.by(_.eventSeqId))
  }

  private def doStreamLedgerEffects(
      queryRange: EventsRange,
      txInternalEventFormat: Option[InternalEventFormat],
      reassignmentInternalEventFormat: Option[InternalEventFormat],
      txAndReassignmentFilters: Set[DecomposedFilter],
      justTxFilters: Set[DecomposedFilter],
      justReassignmentFilters: Set[DecomposedFilter],
      payloadQueriesLimiter: QueueBasedConcurrencyLimiter,
      idPageSizing: IdPageSizing,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawEvent, NotUsed] = {
    val activateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdActivateQueries, executionContext)
    val deactivateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdDeactivateQueries, executionContext)
    val variousWitnessedEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdVariousWitnessedQueries, executionContext)

    val idsActivate =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            filter = filter,
            target = EventIdSource.ActivateStakeholder,
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
            metric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              filter = filter,
              eventTypes = Set(PersistentEventType.Create),
              target = EventIdSource.ActivateStakeholder,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredIds,
            )
          )
        )
        .++(
          justReassignmentFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              filter = filter,
              eventTypes = Set(PersistentEventType.Assign),
              target = EventIdSource.ActivateStakeholder,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredIds,
            )
          )
        )
        .++(
          txAndReassignmentFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              filter = filter,
              target = EventIdSource.ActivateWitnesses,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsWitness,
            )
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              filter = filter,
              target = EventIdSource.ActivateWitnesses,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsWitness,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadActivateQueries + 1,
          )
        )
    val idsDeactivate =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            filter = filter,
            target = EventIdSource.DeactivateStakeholder,
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
            metric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              filter = filter,
              eventTypes = Set(PersistentEventType.ConsumingExercise),
              target = EventIdSource.DeactivateStakeholder,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredIds,
            )
          )
        )
        .++(
          justReassignmentFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              filter = filter,
              eventTypes = Set(PersistentEventType.Unassign),
              target = EventIdSource.DeactivateStakeholder,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredIds,
            )
          )
        )
        .++(
          txAndReassignmentFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              filter = filter,
              target = EventIdSource.DeactivateWitnesses,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsWitness,
            )
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              filter = filter,
              target = EventIdSource.DeactivateWitnesses,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsWitness,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadDeactivateQueries + 1,
          )
        )
    val idsVariousWitnessed =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            filter = filter,
            target = EventIdSource.VariousWitnesses,
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = variousWitnessedEventIdQueriesLimiter,
            metric = dbMetrics.updatesLedgerEffectsStream.fetchEventVariousIdsWitness,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              filter = filter,
              target = EventIdSource.VariousWitnesses,
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = variousWitnessedEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventVariousIdsWitness,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadVariousWitnessedQueries + 1,
          )
        )

    val payloadsActivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsActivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.Activate
          )(
            eventSequentialIds = Ids(ids),
            requestingPartiesForTx =
              txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            requestingPartiesForReassignment =
              reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
          )(connection),
        maxParallelPayloadQueries = maxParallelPayloadActivateQueries,
        dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
      )
    val payloadsDeactivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsDeactivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.Deactivate
          )(
            eventSequentialIds = Ids(ids),
            requestingPartiesForTx =
              txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            requestingPartiesForReassignment =
              reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
          )(connection),
        maxParallelPayloadQueries = maxParallelPayloadDeactivateQueries,
        dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
      )
    val payloadsVariousWitnessed =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsVariousWitnessed,
        fetchEvents = (ids, connection) =>
          eventStorageBackend.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
          )(
            eventSequentialIds = Ids(ids),
            requestingPartiesForTx =
              txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            requestingPartiesForReassignment =
              reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
          )(connection),
        maxParallelPayloadQueries = maxParallelPayloadVariousWitnessedQueries,
        dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventVariousWitnessedPayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
      )

    payloadsActivate
      .mergeSorted(payloadsDeactivate)(Ordering.by(_.eventSeqId))
      .mergeSorted(payloadsVariousWitnessed)(Ordering.by(_.eventSeqId))
  }

  private def fetchIdsSorted(
      txDecomposedFilters: Vector[(DecomposedFilter, Set[PersistentEventType])],
      target: EventIdSource,
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      maxOutputBatchCount: Int,
      metricNonFiltered: DatabaseMetrics,
      metricFilteredLast: DatabaseMetrics,
      metricFilteredIds: DatabaseMetrics,
      queryRange: EventsRange,
      idPageSizing: IdPageSizing,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Source[Iterable[Long], NotUsed] =
    txDecomposedFilters
      .map {
        case (filter, eventTypes) if eventTypes.isEmpty =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            filter = filter,
            target = target,
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = maxParallelIdQueriesLimiter,
            metric = metricNonFiltered,
          )

        case (filter, eventTypes) =>
          fetchIdsFiltered(
            queryRange = queryRange,
            filter = filter,
            eventTypes = eventTypes,
            target = target,
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = maxParallelIdQueriesLimiter,
            metricForLast = metricFilteredLast,
            metricFiltered = metricFilteredIds,
          )

      }
      .pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxOutputBatchCount,
        )
      )

  private def fetchIdsNonFiltered(
      queryRange: EventsRange,
      filter: DecomposedFilter,
      target: EventIdSource,
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
      eventStorageBackend.updateStreamingQueries.fetchEventIds(
        target = target
      )(
        witnessO = filter.party,
        templateIdO = filter.templateId,
        eventTypes = Set.empty,
      )
    )(
      executeIdQuery = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metric)(f)
          }
        }
    )

  private def fetchIdsFiltered(
      queryRange: EventsRange,
      filter: DecomposedFilter,
      eventTypes: Set[PersistentEventType],
      target: EventIdSource,
      idPageSizing: IdPageSizing,
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      metricForLast: DatabaseMetrics,
      metricFiltered: DatabaseMetrics,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[Long, NotUsed] =
    paginatingAsyncStream.streamIdsFromSeekPaginationWithIdFilter(
      idStreamName = s"Update IDs for $target $filter",
      idPageSizing = idPageSizing,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
      initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
      initialEndInclusive = queryRange.endInclusiveEventSeqId,
    )(
      eventStorageBackend.updateStreamingQueries.fetchEventIds(
        target = target
      )(
        witnessO = filter.party,
        templateIdO = filter.templateId,
        eventTypes = eventTypes,
      )
    )(
      executeLastIdQuery = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metricForLast)(f)
          }
        },
      idFilterQueryParallelism = idFilterQueryParallelism,
      executeIdFilterQuery = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metricFiltered)(f)
          }
        },
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

  private def fetchPayloads(
      queryRange: EventsRange,
      ids: Source[Iterable[Long], NotUsed],
      fetchEvents: (Iterable[Long], Connection) => Vector[RawThinEvent],
      maxParallelPayloadQueries: Int,
      dbMetric: DatabaseMetrics,
      payloadQueriesLimiter: ConcurrencyLimiter,
      contractStore: ContractStore,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawEvent, NotUsed] = {
    // Pekko requires for this buffer's size to be a power of two.
    val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
    ids
      .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
      .mapAsync(maxParallelPayloadQueries)(ids =>
        payloadQueriesLimiter.execute {
          globalPayloadQueriesLimiter.execute {
            queryValidRange
              .withRangeNotPruned(
                minOffsetInclusive = queryRange.startInclusiveOffset,
                maxOffsetInclusive = queryRange.endInclusiveOffset,
                errorPruning = (prunedOffset: Offset) =>
                  s"Updates request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
                errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                  s"Updates request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                      .fold(0L)(_.unwrap)}",
              ) {
                dbDispatcher
                  .executeSql(dbMetric)(fetchEvents(ids, _))
                  .flatMap(UpdateReader.withFatContractIfNeeded(contractStore))
              }
              .map(UpdateReader.tryToResolveFatInstance)
          }
        }
      )
      .mapConcat(identity)
  }
}
