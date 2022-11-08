// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.daml.ledger.api.TraceIdentifiers
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction_service.GetTransactionsResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform.{Identifier, Party, TemplatePartiesFilter}
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.store.backend.{
  EventIdFetchingForStakeholdersTarget,
  EventStorageBackend,
  PayloadFetchingForFlatTxTarget,
}
import com.daml.platform.store.dao.{DbDispatcher, EventProjectionProperties, PaginatingAsyncStream}
import com.daml.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.daml.platform.store.dao.events.EventsTable.TransactionConversions
import com.daml.platform.store.dao.events.FilterTableACSReader.{
  Filter,
  IdQueryConfiguration,
  statefulDeduplicate,
}
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter, Telemetry}
import com.daml.telemetry
import com.daml.telemetry.Spans

import scala.util.chaining._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class TransactionsFlatReader(
    maxPayloadsPerPayloadsPage: Int,
    payloadProcessingParallelism: Int,
    maxIdsPerIdPage: Int = IndexServiceConfig.DefaultAcsIdPageSize,
    maxPagesPerPagesBuffer: Int = IndexServiceConfig.DefaultAcsIdPageBufferSize,
    maxWorkingMemoryInBytesForIdPages: Int = IndexServiceConfig.DefaultAcsIdPageWorkingMemoryBytes,
    // NOTE: These must be powers of 2
    // BEGIN
    maxParallelConsumingEventQueries: Int = 2,
    maxParallelCreateEventQueries: Int = 2,
    maxParallelCreateEventIdQueries: Int = 4,
    maxParallelConsumingEventIdQueries: Int = 4,
    // END
    maxParallelPayloadQueries: Int = 2,
    globalMaxParallelIdQueriesLimiter: ConcurrencyLimiter,
    globalMaxParallelPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext) {
  import TransactionsReader._

  private val dbMetrics: metrics.daml.index.db.type = metrics.daml.index.db
  private val logger = ContextualizedLogger.get(getClass)

  private val orderBySequentialEventId =
    Ordering.by[EventStorageBackend.Entry[Raw.FlatEvent], Long](_.eventSequentialId)

  def streamFlatTransactions(
      queryRange: EventsRange[(Offset, Long)],
      filteringConstraints: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val startExclusiveOffset: Offset = queryRange.startExclusive._1
    val endInclusiveOffset: Offset = queryRange.endInclusive._1
    val span = Telemetry.Transactions.createSpan(startExclusiveOffset, endInclusiveOffset)(
      qualifiedNameOfCurrentFunc
    )
    logger.debug(
      s"Streaming flat transactions ($startExclusiveOffset, $endInclusiveOffset, $filteringConstraints, $eventProjectionProperties)"
    )
    val sourceOfFlatTransactions = doStreamFlatTransactions(
      queryRange,
      filteringConstraints,
      eventProjectionProperties,
    )
    sourceOfFlatTransactions
      .wireTap(_ match {
        case (_, getTransactionsResponse) =>
          getTransactionsResponse.transactions.foreach { transaction =>
            val event =
              telemetry.Event("transaction", TraceIdentifiers.fromTransaction(transaction))
            Spans.addEventToSpan(event, span)
          }
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamFlatTransactions(
      queryRange: EventsRange[(Offset, Long)],
      filteringConstraints: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val (startExclusiveOffset, startExclusiveEventSequentialId): (Offset, Long) =
      queryRange.startExclusive
    val (endInclusiveOffset, endInclusiveEventSequentialId): (Offset, Long) =
      queryRange.endInclusive

    val maxParallelCreateEventIdQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelCreateEventIdQueries,
      executionContext,
      parentO = Some(globalMaxParallelIdQueriesLimiter),
    )
    val maxParallelConsumingEventIdQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelConsumingEventIdQueries,
      executionContext,
      parentO = Some(globalMaxParallelIdQueriesLimiter),
    )
    val maxParallelPayloadQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelPayloadQueries,
      executionContext,
      parentO = Some(globalMaxParallelPayloadQueriesLimiter),
    )

    val decomposedFilters = FilterTableACSReader.makeSimpleFilters(filteringConstraints).toVector
    val idPageSizing = IdQueryConfiguration(
      maxIdPageSize = maxIdsPerIdPage,
      // The full set of ids for flat transactions is the union of two disjoint sets ids from 1) consuming events, and 2) create events.
      // We assign half of the working memory for each of the two source sets.
      idPageWorkingMemoryBytes = maxWorkingMemoryInBytesForIdPages / 2,
      filterSize = decomposedFilters.size,
      idPageBufferSize = maxPagesPerPagesBuffer,
    )

    val sourceOfBatchedConsumingEventIds = streamIdsForFilters(
      startExclusiveEventSequentialId = startExclusiveEventSequentialId,
      endInclusiveEventSequentialId = endInclusiveEventSequentialId,
      decomposedFilters = decomposedFilters,
      idPageSizing = idPageSizing,
      maxParallelIdQueriesLimiter = maxParallelConsumingEventIdQueriesLimiter,
      fetchIdsMetric = metrics.daml.index.db.getConsumingIds_stakeholdersFilter,
      target = EventIdFetchingForStakeholdersTarget.ConsumingStakeholder,
      maxOutputBatchSize = maxPayloadsPerPayloadsPage,
      maxOutputBatchCount = maxParallelConsumingEventQueries + 1,
    )
    val sourceOfBatchedCreateEventIds = streamIdsForFilters(
      startExclusiveEventSequentialId = startExclusiveEventSequentialId,
      endInclusiveEventSequentialId = endInclusiveEventSequentialId,
      decomposedFilters = decomposedFilters,
      idPageSizing = idPageSizing,
      maxParallelIdQueriesLimiter = maxParallelCreateEventIdQueriesLimiter,
      fetchIdsMetric = metrics.daml.index.db.getCreateEventIds_stakeholdersFilter,
      target = EventIdFetchingForStakeholdersTarget.CreateStakeholder,
      maxOutputBatchSize = maxPayloadsPerPayloadsPage,
      maxOutputBatchCount = maxParallelCreateEventQueries + 1,
    )

    val sourceOfFlatTransactions: Source[(Offset, GetTransactionsResponse), NotUsed] =
      buildSourceOfFlatTransactions(
        startExclusiveOffset = startExclusiveOffset,
        endInclusiveOffset = endInclusiveOffset,
        allFilterParties = filteringConstraints.allFilterParties,
        eventProjectionProperties = eventProjectionProperties,
        sourceOfBatchedConsumingEventIds = sourceOfBatchedConsumingEventIds,
        sourceOfBatchedCreateEventIds = sourceOfBatchedCreateEventIds,
        eventFetchingLimiter = maxParallelPayloadQueriesLimiter,
      )
    sourceOfFlatTransactions
  }

  private def streamIdsForFilters(
      startExclusiveEventSequentialId: Long,
      endInclusiveEventSequentialId: Long,
      decomposedFilters: Vector[Filter],
      idPageSizing: IdQueryConfiguration,
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      fetchIdsMetric: DatabaseMetrics,
      target: EventIdFetchingForStakeholdersTarget,
      maxOutputBatchSize: Int,
      maxOutputBatchCount: Int,
  )(implicit lc: LoggingContext): Source[ArrayBuffer[Long], NotUsed] = {
    decomposedFilters
      .map(filter => {
        streamIdsForStakeholderAndTemplate(
          stakeholder = filter.party,
          templateIdO = filter.templateId,
          startExclusiveEventSequentialId = startExclusiveEventSequentialId,
          endInclusiveEventSequentialId = endInclusiveEventSequentialId,
          idPageSizing = idPageSizing,
          maxParallelIdQueriesLimiter = maxParallelIdQueriesLimiter,
          fetchIdsMetric = fetchIdsMetric,
          target = target,
        )
      })
      .pipe(FilterTableACSReader.mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = maxOutputBatchSize,
          maxBatchCount = maxOutputBatchCount,
        )
      )
  }

  private def buildSourceOfFlatTransactions(
      sourceOfBatchedConsumingEventIds: Source[ArrayBuffer[Long], NotUsed],
      sourceOfBatchedCreateEventIds: Source[ArrayBuffer[Long], NotUsed],
      allFilterParties: Set[Party],
      startExclusiveOffset: Offset,
      endInclusiveOffset: Offset,
      eventFetchingLimiter: ConcurrencyLimiter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val streamOfConsumingEventPayloads =
      buildSourceOfPayloads(
        allFilterParties = allFilterParties,
        firstOffset = startExclusiveOffset,
        lastOffset = endInclusiveOffset,
        eventFetchingLimiter = eventFetchingLimiter,
        maxParallelPayloadQueries = maxParallelConsumingEventQueries,
        dbMetric = metrics.daml.index.db.getFlatTransactions,
        target = PayloadFetchingForFlatTxTarget.ConsumingEventPayloads,
        sourceOfBatchedIds = sourceOfBatchedConsumingEventIds,
      )
    val streamOfCreateEventPayloads =
      buildSourceOfPayloads(
        allFilterParties = allFilterParties,
        firstOffset = startExclusiveOffset,
        lastOffset = endInclusiveOffset,
        eventFetchingLimiter = eventFetchingLimiter,
        maxParallelPayloadQueries = maxParallelCreateEventQueries,
        dbMetric = metrics.daml.index.db.getFlatTransactions,
        target = PayloadFetchingForFlatTxTarget.CreateEventPayloads,
        sourceOfBatchedIds = sourceOfBatchedCreateEventIds,
      )
    val combinedSource = streamOfConsumingEventPayloads.mergeSorted(streamOfCreateEventPayloads)(
      orderBySequentialEventId
    )
    TransactionsReader
      .groupContiguous(combinedSource)(by = _.transactionId)
      .mapAsync(payloadProcessingParallelism)(decode(_, eventProjectionProperties))
      .mapConcat { events: Vector[EventStorageBackend.Entry[Event]] =>
        val response = TransactionConversions.toGetTransactionsResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
  }

  private def buildSourceOfPayloads(
      allFilterParties: Set[_root_.com.daml.platform.Party],
      firstOffset: Offset,
      lastOffset: Offset,
      eventFetchingLimiter: ConcurrencyLimiter,
      maxParallelPayloadQueries: Int,
      dbMetric: DatabaseMetrics,
      target: PayloadFetchingForFlatTxTarget,
      sourceOfBatchedIds: Source[ArrayBuffer[Long], NotUsed],
  )(implicit lc: LoggingContext): Source[EventStorageBackend.Entry[Raw.FlatEvent], NotUsed] = {
    sourceOfBatchedIds
      .addAttributes(
        Attributes.inputBuffer(
          initial = maxParallelPayloadQueries,
          max = maxParallelPayloadQueries,
        )
      )
      .mapAsync(maxParallelPayloadQueries)(
        fetchPayloads(
          _,
          allFilterParties,
          firstOffset,
          lastOffset,
          eventFetchingLimiter,
          dbMetric = dbMetric,
          target = target,
        )
      )
      .mapConcat(identity)
  }

  private def streamIdsForStakeholderAndTemplate(
      stakeholder: Ref.Party,
      templateIdO: Option[Identifier],
      startExclusiveEventSequentialId: Long,
      endInclusiveEventSequentialId: Long,
      idPageSizing: IdQueryConfiguration,
      maxParallelIdQueriesLimiter: ConcurrencyLimiter,
      fetchIdsMetric: DatabaseMetrics,
      target: EventIdFetchingForStakeholdersTarget,
  )(implicit lc: LoggingContext): Source[Long, NotUsed] = {
    PaginatingAsyncStream.streamIdsFromSeekPagination(
      pageConfig = idPageSizing,
      pageBufferSize = maxPagesPerPagesBuffer,
      initialStartOffset = startExclusiveEventSequentialId,
    )(
      fetchPage = (state: IdPaginationState) => {
        maxParallelIdQueriesLimiter.execute {
          dbDispatcher.executeSql(fetchIdsMetric) { connection =>
            eventStorageBackend.fetchEventIdsForStakeholder(
              target = target
            )(
              partyFilter = stakeholder,
              templateIdFilter = templateIdO,
              startExclusive = state.startOffset,
              endInclusive = endInclusiveEventSequentialId,
              limit = state.pageSize,
            )(connection)
          }
        }
      }
    )
  }

  private def fetchPayloads(
      ids: Iterable[Long],
      allFilterParties: Set[Ref.Party],
      firstOffset: Offset,
      lastOffset: Offset,
      eventFetchingLimiter: ConcurrencyLimiter,
      dbMetric: DatabaseMetrics,
      target: PayloadFetchingForFlatTxTarget,
  )(implicit lc: LoggingContext): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] = {
    eventFetchingLimiter
      .execute(
        dbDispatcher.executeSql(dbMetric) { implicit connection =>
          queryNonPruned.executeSql(
            query = eventStorageBackend.fetchEventPayloadsFlat(target = target)(
              eventSequentialIds = ids,
              allFilterParties = allFilterParties,
            )(connection),
            minOffsetExclusive = firstOffset,
            error = (prunedOffset: Offset) =>
              s"Transactions request from ${firstOffset.toHexString} to ${lastOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
          )
        }
      )
  }

  private def decode(
      rawEvents: Vector[EventStorageBackend.Entry[Raw.FlatEvent]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContext): Future[Vector[EventStorageBackend.Entry[Event]]] = {
    Timed.future(
      future =
        Future.traverse(rawEvents)(deserializeEntry(eventProjectionProperties, lfValueTranslation)),
      timer = dbMetrics.getFlatTransactions.translationTimer,
    )
  }

}
