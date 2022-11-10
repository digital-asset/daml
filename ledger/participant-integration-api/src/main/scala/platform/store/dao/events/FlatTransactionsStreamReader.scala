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
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform.TemplatePartiesFilter
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.common.{
  EventIdSourceForStakeholders,
  EventPayloadSourceForFlatTx,
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

class FlatTransactionsStreamReader(
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

  private val logger = ContextualizedLogger.get(getClass)
  private val dbMetrics: metrics.daml.index.db.type = metrics.daml.index.db

  private val orderBySequentialEventId =
    Ordering.by[EventStorageBackend.Entry[Raw.FlatEvent], Long](_.eventSequentialId)

  def streamFlatTransactions(
      queryRange: EventsRange[(Offset, Long)],
      filteringConstraints: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val startExclusiveOffset: Offset = queryRange.startExclusive._1
    val endInclusiveOffset: Offset = queryRange.endInclusive._1
    val span =
      Telemetry.Transactions.createSpan(startExclusiveOffset, endInclusiveOffset)(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamFlatTransactions($startExclusiveOffset, $endInclusiveOffset, $filteringConstraints, $eventProjectionProperties)"
    )
    val sourceOfFlatTransactions: Source[(Offset, GetTransactionsResponse), NotUsed] =
      doStreamFlatTransactions(
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
    val createEventIdQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelCreateEventIdQueries,
      executionContext,
      parentO = Some(globalMaxParallelIdQueriesLimiter),
    )
    val consumingEventIdQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelConsumingEventIdQueries,
      executionContext,
      parentO = Some(globalMaxParallelIdQueriesLimiter),
    )
    val payloadQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelPayloadQueries,
      executionContext,
      parentO = Some(globalMaxParallelPayloadQueriesLimiter),
    )
    val decomposedFilters = FilterTableACSReader.makeSimpleFilters(filteringConstraints).toVector
    val idPageSizing = IdQueryConfiguration(
      maxIdPageSize = maxIdsPerIdPage,
      // The full set of ids for flat transactions is the union of two disjoint sets ids:
      // 1) ids for consuming events and 2) ids for create events.
      // We assign half of the working memory to each source.
      idPageWorkingMemoryBytes = maxWorkingMemoryInBytesForIdPages / 2,
      filterSize = decomposedFilters.size,
      idPageBufferSize = maxPagesPerPagesBuffer,
    )
    val buildSourceOfSortedAndBatchedIdsFun = buildSourceOfSortedAndBatchedIds(
      startExclusiveEventSequentialId = startExclusiveEventSequentialId,
      endInclusiveEventSequentialId = endInclusiveEventSequentialId,
      decomposedFilters = decomposedFilters,
      idPageSizing = idPageSizing,
      maxOutputBatchSize = maxPayloadsPerPayloadsPage,
    ) _
    val buildSourceOfPayloadsFun = buildSourceOfPayloads(
      allFilterParties = filteringConstraints.allFilterParties,
      startExclusiveOffset = startExclusiveOffset,
      endInclusiveOffset = endInclusiveOffset,
      eventFetchingLimiter = payloadQueriesLimiter,
    ) _
    val sourceOfBatchedConsumingEventIds: Source[ArrayBuffer[Long], NotUsed] =
      buildSourceOfSortedAndBatchedIdsFun(
        consumingEventIdQueriesLimiter,
        dbMetrics.flatTxIdsConsuming,
        EventIdSourceForStakeholders.Consuming,
        maxParallelConsumingEventQueries + 1,
      )
    val sourceOfBatchedCreateEventIds: Source[ArrayBuffer[Long], NotUsed] = {
      buildSourceOfSortedAndBatchedIdsFun(
        createEventIdQueriesLimiter,
        dbMetrics.flatTxIdsCreate,
        EventIdSourceForStakeholders.Create,
        maxParallelCreateEventQueries + 1,
      )
    }
    val streamOfConsumingEventPayloads = {
      buildSourceOfPayloadsFun(
        maxParallelConsumingEventQueries,
        dbMetrics.flatTxPayloadConsuming,
        EventPayloadSourceForFlatTx.Consuming,
        sourceOfBatchedConsumingEventIds,
      )
    }
    val streamOfCreateEventPayloads =
      buildSourceOfPayloadsFun(
        maxParallelCreateEventQueries,
        dbMetrics.flatTxPayloadCreate,
        EventPayloadSourceForFlatTx.Create,
        sourceOfBatchedCreateEventIds,
      )
    val sourceOfCombinedOrderedPayloads
        : Source[EventStorageBackend.Entry[Raw.FlatEvent], NotUsed] = {
      val combinedSource: Source[EventStorageBackend.Entry[Raw.FlatEvent], NotUsed] =
        streamOfConsumingEventPayloads.mergeSorted(streamOfCreateEventPayloads)(
          orderBySequentialEventId
        )
      combinedSource
    }
    val sourceOfFlatTransactions: Source[(Offset, GetTransactionsResponse), NotUsed] =
      TransactionsReader
        .groupContiguous(sourceOfCombinedOrderedPayloads)(by = _.transactionId)
        .mapAsync(payloadProcessingParallelism)(deserializeLfValues(_, eventProjectionProperties))
        .mapConcat { groupOfPayloads: Vector[EventStorageBackend.Entry[Event]] =>
          val response = TransactionConversions.toGetTransactionsResponse(groupOfPayloads)
          response.map(r => offsetFor(r) -> r)
        }
    sourceOfFlatTransactions
  }

  private def buildSourceOfSortedAndBatchedIds(
      startExclusiveEventSequentialId: Long,
      endInclusiveEventSequentialId: Long,
      decomposedFilters: Vector[Filter],
      idPageSizing: IdQueryConfiguration,
      maxOutputBatchSize: Int,
  )(
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      fetchIdsMetric: DatabaseMetrics,
      target: EventIdSourceForStakeholders,
      maxOutputBatchCount: Int,
  )(implicit lc: LoggingContext): Source[ArrayBuffer[Long], NotUsed] = {
    decomposedFilters
      .map { filter =>
        PaginatingAsyncStream.streamIdsFromSeekPagination(
          pageConfig = idPageSizing,
          pageBufferSize = maxPagesPerPagesBuffer,
          initialStartOffset = startExclusiveEventSequentialId,
        )(
          fetchPage = (state: IdPaginationState) => {
            maxParallelIdQueriesLimiter.execute {
              dbDispatcher.executeSql(fetchIdsMetric) { connection =>
                eventStorageBackend.streamingTransactionQueries.fetchEventIdsForStakeholder(
                  target = target
                )(
                  stakeholder = filter.party,
                  templateIdO = filter.templateId,
                  startExclusive = state.startOffset,
                  endInclusive = endInclusiveEventSequentialId,
                  limit = state.pageSize,
                )(connection)
              }
            }
          }
        )
      }
      .pipe(FilterTableACSReader.mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = maxOutputBatchSize,
          maxBatchCount = maxOutputBatchCount,
        )
      )
  }

  private def buildSourceOfPayloads(
      allFilterParties: Set[_root_.com.daml.platform.Party],
      startExclusiveOffset: Offset,
      endInclusiveOffset: Offset,
      eventFetchingLimiter: ConcurrencyLimiter,
  )(
      maxParallelPayloadQueries: Int,
      dbMetric: DatabaseMetrics,
      target: EventPayloadSourceForFlatTx,
      sourceOfBatchedIds: Source[ArrayBuffer[Long], NotUsed],
  )(implicit lc: LoggingContext): Source[EventStorageBackend.Entry[Raw.FlatEvent], NotUsed] = {
    sourceOfBatchedIds
      .addAttributes(
        Attributes.inputBuffer(
          initial = maxParallelPayloadQueries,
          max = maxParallelPayloadQueries,
        )
      )
      .mapAsync(maxParallelPayloadQueries)(ids =>
        eventFetchingLimiter
          .execute(
            dbDispatcher.executeSql(dbMetric) { implicit connection =>
              queryNonPruned.executeSql(
                query = eventStorageBackend.streamingTransactionQueries
                  .fetchEventPayloadsFlat(target = target)(
                    eventSequentialIds = ids,
                    allFilterParties = allFilterParties,
                  )(connection),
                minOffsetExclusive = startExclusiveOffset,
                error = (prunedOffset: Offset) =>
                  s"Transactions request from ${startExclusiveOffset.toHexString} to ${endInclusiveOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
              )
            }
          )
      )
      .mapConcat(identity)
  }

  private def deserializeLfValues(
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
