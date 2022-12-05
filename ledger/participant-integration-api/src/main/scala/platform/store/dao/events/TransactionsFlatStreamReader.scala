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
import com.daml.platform.configuration.TransactionsFlatStreamReaderConfig
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
  IdQueryConfiguration,
  statefulDeduplicate,
}
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter, Telemetry}
import com.daml.telemetry
import com.daml.telemetry.Spans

import scala.util.chaining._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class TransactionsFlatStreamReader(
    config: TransactionsFlatStreamReaderConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext) {
  import TransactionsReader._
  import config._

  private val logger = ContextualizedLogger.get(getClass)
  private val dbMetrics = metrics.daml.index.db

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
    val createEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdCreateQueries, executionContext)
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdConsumingQueries, executionContext)
    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)
    val decomposedFilters = FilterTableACSReader.makeSimpleFilters(filteringConstraints).toVector
    val idPageSizing = IdQueryConfiguration(
      maxIdPageSize = maxIdsPerIdPage,
      // The full set of ids for flat transactions is the union of two disjoint sets ids:
      // 1) ids for consuming events and 2) ids for create events.
      // We assign half of the working memory to each source.
      idPageWorkingMemoryBytes = maxWorkingMemoryInBytesForIdPages / 2,
      filterSize = decomposedFilters.size,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
    )

    def fetchIds(
        target: EventIdSourceForStakeholders,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        maxOutputBatchCount: Int,
        metric: DatabaseMetrics,
    ): Source[ArrayBuffer[Long], NotUsed] = {
      decomposedFilters
        .map { filter =>
          PaginatingAsyncStream.streamIdsFromSeekPagination(
            pageConfig = idPageSizing,
            pageBufferSize = maxPagesPerIdPagesBuffer,
            initialStartOffset = startExclusiveEventSequentialId,
          )(
            fetchPage = (state: IdPaginationState) => {
              maxParallelIdQueriesLimiter.execute {
                globalIdQueriesLimiter.execute {
                  dbDispatcher.executeSql(metric) { connection =>
                    eventStorageBackend.transactionStreamingQueries.fetchEventIdsForStakeholder(
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
            }
          )
        }
        .pipe(FilterTableACSReader.mergeSort[Long])
        .statefulMapConcat(statefulDeduplicate)
        .via(
          BatchN(
            maxBatchSize = maxPayloadsPerPayloadsPage,
            maxBatchCount = maxOutputBatchCount,
          )
        )
    }

    def fetchPayloads(
        ids: Source[ArrayBuffer[Long], NotUsed],
        target: EventPayloadSourceForFlatTx,
        maxParallelPayloadQueries: Int,
        dbMetric: DatabaseMetrics,
    ): Source[EventStorageBackend.Entry[Raw.FlatEvent], NotUsed] = {
      ids.async
        .addAttributes(
          Attributes.inputBuffer(
            initial = maxParallelPayloadQueries,
            max = maxParallelPayloadQueries,
          )
        )
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              dbDispatcher.executeSql(dbMetric) { implicit connection =>
                queryNonPruned.executeSql(
                  query = eventStorageBackend.transactionStreamingQueries
                    .fetchEventPayloadsFlat(target = target)(
                      eventSequentialIds = ids,
                      allFilterParties = filteringConstraints.allFilterParties,
                    )(connection),
                  minOffsetExclusive = startExclusiveOffset,
                  error = (prunedOffset: Offset) =>
                    s"Transactions request from ${startExclusiveOffset.toHexString} to ${endInclusiveOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
                )
              }
            }
          }
        )
        .mapConcat(identity)
    }

    val idsCreate =
      fetchIds(
        target = EventIdSourceForStakeholders.Create,
        maxParallelIdQueriesLimiter = createEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadCreateQueries + 1,
        metric = dbMetrics.flatTxIdsCreate,
      )
    val idsConsuming =
      fetchIds(
        target = EventIdSourceForStakeholders.Consuming,
        maxParallelIdQueriesLimiter = consumingEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadConsumingQueries + 1,
        metric = dbMetrics.flatTxIdsConsuming,
      )
    val payloadsCreate =
      fetchPayloads(
        ids = idsCreate,
        target = EventPayloadSourceForFlatTx.Create,
        maxParallelPayloadQueries = maxParallelPayloadCreateQueries,
        dbMetric = dbMetrics.flatTxPayloadCreate,
      )
    val payloadsConsuming =
      fetchPayloads(
        ids = idsConsuming,
        target = EventPayloadSourceForFlatTx.Consuming,
        maxParallelPayloadQueries = maxParallelPayloadConsumingQueries,
        dbMetric = dbMetrics.flatTxPayloadConsuming,
      )
    val allSortedPayloads = payloadsConsuming.mergeSorted(payloadsCreate)(orderBySequentialEventId)
    val sourceOfFlatTransactions: Source[(Offset, GetTransactionsResponse), NotUsed] =
      TransactionsReader
        .groupContiguous(allSortedPayloads)(by = _.transactionId)
        .mapAsync(payloadProcessingParallelism)(deserializeLfValues(_, eventProjectionProperties))
        .mapConcat { groupOfPayloads: Vector[EventStorageBackend.Entry[Event]] =>
          val response = TransactionConversions.toGetTransactionsResponse(groupOfPayloads)
          response.map(r => offsetFor(r) -> r)
        }
    sourceOfFlatTransactions
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
