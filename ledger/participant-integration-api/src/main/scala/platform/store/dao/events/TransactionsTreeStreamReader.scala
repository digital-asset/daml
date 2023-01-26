// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.daml.ledger.api.TraceIdentifiers
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_service.GetTransactionTreesResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform.Party
import com.daml.platform.configuration.TransactionTreeStreamsConfig
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.common.{
  EventIdSourceForInformees,
  EventPayloadSourceForTreeTx,
}
import com.daml.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.daml.platform.store.dao.events.EventsTable.TransactionConversions
import com.daml.platform.store.dao.{DbDispatcher, EventProjectionProperties, PaginatingAsyncStream}
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter, Telemetry}
import com.daml.tracing
import com.daml.tracing.Spans

import scala.collection.mutable.ArrayBuffer
import scala.util.chaining._
import scala.concurrent.{ExecutionContext, Future}

class TransactionsTreeStreamReader(
    config: TransactionTreeStreamsConfig,
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
    Ordering.by[EventStorageBackend.Entry[Raw.TreeEvent], Long](_.eventSequentialId)

  def streamTreeTransaction(
      queryRange: EventsRange[(Offset, Long)],
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val startExclusiveOffset: Offset = queryRange.startExclusive._1
    val endInclusiveOffset: Offset = queryRange.endInclusive._1
    val span =
      Telemetry.Transactions.createSpan(startExclusiveOffset, endInclusiveOffset)(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamTreeTransaction($startExclusiveOffset, $endInclusiveOffset, $requestingParties, $eventProjectionProperties)"
    )
    val sourceOfTreeTransactions = doStreamTreeTransaction(
      queryRange,
      requestingParties,
      eventProjectionProperties,
    )
    sourceOfTreeTransactions
      .wireTap(_ match {
        case (_, response) =>
          response.transactions.foreach(txn =>
            Spans.addEventToSpan(
              tracing.Event("transaction", TraceIdentifiers.fromTransactionTree(txn)),
              span,
            )
          )
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamTreeTransaction(
      queryRange: EventsRange[(Offset, Long)],
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val (startExclusiveOffset, startExclusiveEventSequentialId): (Offset, Long) =
      queryRange.startExclusive
    val (endInclusiveOffset, endInclusiveEventSequentialId): (Offset, Long) =
      queryRange.endInclusive
    val createEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdCreateQueries, executionContext)
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdConsumingQueries, executionContext)
    val nonConsumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdNonConsumingQueries, executionContext)
    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)
    val filterParties = requestingParties.toVector
    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      // The ids for tree transactions are retrieved from five separate id tables.
      // To account for that we assign a fifth of the working memory to each table.
      idPageWorkingMemoryBytes = maxWorkingMemoryInBytesForIdPages / 5,
      filterSize = filterParties.size,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
    )

    def fetchIds(
        filterParty: Party,
        target: EventIdSourceForInformees,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        metric: DatabaseMetrics,
    ): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idPageSizing,
        idPageBufferSize = maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = startExclusiveEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          maxParallelIdQueriesLimiter.execute {
            globalIdQueriesLimiter.execute {
              dbDispatcher.executeSql(metric) { connection =>
                eventStorageBackend.transactionStreamingQueries.fetchEventIdsForInformee(
                  target = target
                )(
                  informee = filterParty,
                  startExclusive = state.fromIdExclusive,
                  endInclusive = endInclusiveEventSequentialId,
                  limit = state.pageSize,
                )(connection)
              }
            }
          }
        }
      )
    }

    def fetchPayloads(
        ids: Source[ArrayBuffer[Long], NotUsed],
        target: EventPayloadSourceForTreeTx,
        maxParallelPayloadQueries: Int,
        metric: DatabaseMetrics,
    ): Source[EventStorageBackend.Entry[Raw.TreeEvent], NotUsed] = {
      ids.async
        .addAttributes(
          Attributes
            .inputBuffer(initial = 8, max = 8)
        )
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              dbDispatcher.executeSql(metric) { implicit connection =>
                queryNonPruned.executeSql(
                  query = eventStorageBackend.transactionStreamingQueries.fetchEventPayloadsTree(
                    target = target
                  )(
                    eventSequentialIds = ids,
                    allFilterParties = requestingParties,
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
      (filterParties.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.CreateStakeholder,
          createEventIdQueriesLimiter,
          dbMetrics.treeTxStream.fetchEventCreateIdsStakeholder,
        )
      ) ++ filterParties.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.CreateNonStakeholder,
          createEventIdQueriesLimiter,
          dbMetrics.treeTxStream.fetchEventCreateIdsNonStakeholder,
        )
      )).pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadCreateQueries + 1,
        )
      )
    val idsConsuming =
      (filterParties.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.ConsumingStakeholder,
          consumingEventIdQueriesLimiter,
          dbMetrics.treeTxStream.fetchEventConsumingIdsStakeholder,
        )
      ) ++ filterParties.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.ConsumingNonStakeholder,
          consumingEventIdQueriesLimiter,
          dbMetrics.treeTxStream.fetchEventConsumingIdsNonStakeholder,
        )
      )).pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadConsumingQueries + 1,
        )
      )
    val idsNonConsuming = filterParties
      .map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.NonConsumingInformee,
          nonConsumingEventIdQueriesLimiter,
          dbMetrics.treeTxStream.fetchEventNonConsumingIds,
        )
      )
      .pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadNonConsumingQueries + 1,
        )
      )
    val payloadsCreate = fetchPayloads(
      idsCreate,
      EventPayloadSourceForTreeTx.Create,
      maxParallelPayloadCreateQueries,
      dbMetrics.treeTxStream.fetchEventCreatePayloads,
    )
    val payloadsConsuming = fetchPayloads(
      idsConsuming,
      EventPayloadSourceForTreeTx.Consuming,
      maxParallelPayloadConsumingQueries,
      dbMetrics.treeTxStream.fetchEventConsumingPayloads,
    )
    val payloadsNonConsuming = fetchPayloads(
      idsNonConsuming,
      EventPayloadSourceForTreeTx.NonConsuming,
      maxParallelPayloadNonConsumingQueries,
      dbMetrics.treeTxStream.fetchEventNonConsumingPayloads,
    )
    val allSortedPayloads = payloadsConsuming
      .mergeSorted(payloadsCreate)(orderBySequentialEventId)
      .mergeSorted(payloadsNonConsuming)(orderBySequentialEventId)
    val sourceOfTreeTransactions = TransactionsReader
      .groupContiguous(allSortedPayloads)(by = _.transactionId)
      .mapAsync(transactionsProcessingParallelism)(
        deserializeLfValues(_, eventProjectionProperties)
      )
      .mapConcat { events =>
        val response = TransactionConversions.toGetTransactionTreesResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
    sourceOfTreeTransactions
  }

  private def mergeSortAndBatch(
      maxOutputBatchSize: Int,
      maxOutputBatchCount: Int,
  )(sourcesOfIds: Vector[Source[Long, NotUsed]]): Source[ArrayBuffer[Long], NotUsed] = {
    EventIdsUtils
      .sortAndDeduplicateIds(sourcesOfIds)
      .via(
        BatchN(
          maxBatchSize = maxOutputBatchSize,
          maxBatchCount = maxOutputBatchCount,
        )
      )
  }

  private def deserializeLfValues(
      rawEvents: Vector[EventStorageBackend.Entry[Raw.TreeEvent]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContext): Future[Vector[EventStorageBackend.Entry[TreeEvent]]] = {
    Timed.future(
      future =
        Future.traverse(rawEvents)(deserializeEntry(eventProjectionProperties, lfValueTranslation)),
      timer = dbMetrics.treeTxStream.translationTimer,
    )
  }

}
