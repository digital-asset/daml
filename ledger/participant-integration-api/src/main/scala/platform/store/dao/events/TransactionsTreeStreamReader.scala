// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.platform.configuration.TransactionsTreeStreamReaderConfig
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.common.{
  EventIdSourceForInformees,
  EventPayloadSourceForTreeTx,
}
import com.daml.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.daml.platform.store.dao.events.EventsTable.TransactionConversions
import com.daml.platform.store.dao.events.FilterTableACSReader.{
  Filter,
  IdQueryConfiguration,
  statefulDeduplicate,
}
import com.daml.platform.store.dao.{DbDispatcher, EventProjectionProperties, PaginatingAsyncStream}
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter, Telemetry}
import com.daml.telemetry
import com.daml.telemetry.Spans

import scala.collection.mutable.ArrayBuffer
import scala.util.chaining._
import scala.concurrent.{ExecutionContext, Future}

class TransactionsTreeStreamReader(
    config: TransactionsTreeStreamReaderConfig,
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
              telemetry.Event("transaction", TraceIdentifiers.fromTransactionTree(txn)),
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
      new QueueBasedConcurrencyLimiter(
        maxParallelIdCreateQueries,
        executionContext,
        parentO = Some(globalIdQueriesLimiter),
      )
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(
        maxParallelIdConsumingQueries,
        executionContext,
        parentO = Some(globalIdQueriesLimiter),
      )
    val nonConsumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(
        maxParallelIdNonConsumingQueries,
        executionContext,
        parentO = Some(globalIdQueriesLimiter),
      )
    val payloadQueriesLimiter = new QueueBasedConcurrencyLimiter(
      maxParallelPayloadQueries,
      executionContext,
      parentO = Some(globalPayloadQueriesLimiter),
    )
    // TODO etq: Use dedicated filter type for TreeTransactions (only parties)
    val decomposedFilters = requestingParties.iterator.map(party => Filter(party, None)).toVector
    val idPageSizing = IdQueryConfiguration(
      maxIdPageSize = maxIdsPerIdPage,
      // The full set of ids for tree transactions is the union of three disjoint sets ids:
      // 1) ids for consuming events, 2) ids for non-consuming events and 3) ids for create events.
      // We assign a third of the working memory to each source.
      idPageWorkingMemoryBytes = maxWorkingMemoryInBytesForIdPages / 3,
      filterSize = decomposedFilters.size,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
    )

    def fetchIds(
        filter: Filter,
        target: EventIdSourceForInformees,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        metric: DatabaseMetrics,
    ): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        pageConfig = idPageSizing,
        pageBufferSize = maxPagesPerIdPagesBuffer,
        initialStartOffset = startExclusiveEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          maxParallelIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metric) { connection =>
              eventStorageBackend.transactionStreamingQueries.fetchEventIdsForInformees(
                target = target
              )(
                informee = filter.party,
                startExclusive = state.startOffset,
                endInclusive = endInclusiveEventSequentialId,
                limit = state.pageSize,
              )(connection)
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
            .inputBuffer(initial = maxParallelPayloadQueries, max = maxParallelPayloadQueries)
        )
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter
            .execute(
              dbDispatcher.executeSql(metric) { implicit connection =>
                queryNonPruned.executeSql(
                  query = eventStorageBackend.transactionStreamingQueries.fetchEventPayloadsTree(
                    target = target
                  )(
                    eventSequentialIds = ids,
                    allFilterParties = requestingParties,
                  )(connection),
                  // TODO etq: Consider rolling out event-seq-id based queryNonPruned
                  minOffsetExclusive = startExclusiveOffset,
                  error = (prunedOffset: Offset) =>
                    s"Transactions request from ${startExclusiveOffset.toHexString} to ${endInclusiveOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
                )
              }
            )
        )
        .mapConcat(identity)
    }

    val idsCreate =
      (decomposedFilters.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.CreateStakeholder,
          createEventIdQueriesLimiter,
          dbMetrics.treeTxIdsCreateStakeholder,
        )
      ) ++ decomposedFilters.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.CreateNonStakeholder,
          createEventIdQueriesLimiter,
          dbMetrics.treeTxIdsCreateNonStakeholderInformee,
        )
      )).pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadCreateQueries + 1,
        )
      )
    val idsConsuming =
      (decomposedFilters.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.ConsumingStakeholder,
          consumingEventIdQueriesLimiter,
          dbMetrics.treeTxIdsConsumingStakeholder,
        )
      ) ++ decomposedFilters.map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.ConsumingNonStakeholder,
          consumingEventIdQueriesLimiter,
          dbMetrics.treeTxIdsConsumingNonStakeholderInformee,
        )
      )).pipe(
        mergeSortAndBatch(
          maxOutputBatchSize = maxPayloadsPerPayloadsPage,
          maxOutputBatchCount = maxParallelPayloadConsumingQueries + 1,
        )
      )
    val idsNonConsuming = decomposedFilters
      .map(filter =>
        fetchIds(
          filter,
          EventIdSourceForInformees.NonConsumingInformee,
          nonConsumingEventIdQueriesLimiter,
          dbMetrics.treeTxIdsNonConsumingInformee,
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
      dbMetrics.treeTxPayloadCreate,
    )
    val payloadsConsuming = fetchPayloads(
      idsConsuming,
      EventPayloadSourceForTreeTx.Consuming,
      maxParallelPayloadConsumingQueries,
      dbMetrics.treeTxPayloadConsuming,
    )
    val payloadsNonConsuming = fetchPayloads(
      idsNonConsuming,
      EventPayloadSourceForTreeTx.NonConsuming,
      maxParallelPayloadNonConsumingQueries,
      dbMetrics.treeTxPayloadNonConsuming,
    )
    val allSortedPayloads = payloadsConsuming
      .mergeSorted(payloadsCreate)(orderBySequentialEventId)
      .mergeSorted(payloadsNonConsuming)(orderBySequentialEventId)
    val sourceOfTreeTransactions = TransactionsReader
      .groupContiguous(allSortedPayloads)(by = _.transactionId)
      .mapAsync(payloadProcessingParallelism)(deserializeLfValues(_, eventProjectionProperties))
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
    sourcesOfIds
      .pipe(FilterTableACSReader.mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
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
      timer = dbMetrics.getTransactionTrees.translationTimer,
    )
  }

}
