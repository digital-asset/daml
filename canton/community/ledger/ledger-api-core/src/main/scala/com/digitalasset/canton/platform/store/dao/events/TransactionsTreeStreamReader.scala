// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v2.update_service.GetUpdateTreesResponse
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.Spans
import com.digitalasset.canton.ledger.api.TraceIdentifiers
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.config.TransactionTreeStreamsConfig
import com.digitalasset.canton.platform.indexer.parallel.BatchN
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSourceForInformees,
  EventPayloadSourceForTreeTx,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.events.ReassignmentStreamReader.ReassignmentStreamQueryParams
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
import com.digitalasset.canton.platform.{ApiOffset, Party, TemplatePartiesFilter}
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class TransactionsTreeStreamReader(
    config: TransactionTreeStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
    tracer: Tracer,
    reassignmentStreamReader: ReassignmentStreamReader,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import TransactionsReader.*
  import config.*

  private val dbMetrics = metrics.daml.index.db

  private val orderBySequentialEventId =
    Ordering.by[EventStorageBackend.Entry[Raw.TreeEvent], Long](_.eventSequentialId)

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  def streamTreeTransaction(
      queryRange: EventsRange,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
    val span =
      Telemetry.Transactions.createSpan(
        tracer,
        queryRange.startExclusiveOffset,
        queryRange.endInclusiveOffset,
      )(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamTreeTransaction(${queryRange.startExclusiveOffset}, ${queryRange.endInclusiveOffset}, $requestingParties, $eventProjectionProperties)"
    )
    val sourceOfTreeTransactions = doStreamTreeTransaction(
      queryRange,
      requestingParties,
      eventProjectionProperties,
      multiDomainEnabled,
    )
    sourceOfTreeTransactions
      .wireTap(_ match {
        case (_, response) =>
          response.update match {
            case GetUpdateTreesResponse.Update.TransactionTree(txn) =>
              Spans.addEventToSpan(
                tracing.Event("transaction", TraceIdentifiers.fromTransactionTree(txn)),
                span,
              )
            case GetUpdateTreesResponse.Update.Reassignment(reassignment) =>
              Spans.addEventToSpan(
                tracing.Event("transaction", TraceIdentifiers.fromReassignment(reassignment)),
                span,
              )
            case _ => ()
          }
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamTreeTransaction(
      queryRange: EventsRange,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
    val createEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdCreateQueries, executionContext)
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdConsumingQueries, executionContext)
    val nonConsumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdNonConsumingQueries, executionContext)
    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)
    val deserializationQueriesLimiter =
      new QueueBasedConcurrencyLimiter(transactionsProcessingParallelism, executionContext)
    val filterParties = requestingParties.toVector
    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      // The ids for tree transactions are retrieved from five separate id tables.
      // To account for that we assign a fifth of the working memory to each table.
      workingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages / (
        if (multiDomainEnabled) 7 else 5
      ),
      numOfDecomposedFilters = filterParties.size,
      numOfPagesInIdPageBuffer = maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    def fetchIds(
        filterParty: Party,
        target: EventIdSourceForInformees,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        metric: DatabaseMetrics,
    ): Source[Long, NotUsed] = {
      paginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idPageSizing,
        idPageBufferSize = maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = queryRange.startExclusiveEventSeqId,
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
                  endInclusive = queryRange.endInclusiveEventSeqId,
                  limit = state.pageSize,
                )(connection)
              }
            }
          }
        }
      )
    }

    def fetchPayloads(
        ids: Source[Iterable[Long], NotUsed],
        target: EventPayloadSourceForTreeTx,
        maxParallelPayloadQueries: Int,
        metric: DatabaseMetrics,
    ): Source[EventStorageBackend.Entry[Raw.TreeEvent], NotUsed] = {
      // Akka requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
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
                  minOffsetExclusive = queryRange.startExclusiveOffset,
                  error = (prunedOffset: Offset) =>
                    s"Transactions request from ${queryRange.startExclusiveOffset.toHexString} to ${queryRange.endInclusiveOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
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
      .mapAsync(transactionsProcessingParallelism)(rawEvents =>
        deserializationQueriesLimiter.execute(
          deserializeLfValues(rawEvents, eventProjectionProperties)
        )
      )
      .mapConcat { events =>
        val responses = TransactionConversions.toGetTransactionTreesResponse(events)
        responses.map { case (offset, response) => ApiOffset.assertFromString(offset) -> response }
      }

    if (multiDomainEnabled) {
      reassignmentStreamReader
        .streamReassignments(
          ReassignmentStreamQueryParams(
            queryRange = queryRange,
            filteringConstraints = TemplatePartiesFilter(
              relation = Map.empty,
              wildcardParties = requestingParties,
            ),
            eventProjectionProperties = eventProjectionProperties,
            payloadQueriesLimiter = payloadQueriesLimiter,
            deserializationQueriesLimiter = deserializationQueriesLimiter,
            idPageSizing = idPageSizing,
            decomposedFilters = requestingParties.map(DecomposedFilter(_, None)).toVector,
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
          offset -> GetUpdateTreesResponse(
            GetUpdateTreesResponse.Update.Reassignment(reassignment)
          )
        }
        .mergeSorted(sourceOfTreeTransactions)(
          Ordering.by(_._1)
        )
    } else {
      sourceOfTreeTransactions
    }
  }

  private def mergeSortAndBatch(
      maxOutputBatchSize: Int,
      maxOutputBatchCount: Int,
  )(sourcesOfIds: Vector[Source[Long, NotUsed]]): Source[Iterable[Long], NotUsed] = {
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
  )(implicit lc: LoggingContextWithTrace): Future[Vector[EventStorageBackend.Entry[TreeEvent]]] = {
    Timed.future(
      future =
        Future.traverse(rawEvents)(deserializeEntry(eventProjectionProperties, lfValueTranslation)),
      timer = dbMetrics.treeTxStream.translationTimer,
    )
  }

}
