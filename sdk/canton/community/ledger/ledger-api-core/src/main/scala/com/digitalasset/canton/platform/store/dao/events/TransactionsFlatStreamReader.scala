// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.Spans
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TraceIdentifiers
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.config.TransactionFlatStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{Entry, RawFlatEvent}
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSourceForStakeholders,
  EventPayloadSourceForFlatTx,
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
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class TransactionsFlatStreamReader(
    config: TransactionFlatStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: LedgerApiServerMetrics,
    tracer: Tracer,
    reassignmentStreamReader: ReassignmentStreamReader,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import TransactionsReader.*
  import config.*

  private val dbMetrics = metrics.index.db

  private val orderBySequentialEventId =
    Ordering.by[Entry[RawFlatEvent], Long](_.eventSequentialId)

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  def streamFlatTransactions(
      queryRange: EventsRange,
      filteringConstraints: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val span =
      Telemetry.Transactions.createSpan(
        tracer,
        queryRange.startExclusiveOffset,
        queryRange.endInclusiveOffset,
      )(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamFlatTransactions(${queryRange.startExclusiveOffset}, ${queryRange.endInclusiveOffset}, $filteringConstraints, $eventProjectionProperties)"
    )
    doStreamFlatTransactions(
      queryRange,
      filteringConstraints,
      eventProjectionProperties,
    )
      .wireTap(_ match {
        case (_, getTransactionsResponse) =>
          getTransactionsResponse.update match {
            case GetUpdatesResponse.Update.Transaction(value) =>
              val event = tracing.Event("transaction", TraceIdentifiers.fromTransaction(value))
              Spans.addEventToSpan(event, span)
            case GetUpdatesResponse.Update.Reassignment(reassignment) =>
              Spans.addEventToSpan(
                tracing.Event("transaction", TraceIdentifiers.fromReassignment(reassignment)),
                span,
              )
            case _ => ()
          }
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamFlatTransactions(
      queryRange: EventsRange,
      filteringConstraints: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val createEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdCreateQueries, executionContext)
    val consumingEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdConsumingQueries, executionContext)
    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)
    val deserializationQueriesLimiter =
      new QueueBasedConcurrencyLimiter(transactionsProcessingParallelism, executionContext)
    val decomposedFilters = FilterUtils.decomposeFilters(filteringConstraints).toVector
    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      // The ids for flat transactions are retrieved from 4 separate id tables: (create, archive, assign, unassign)
      // To account for that we assign a quarter of the working memory to each table.
      workingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages / 4,
      numOfDecomposedFilters = decomposedFilters.size,
      numOfPagesInIdPageBuffer = maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    def fetchIds(
        target: EventIdSourceForStakeholders,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        maxOutputBatchCount: Int,
        metric: DatabaseMetrics,
    ): Source[Iterable[Long], NotUsed] =
      decomposedFilters
        .map { filter =>
          paginatingAsyncStream.streamIdsFromSeekPagination(
            idPageSizing = idPageSizing,
            idPageBufferSize = maxPagesPerIdPagesBuffer,
            initialFromIdExclusive = queryRange.startExclusiveEventSeqId,
          )(
            fetchPage = (state: IdPaginationState) => {
              maxParallelIdQueriesLimiter.execute {
                globalIdQueriesLimiter.execute {
                  dbDispatcher.executeSql(metric) { connection =>
                    eventStorageBackend.transactionStreamingQueries.fetchEventIdsForStakeholder(
                      target = target
                    )(
                      stakeholderO = filter.party,
                      templateIdO = filter.templateId,
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
        .pipe(EventIdsUtils.sortAndDeduplicateIds)
        .batchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = maxOutputBatchCount,
        )

    def fetchPayloads(
        ids: Source[Iterable[Long], NotUsed],
        target: EventPayloadSourceForFlatTx,
        maxParallelPayloadQueries: Int,
        dbMetric: DatabaseMetrics,
    ): Source[Entry[RawFlatEvent], NotUsed] = {
      // Pekko requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              dbDispatcher.executeSql(dbMetric) { implicit connection =>
                queryValidRange.withRangeNotPruned(
                  minOffsetExclusive = queryRange.startExclusiveOffset,
                  maxOffsetInclusive = queryRange.endInclusiveOffset,
                  errorPruning = (prunedOffset: Offset) =>
                    s"Transactions request from ${queryRange.startExclusiveOffset.toHexString} to ${queryRange.endInclusiveOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
                  errorLedgerEnd = (ledgerEndOffset: Offset) =>
                    s"Transactions request from ${queryRange.startExclusiveOffset.toHexString} to ${queryRange.endInclusiveOffset.toHexString} is beyond ledger end offset ${ledgerEndOffset.toHexString}",
                ) {
                  eventStorageBackend.transactionStreamingQueries
                    .fetchEventPayloadsFlat(target = target)(
                      eventSequentialIds = ids,
                      allFilterParties = filteringConstraints.allFilterParties,
                    )(connection)
                }
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
        metric = dbMetrics.flatTxStream.fetchEventCreateIdsStakeholder,
      )
    val idsConsuming =
      fetchIds(
        target = EventIdSourceForStakeholders.Consuming,
        maxParallelIdQueriesLimiter = consumingEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadConsumingQueries + 1,
        metric = dbMetrics.flatTxStream.fetchEventConsumingIdsStakeholder,
      )
    val payloadsCreate =
      fetchPayloads(
        ids = idsCreate,
        target = EventPayloadSourceForFlatTx.Create,
        maxParallelPayloadQueries = maxParallelPayloadCreateQueries,
        dbMetric = dbMetrics.flatTxStream.fetchEventCreatePayloads,
      )
    val payloadsConsuming =
      fetchPayloads(
        ids = idsConsuming,
        target = EventPayloadSourceForFlatTx.Consuming,
        maxParallelPayloadQueries = maxParallelPayloadConsumingQueries,
        dbMetric = dbMetrics.flatTxStream.fetchEventConsumingPayloads,
      )
    val allSortedPayloads = payloadsConsuming.mergeSorted(payloadsCreate)(orderBySequentialEventId)
    val sourceOfTransactions = TransactionsReader
      .groupContiguous(allSortedPayloads)(by = _.updateId)
      .mapAsync(transactionsProcessingParallelism)(rawEvents =>
        deserializationQueriesLimiter.execute(
          deserializeLfValues(rawEvents, eventProjectionProperties)
        )
      )
      .mapConcat { (groupOfPayloads: Vector[Entry[Event]]) =>
        val responses = TransactionConversions.toGetTransactionsResponse(groupOfPayloads)
        responses.map { case (offset, response) => Offset.fromLong(offset) -> response }
      }

    reassignmentStreamReader
      .streamReassignments(
        ReassignmentStreamQueryParams(
          queryRange = queryRange,
          filteringConstraints = filteringConstraints,
          eventProjectionProperties = eventProjectionProperties,
          payloadQueriesLimiter = payloadQueriesLimiter,
          deserializationQueriesLimiter = deserializationQueriesLimiter,
          idPageSizing = idPageSizing,
          decomposedFilters = decomposedFilters,
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
      .mergeSorted(sourceOfTransactions)(
        Ordering.by(_._1)
      )
  }

  private def deserializeLfValues(
      rawEvents: Vector[Entry[RawFlatEvent]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[Vector[Entry[Event]]] =
    Timed.future(
      future = Future.traverse(rawEvents)(
        TransactionsReader.deserializeFlatEvent(eventProjectionProperties, lfValueTranslation)
      ),
      timer = dbMetrics.flatTxStream.translationTimer,
    )

}
