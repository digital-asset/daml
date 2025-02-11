// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.transaction.TreeEvent
import com.daml.ledger.api.v2.update_service.GetUpdateTreesResponse
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.Spans
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.{ParticipantAuthorizationFormat, TraceIdentifiers}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.config.TransactionTreeStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{Entry, RawTreeEvent}
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSource,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
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
import com.digitalasset.canton.platform.{Party, TemplatePartiesFilter}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class TransactionsTreeStreamReader(
    config: TransactionTreeStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: LedgerApiServerMetrics,
    tracer: Tracer,
    reassignmentStreamReader: ReassignmentStreamReader,
    topologyTransactionsStreamReader: TopologyTransactionsStreamReader,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import UpdateReader.*
  import config.*

  private val dbMetrics = metrics.index.db

  private val orderBySequentialEventId =
    Ordering.by[Entry[RawTreeEvent], Long](_.eventSequentialId)

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val directEC = DirectExecutionContext(logger)

  def streamTreeTransaction(
      queryRange: EventsRange,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
    val span =
      Telemetry.Updates.createSpan(
        tracer,
        queryRange.startInclusiveOffset,
        queryRange.endInclusiveOffset,
      )(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamTreeTransaction(${queryRange.startInclusiveOffset}, ${queryRange.endInclusiveOffset}, $requestingParties, $eventProjectionProperties)"
    )
    val sourceOfTreeTransactions = doStreamTreeTransaction(
      queryRange,
      requestingParties,
      eventProjectionProperties,
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
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
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
    val filterParties: Vector[Option[Party]] =
      requestingParties.fold(Vector(None: Option[Party]))(_.map(Some(_)).toVector)
    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      // The ids for tree transactions are retrieved from seven separate id tables:
      //   * Create stakeholder
      //   * Create non-stakeholder
      //   * Exercise consuming stakeholder
      //   * Exercise consuming non-stakeholder
      //   * Exercise non-consuming
      //   * Assign
      //   * Unassign
      // To account for that we assign a seventh of the working memory to each table.
      workingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages / 7,
      numOfDecomposedFilters = filterParties.size,
      numOfPagesInIdPageBuffer = maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    def fetchIds(
        filterParty: Option[Party],
        target: EventIdSource,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        metric: DatabaseMetrics,
    ): Source[Long, NotUsed] =
      paginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idPageSizing,
        idPageBufferSize = maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
      )(
        fetchPage = (state: IdPaginationState) => {
          maxParallelIdQueriesLimiter.execute {
            globalIdQueriesLimiter.execute {
              dbDispatcher.executeSql(metric) { connection =>
                eventStorageBackend.transactionStreamingQueries.fetchEventIds(
                  target = target
                )(
                  stakeholderO = filterParty,
                  templateIdO = None,
                  startExclusive = state.fromIdExclusive,
                  endInclusive = queryRange.endInclusiveEventSeqId,
                  limit = state.pageSize,
                )(connection)
              }
            }
          }
        }
      )

    def fetchPayloads(
        ids: Source[Iterable[Long], NotUsed],
        target: EventPayloadSourceForUpdatesLedgerEffects,
        maxParallelPayloadQueries: Int,
        metric: DatabaseMetrics,
    ): Source[Entry[RawTreeEvent], NotUsed] = {
      // Pekko requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              dbDispatcher.executeSql(metric) { implicit connection =>
                queryValidRange.withRangeNotPruned(
                  minOffsetInclusive = queryRange.startInclusiveOffset,
                  maxOffsetInclusive = queryRange.endInclusiveOffset,
                  errorPruning = (prunedOffset: Offset) =>
                    s"Transactions request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
                  errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                    s"Transactions request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                        .fold(0L)(_.unwrap)}",
                ) {
                  eventStorageBackend.transactionStreamingQueries.fetchEventPayloadsLedgerEffects(
                    target = target
                  )(
                    eventSequentialIds = ids,
                    allFilterParties = requestingParties,
                  )(connection)
                }
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
          EventIdSource.CreateStakeholder,
          createEventIdQueriesLimiter,
          dbMetrics.updatesLedgerEffectsStream.fetchEventCreateIdsStakeholder,
        )
      ) ++ filterParties.map(filter =>
        fetchIds(
          filter,
          EventIdSource.CreateNonStakeholder,
          createEventIdQueriesLimiter,
          dbMetrics.updatesLedgerEffectsStream.fetchEventCreateIdsNonStakeholder,
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
          EventIdSource.ConsumingStakeholder,
          consumingEventIdQueriesLimiter,
          dbMetrics.updatesLedgerEffectsStream.fetchEventConsumingIdsStakeholder,
        )
      ) ++ filterParties.map(filter =>
        fetchIds(
          filter,
          EventIdSource.ConsumingNonStakeholder,
          consumingEventIdQueriesLimiter,
          dbMetrics.updatesLedgerEffectsStream.fetchEventConsumingIdsNonStakeholder,
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
          EventIdSource.NonConsumingInformee,
          nonConsumingEventIdQueriesLimiter,
          dbMetrics.updatesLedgerEffectsStream.fetchEventNonConsumingIds,
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
      EventPayloadSourceForUpdatesLedgerEffects.Create,
      maxParallelPayloadCreateQueries,
      dbMetrics.updatesLedgerEffectsStream.fetchEventCreatePayloads,
    )
    val payloadsConsuming = fetchPayloads(
      idsConsuming,
      EventPayloadSourceForUpdatesLedgerEffects.Consuming,
      maxParallelPayloadConsumingQueries,
      dbMetrics.updatesLedgerEffectsStream.fetchEventConsumingPayloads,
    )
    val payloadsNonConsuming = fetchPayloads(
      idsNonConsuming,
      EventPayloadSourceForUpdatesLedgerEffects.NonConsuming,
      maxParallelPayloadNonConsumingQueries,
      dbMetrics.updatesLedgerEffectsStream.fetchEventNonConsumingPayloads,
    )
    val allSortedPayloads = payloadsConsuming
      .mergeSorted(payloadsCreate)(orderBySequentialEventId)
      .mergeSorted(payloadsNonConsuming)(orderBySequentialEventId)
    val sourceOfTreeTransactions = UpdateReader
      .groupContiguous(allSortedPayloads)(by = _.updateId)
      .mapAsync(transactionsProcessingParallelism)(rawEvents =>
        deserializationQueriesLimiter.execute(
          deserializeLfValues(rawEvents, eventProjectionProperties)
        )
      )
      .mapConcat { events =>
        val responses = TransactionConversions.toGetTransactionTreesResponse(events)
        responses.map { case (offset, response) => Offset.tryFromLong(offset) -> response }
      }

    val participantAuthorizationFormat =
      ParticipantAuthorizationFormat(parties = requestingParties)

    val topologyTransactions =
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
          offset -> GetUpdateTreesResponse(
            GetUpdateTreesResponse.Update.TopologyTransaction(topologyTransaction)
          )
        }

    val reassignments =
      reassignmentStreamReader
        .streamReassignments(
          ReassignmentStreamQueryParams(
            queryRange = queryRange,
            filteringConstraints = TemplatePartiesFilter(
              relation = Map.empty,
              templateWildcardParties = requestingParties,
            ),
            eventProjectionProperties = eventProjectionProperties,
            payloadQueriesLimiter = payloadQueriesLimiter,
            deserializationQueriesLimiter = deserializationQueriesLimiter,
            idPageSizing = idPageSizing,
            decomposedFilters = filterParties.map(DecomposedFilter(_, None)),
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

    sourceOfTreeTransactions
      .mergeSorted(topologyTransactions.map { case (offset, response) =>
        offset -> response
      })(Ordering.by(_._1))
      .mergeSorted(reassignments.map { case (offset, response) =>
        offset -> response
      })(Ordering.by(_._1))
  }

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

  private def deserializeLfValues(
      rawEvents: Vector[Entry[RawTreeEvent]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[Seq[Entry[TreeEvent]]] =
    Timed.future(
      future = Future.delegate {
        implicit val executionContext: ExecutionContext =
          directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
        MonadUtil.sequentialTraverse(rawEvents)(
          UpdateReader.deserializeTreeEvent(eventProjectionProperties, lfValueTranslation)
        )
      },
      timer = dbMetrics.updatesLedgerEffectsStream.translationTimer,
    )

}
