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
import com.daml.metrics.{Metrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform.Party
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.store.backend.EventStorageBackend
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

import scala.concurrent.{ExecutionContext, Future}

class TransactionsTreeReader(
    maxPayloadsPerPayloadsPage: Int,
    payloadProcessingParallelism: Int,
    maxIdsPerIdPage: Int = IndexServiceConfig.DefaultAcsIdPageSize,
    maxPagesPerPagesBuffer: Int = IndexServiceConfig.DefaultAcsIdPageBufferSize,
    maxWorkingMemoryInBytesForIdPages: Int = IndexServiceConfig.DefaultAcsIdPageWorkingMemoryBytes,
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
  private val dbMetrics = metrics.daml.index.db

  def doGetTransactionTrees(
      queryRange: EventsRange[(Offset, Long)],
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val (firstOffset, firstEventSequentialId): (Offset, Long) = queryRange.startExclusive
    val (lastOffset, lastEventSequentialId): (Offset, Long) = queryRange.endInclusive

    val span =
      Telemetry.Transactions.createSpan(firstOffset, lastOffset)(qualifiedNameOfCurrentFunc)
    logger.debug(
      s"getTransactionTrees($firstOffset, $lastOffset, $requestingParties, $eventProjectionProperties)"
    )

    val eventsFetchingAndDecodingParallelism_mapAsync = 2
    val idFetchingLimiter_create =
      new QueueBasedConcurrencyLimiter(
        8,
        executionContext,
        parentO = Some(globalMaxParallelIdQueriesLimiter),
      )
    val idFetchingLimiter_consuming =
      new QueueBasedConcurrencyLimiter(
        8,
        executionContext,
        parentO = Some(globalMaxParallelIdQueriesLimiter),
      )
    val idFetchingLimiter_nonConsuming =
      new QueueBasedConcurrencyLimiter(
        4,
        executionContext,
        parentO = Some(globalMaxParallelIdQueriesLimiter),
      )
    val eventFetchingLimiter: ConcurrencyLimiter = new QueueBasedConcurrencyLimiter(
      2,
      executionContext,
      parentO = Some(globalMaxParallelPayloadQueriesLimiter),
    )

    val filters: Vector[Filter] =
      requestingParties.iterator.map(party => Filter(party, None)).toVector
    val allFilterParties = requestingParties

    val idPageConfig = IdQueryConfiguration(
      maxIdPageSize = maxIdsPerIdPage,
      idPageWorkingMemoryBytes = maxWorkingMemoryInBytesForIdPages / 3,
      filterSize = filters.size,
      idPageBufferSize = maxPagesPerPagesBuffer,
    )

    def streamConsumingIds_stakeholders(filter: Filter): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        pageConfig = idPageConfig,
        pageBufferSize = maxPagesPerPagesBuffer,
        initialStartOffset = firstEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          idFetchingLimiter_consuming.execute {
            dbDispatcher.executeSql(metrics.daml.index.db.getConsumingIds_stakeholdersFilter) {
              connection =>
                eventStorageBackend.fetchIds_consuming_stakeholders(
                  partyFilter = filter.party,
                  templateIdFilter = filter.templateId,
                  startExclusive = state.startOffset,
                  endInclusive = lastEventSequentialId,
                  limit = state.pageSize,
                )(connection)
            }
          }
        }
      )
    }

    def streamConsumingIds_nonStakeholderInformees(filter: Filter): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        pageConfig = idPageConfig,
        pageBufferSize = maxPagesPerPagesBuffer,
        initialStartOffset = firstEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          idFetchingLimiter_consuming.execute {
            dbDispatcher.executeSql(
              metrics.daml.index.db.getConsumingIds_nonStakeholderInformeesFilter
            ) { connection =>
              eventStorageBackend.fetchIds_consuming_nonStakeholderInformees(
                partyFilter = filter.party,
                startExclusive = state.startOffset,
                endInclusive = lastEventSequentialId,
                limit = state.pageSize,
              )(connection)
            }
          }
        }
      )
    }

    def streamCreateIds_stakeholders(filter: Filter): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        pageConfig = idPageConfig,
        pageBufferSize = maxPagesPerPagesBuffer,
        initialStartOffset = firstEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          idFetchingLimiter_create.execute {
            dbDispatcher.executeSql(metrics.daml.index.db.getCreateEventIds_stakeholdersFilter) {
              connection =>
                eventStorageBackend.fetchIds_create_stakeholders(
                  partyFilter = filter.party,
                  templateIdFilter = filter.templateId,
                  startExclusive = state.startOffset,
                  endInclusive = lastEventSequentialId,
                  limit = state.pageSize,
                )(connection)
            }
          }
        }
      )
    }

    def streamCreateIds_nonStakeholderInformees(filter: Filter): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        pageConfig = idPageConfig,
        pageBufferSize = maxPagesPerPagesBuffer,
        initialStartOffset = firstEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          idFetchingLimiter_create.execute {
            dbDispatcher.executeSql(
              metrics.daml.index.db.getCreateEventIds_nonStakeholderInformeesFilter
            ) { connection =>
              eventStorageBackend.fetchIds_create_nonStakeholderInformees(
                partyFilter = filter.party,
                startExclusive = state.startOffset,
                endInclusive = lastEventSequentialId,
                limit = state.pageSize,
              )(connection)
            }
          }
        }
      )
    }

    def streamNonConsumingIds_informees(filter: Filter): Source[Long, NotUsed] = {
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        pageConfig = idPageConfig,
        pageBufferSize = maxPagesPerPagesBuffer,
        initialStartOffset = firstEventSequentialId,
      )(
        fetchPage = (state: IdPaginationState) => {
          idFetchingLimiter_nonConsuming.execute {
            dbDispatcher.executeSql(metrics.daml.index.db.getNonConsumingEventIds_informeesFilter) {
              connection =>
                eventStorageBackend.fetchIds_nonConsuming_informees(
                  partyFilter = filter.party,
                  startExclusive = state.startOffset,
                  endInclusive = lastEventSequentialId,
                  limit = state.pageSize,
                )(connection)
            }
          }
        }
      )
    }

    def timedDeserialize(
        rawEvents: Vector[EventStorageBackend.Entry[Raw.TreeEvent]]
    ): Future[Vector[EventStorageBackend.Entry[TreeEvent]]] = {
      Timed.future(
        future = Future.traverse(rawEvents)(
          deserializeEntry(eventProjectionProperties, lfValueTranslation)
        ),
        timer = dbMetrics.getFlatTransactions.translationTimer,
      )
    }

    def fetchConsumingEvents(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.Entry[Raw.TreeEvent]]] = {
      eventFetchingLimiter
        .execute(
          dbDispatcher.executeSql(metrics.daml.index.db.getTransactionTrees) { implicit connection =>
            queryNonPruned.executeSql(
              query = eventStorageBackend.fetchTreeConsumingEvents(
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

    def fetchCreateEvents(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.Entry[Raw.TreeEvent]]] = {
      eventFetchingLimiter
        .execute(
          dbDispatcher.executeSql(metrics.daml.index.db.getTransactionTrees) { implicit connection =>
            queryNonPruned.executeSql(
              query = eventStorageBackend.fetchTreeCreateEvents(
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

    def fetchNonConsumingEvents(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.Entry[Raw.TreeEvent]]] = {
      eventFetchingLimiter
        .execute(
          dbDispatcher.executeSql(metrics.daml.index.db.getTransactionTrees) { implicit connection =>
            queryNonPruned.executeSql(
              query = eventStorageBackend.fetchTreeNonConsumingEvents(
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

    import scala.util.chaining._

    val consumingStream: Source[EventStorageBackend.Entry[Raw.TreeEvent], NotUsed] = {
      filters.map(streamConsumingIds_stakeholders) ++ filters.map(
        streamConsumingIds_nonStakeholderInformees
      )
    }
      .pipe(FilterTableACSReader.mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = eventsFetchingAndDecodingParallelism_mapAsync + 1,
        )
      )
      .async
      .addAttributes(
        Attributes.inputBuffer(
          initial = eventsFetchingAndDecodingParallelism_mapAsync,
          max = eventsFetchingAndDecodingParallelism_mapAsync,
        )
      )
      .mapAsync(eventsFetchingAndDecodingParallelism_mapAsync)(fetchConsumingEvents)
      .mapConcat(identity)

    val createStream: Source[EventStorageBackend.Entry[Raw.TreeEvent], NotUsed] = {
      filters.map(streamCreateIds_stakeholders) ++ filters.map(
        streamCreateIds_nonStakeholderInformees
      )
    }
      .pipe(FilterTableACSReader.mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = eventsFetchingAndDecodingParallelism_mapAsync + 1,
        )
      )
      .async
      .addAttributes(
        Attributes.inputBuffer(
          initial = eventsFetchingAndDecodingParallelism_mapAsync,
          max = eventsFetchingAndDecodingParallelism_mapAsync,
        )
      )
      .mapAsync(eventsFetchingAndDecodingParallelism_mapAsync)(fetchCreateEvents)
      .mapConcat(identity)

    val nonConsumingStream: Source[EventStorageBackend.Entry[Raw.TreeEvent], NotUsed] = filters
      .map(streamNonConsumingIds_informees)
      .pipe(FilterTableACSReader.mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = eventsFetchingAndDecodingParallelism_mapAsync + 1,
        )
      )
      .addAttributes(
        Attributes.inputBuffer(
          initial = eventsFetchingAndDecodingParallelism_mapAsync,
          max = eventsFetchingAndDecodingParallelism_mapAsync,
        )
      )
      .mapAsync(eventsFetchingAndDecodingParallelism_mapAsync)(fetchNonConsumingEvents)
      .mapConcat(identity)

    val ordering = Ordering.by[EventStorageBackend.Entry[Raw.TreeEvent], Long](_.eventSequentialId)

    val allEvents: Source[EventStorageBackend.Entry[Raw.TreeEvent], NotUsed] =
      consumingStream
        .mergeSorted(createStream)(ord = ordering)
        .mergeSorted(nonConsumingStream)(ord = ordering)

    TransactionsReader
      .groupContiguous(allEvents)(by = _.transactionId)
      .mapAsync(payloadProcessingParallelism)(timedDeserialize)
      .mapConcat { events =>
        val response = TransactionConversions.toGetTransactionTreesResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
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

}
