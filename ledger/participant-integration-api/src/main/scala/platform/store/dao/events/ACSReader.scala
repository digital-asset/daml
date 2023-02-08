// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.Attributes
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.TemplatePartiesFilter
import com.daml.platform.configuration.AcsStreamsConfig
import com.daml.platform.store.dao.{DbDispatcher, EventProjectionProperties, PaginatingAsyncStream}
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.daml.platform.store.dao.events.EventsTable.TransactionConversions
import com.daml.platform.store.dao.events.TransactionsReader.{
  deserializeEntry,
  endSpanOnTermination,
}
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter, Telemetry}
import com.daml.tracing
import com.daml.tracing.{SpanAttribute, Spans}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** Streams ACS events (active contracts) in a two step process consisting of:
  * 1) fetching event sequential ids of the active contracts based on the filtering constraints,
  * 2) fetching the active contracts based on the fetched event sequential ids.
  *
  * Details:
  * An input filtering constraint (consisting of parties and template ids) is converted into
  * decomposed filtering constraints (a constraint with exactly one party and at most one template id).
  * For each decomposed filter, the matching event sequential ids are fetched in parallel and then merged into
  * a strictly increasing sequence. The elements from this sequence are then batched and the batch ids serve as
  * the input to the payload fetching step.
  */
class ACSReader(
    config: AcsStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext) {
  private val logger = ContextualizedLogger.get(getClass)
  private val dbMetrics = metrics.daml.index.db

  def streamActiveContracts(
      filteringConstraints: TemplatePartiesFilter,
      activeAt: (Offset, Long),
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, _) = activeAt
    val span =
      Telemetry.Transactions.createSpan(activeAtOffset)(qualifiedNameOfCurrentFunc)
    logger.debug(
      s"getActiveContracts($activeAtOffset, $filteringConstraints, $eventProjectionProperties)"
    )
    doStreamActiveContracts(
      filteringConstraints,
      activeAt,
      eventProjectionProperties,
    )
      .wireTap(getActiveContractsResponse => {
        val event =
          tracing.Event("contract", Map((SpanAttribute.Offset, getActiveContractsResponse.offset)))
        Spans.addEventToSpan(event, span)
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamActiveContracts(
      filter: TemplatePartiesFilter,
      activeAt: (Offset, Long),
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContext
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val allFilterParties = filter.allFilterParties
    val decomposedFilters = FilterUtils.decomposeFilters(filter).toVector
    val idQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelIdCreateQueries, executionContext)
    val idQueryPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = config.maxIdsPerIdPage,
      workingMemoryInBytesForIdPages = config.maxWorkingMemoryInBytesForIdPages,
      numOfDecomposedFilters = decomposedFilters.size,
      numOfPagesInIdPageBuffer = config.maxPagesPerIdPagesBuffer,
    )

    def fetchIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
      )((state: IdPaginationState) =>
        idQueriesLimiter.execute(
          globalIdQueriesLimiter.execute(
            dispatcher.executeSql(metrics.daml.index.db.getActiveContractIds) { connection =>
              val ids =
                eventStorageBackend.transactionStreamingQueries
                  .fetchIdsOfCreateEventsForStakeholder(
                    stakeholder = filter.party,
                    templateIdO = filter.templateId,
                    startExclusive = state.fromIdExclusive,
                    endInclusive = activeAt._2,
                    limit = state.pageSize,
                  )(connection)
              logger.debug(
                s"getActiveContractIds $filter returned #${ids.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              ids
            }
          )
        )
      )

    def fetchPayloads(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] =
      globalPayloadQueriesLimiter.execute(
        dispatcher.executeSql(metrics.daml.index.db.getActiveContractBatch) { connection =>
          val result = queryNonPruned.executeSql(
            eventStorageBackend.activeContractEventBatch(
              eventSequentialIds = ids,
              allFilterParties = allFilterParties,
              endInclusive = activeAt._2,
            )(connection),
            activeAt._1,
            pruned =>
              ACSReader.acsBeforePruningErrorReason(
                acsOffset = activeAt._1.toHexString,
                prunedUpToOffset = pruned.toHexString,
              ),
          )(connection, implicitly)
          logger.debug(
            s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          result
        }
      )

    // Akka requires for this buffer's size to be a power of two.
    val inputBufferSize =
      Utils.largestSmallerOrEqualPowerOfTwo(config.maxParallelPayloadCreateQueries)
    decomposedFilters
      .map(fetchIds)
      .pipe(EventIdsUtils.sortAndDeduplicateIds)
      .via(
        BatchN(
          maxBatchSize = config.maxPayloadsPerPayloadsPage,
          maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
        )
      )
      .async
      .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
      .mapAsync(config.maxParallelPayloadCreateQueries)(fetchPayloads)
      .mapAsync(config.contractProcessingParallelism)(
        deserializeLfValues(_, eventProjectionProperties)
      )
      .mapConcat(
        TransactionConversions.toGetActiveContractsResponse(_)(
          new DamlContextualizedErrorLogger(logger, loggingContext, None)
        )
      )
  }

  private def deserializeLfValues(
      rawEvents: Vector[EventStorageBackend.Entry[Raw.FlatEvent]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContext): Future[Vector[EventStorageBackend.Entry[Event]]] = {
    Timed.future(
      future = Future.delegate(
        Future.traverse(rawEvents)(
          deserializeEntry(eventProjectionProperties, lfValueTranslation)
        )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )
  }

}

object ACSReader {

  def acsBeforePruningErrorReason(acsOffset: String, prunedUpToOffset: String): String =
    s"Active contracts request at offset ${acsOffset} precedes pruned offset ${prunedUpToOffset}"

}
