// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawAssignEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.ReassignmentStreamReader.{
  IdDbQuery,
  PayloadDbQuery,
  ReassignmentStreamQueryParams,
}
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  PaginatingAsyncStream,
}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
}
import com.digitalasset.canton.platform.{ApiOffset, TemplatePartiesFilter}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class ReassignmentStreamReader(
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val dbMetrics = metrics.daml.index.db

  def streamReassignments(reassignmentStreamQueryParams: ReassignmentStreamQueryParams)(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, Reassignment), NotUsed] = {
    import reassignmentStreamQueryParams.*
    logger.debug(
      s"streamReassignments(${queryRange.startExclusiveOffset}, ${queryRange.endInclusiveOffset}, $filteringConstraints, $eventProjectionProperties)"
    )

    val assignedEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdAssignQueries, executionContext)
    val unassignedEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdUnassignQueries, executionContext)

    def fetchIds(
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        maxOutputBatchCount: Int,
        metric: DatabaseMetrics,
        idDbQuery: IdDbQuery,
    ): Source[Iterable[Long], NotUsed] = {
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
                  dbDispatcher.executeSql(metric) {
                    idDbQuery.fetchIds(
                      stakeholder = filter.party,
                      templateIdO = filter.templateId,
                      startExclusive = state.fromIdExclusive,
                      endInclusive = queryRange.endInclusiveEventSeqId,
                      limit = state.pageSize,
                    )
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
    }

    def fetchPayloads[T](
        ids: Source[Iterable[Long], NotUsed],
        maxParallelPayloadQueries: Int,
        dbMetric: DatabaseMetrics,
        payloadDbQuery: PayloadDbQuery[T],
        deserialize: T => Future[Reassignment],
    ): Source[Reassignment, NotUsed] = {
      // Pekko requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              dbDispatcher.executeSql(dbMetric) { implicit connection =>
                queryNonPruned.executeSql(
                  query = payloadDbQuery.fetchPayloads(
                    eventSequentialIds = ids,
                    allFilterParties = filteringConstraints.allFilterParties,
                  )(connection),
                  minOffsetExclusive = queryRange.startExclusiveOffset,
                  error = (prunedOffset: Offset) =>
                    s"Reassignment request from ${queryRange.startExclusiveOffset.toHexString} to ${queryRange.endInclusiveOffset.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
                )
              }(LoggingContextWithTrace(TraceContext.empty))
            }
          }
        )
        .mapConcat(identity)
        .mapAsync(deserializationProcessingParallelism)(t =>
          deserializationQueriesLimiter.execute(
            deserialize(t)
          )
        )
    }

    val idsAssign =
      fetchIds(
        maxParallelIdQueriesLimiter = assignedEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadAssignQueries + 1,
        metric = dbMetrics.reassignmentStream.fetchEventAssignIdsStakeholder,
        idDbQuery = eventStorageBackend.fetchAssignEventIdsForStakeholder,
      )
    val idsUnassign =
      fetchIds(
        maxParallelIdQueriesLimiter = unassignedEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadUnassignQueries + 1,
        metric = dbMetrics.reassignmentStream.fetchEventUnassignIdsStakeholder,
        idDbQuery = eventStorageBackend.fetchAssignEventIdsForStakeholder,
      )
    val payloadsAssign =
      fetchPayloads(
        ids = idsAssign,
        maxParallelPayloadQueries = maxParallelPayloadAssignQueries,
        dbMetric = dbMetrics.reassignmentStream.fetchEventAssignPayloads,
        payloadDbQuery = eventStorageBackend.assignEventBatch,
        deserialize = toApiAssigned(eventProjectionProperties),
      )
    val payloadsUnassign =
      fetchPayloads(
        ids = idsUnassign,
        maxParallelPayloadQueries = maxParallelPayloadUnassignQueries,
        dbMetric = dbMetrics.reassignmentStream.fetchEventUnassignPayloads,
        payloadDbQuery = eventStorageBackend.unassignEventBatch,
        deserialize = toApiUnassigned,
      )

    payloadsAssign
      .mergeSorted(payloadsUnassign)(Ordering.by(_.offset))
      .map(response => ApiOffset.assertFromString(response.offset) -> response)
  }

  private def toApiUnassigned(rawUnassignEvent: RawUnassignEvent): Future[Reassignment] = {
    Timed.future(
      future = Future {
        Reassignment(
          updateId = rawUnassignEvent.updateId,
          commandId = rawUnassignEvent.commandId.getOrElse(""),
          workflowId = rawUnassignEvent.workflowId.getOrElse(""),
          offset = rawUnassignEvent.offset,
          event = Reassignment.Event.UnassignedEvent(
            TransactionsReader.toUnassignedEvent(rawUnassignEvent)
          ),
        )
      },
      timer = dbMetrics.reassignmentStream.translationTimer,
    )
  }

  private def toApiAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawAssignEvent: RawAssignEvent
  )(implicit lc: LoggingContextWithTrace): Future[Reassignment] = {
    Timed.future(
      future = Future.delegate(
        TransactionsReader
          .deserializeRawCreatedEvent(lfValueTranslation, eventProjectionProperties)(
            rawAssignEvent.rawCreatedEvent
          )
          .map(createdEvent =>
            Reassignment(
              updateId = rawAssignEvent.rawCreatedEvent.updateId,
              commandId = rawAssignEvent.commandId.getOrElse(""),
              workflowId = rawAssignEvent.workflowId.getOrElse(""),
              offset = rawAssignEvent.offset,
              event = Reassignment.Event.AssignedEvent(
                TransactionsReader.toAssignedEvent(
                  rawAssignEvent,
                  createdEvent,
                )
              ),
            )
          )
      ),
      timer = dbMetrics.reassignmentStream.translationTimer,
    )
  }

}

object ReassignmentStreamReader {
  final case class ReassignmentStreamQueryParams(
      queryRange: EventsRange,
      filteringConstraints: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
      payloadQueriesLimiter: ConcurrencyLimiter,
      deserializationQueriesLimiter: ConcurrencyLimiter,
      idPageSizing: IdPageSizing,
      decomposedFilters: Vector[DecomposedFilter],
      maxParallelIdAssignQueries: Int,
      maxParallelIdUnassignQueries: Int,
      maxPagesPerIdPagesBuffer: Int,
      maxPayloadsPerPayloadsPage: Int,
      maxParallelPayloadAssignQueries: Int,
      maxParallelPayloadUnassignQueries: Int,
      deserializationProcessingParallelism: Int,
  )

  @FunctionalInterface
  trait IdDbQuery {
    def fetchIds(
        stakeholder: Party,
        templateIdO: Option[Ref.Identifier],
        startExclusive: Long,
        endInclusive: Long,
        limit: Int,
    ): Connection => Vector[Long]
  }

  @FunctionalInterface
  trait PayloadDbQuery[T] {
    def fetchPayloads(
        eventSequentialIds: Iterable[Long],
        allFilterParties: Set[Ref.Party],
    ): Connection => Vector[T]
  }
}
