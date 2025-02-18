// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
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
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
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
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val dbMetrics = metrics.index.db

  def streamReassignments(reassignmentStreamQueryParams: ReassignmentStreamQueryParams)(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, Reassignment), NotUsed] = {
    import reassignmentStreamQueryParams.*
    logger.debug(
      s"streamReassignments(${queryRange.startInclusiveOffset}, ${queryRange.endInclusiveOffset}, $filteringConstraints, $eventProjectionProperties)"
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
    ): Source[Iterable[Long], NotUsed] =
      decomposedFilters
        .map { filter =>
          paginatingAsyncStream.streamIdsFromSeekPagination(
            idPageSizing = idPageSizing,
            idPageBufferSize = maxPagesPerIdPagesBuffer,
            initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
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
                queryValidRange.withRangeNotPruned(
                  minOffsetInclusive = queryRange.startInclusiveOffset,
                  maxOffsetInclusive = queryRange.endInclusiveOffset,
                  errorPruning = (prunedOffset: Offset) =>
                    s"Reassignment request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
                  errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                    s"Reassignment request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                        .fold(0L)(_.unwrap)}",
                ) {
                  payloadDbQuery.fetchPayloads(
                    eventSequentialIds = ids,
                    allFilterParties = filteringConstraints.allFilterParties,
                  )(connection)
                }
              }
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
        idDbQuery = eventStorageBackend.fetchUnassignEventIdsForStakeholder,
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
      .map(response => Offset.tryFromLong(response.offset) -> response)
  }

  private def toApiUnassigned(rawUnassignEntry: Entry[RawUnassignEvent]): Future[Reassignment] =
    Timed.future(
      future = Future {
        Reassignment(
          updateId = rawUnassignEntry.updateId,
          commandId = rawUnassignEntry.commandId.getOrElse(""),
          workflowId = rawUnassignEntry.workflowId.getOrElse(""),
          offset = rawUnassignEntry.offset,
          event = Reassignment.Event.UnassignedEvent(
            UpdateReader.toUnassignedEvent(rawUnassignEntry.event)
          ),
          recordTime = Some(TimestampConversion.fromLf(rawUnassignEntry.recordTime)),
        )
      },
      timer = dbMetrics.reassignmentStream.translationTimer,
    )

  private def toApiAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawAssignEntry: Entry[RawAssignEvent]
  )(implicit lc: LoggingContextWithTrace): Future[Reassignment] =
    Timed.future(
      future = Future.delegate(
        lfValueTranslation
          .deserializeRaw(eventProjectionProperties)(
            rawAssignEntry.event.rawCreatedEvent
          )
          .map(createdEvent =>
            Reassignment(
              updateId = rawAssignEntry.updateId,
              commandId = rawAssignEntry.commandId.getOrElse(""),
              workflowId = rawAssignEntry.workflowId.getOrElse(""),
              offset = rawAssignEntry.offset,
              event = Reassignment.Event.AssignedEvent(
                UpdateReader.toAssignedEvent(
                  rawAssignEntry.event,
                  createdEvent,
                )
              ),
              recordTime = Some(TimestampConversion.fromLf(rawAssignEntry.recordTime)),
            )
          )
      ),
      timer = dbMetrics.reassignmentStream.translationTimer,
    )

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
        stakeholder: Option[Party],
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
        allFilterParties: Option[Set[Ref.Party]],
    ): Connection => Vector[T]
  }
}
