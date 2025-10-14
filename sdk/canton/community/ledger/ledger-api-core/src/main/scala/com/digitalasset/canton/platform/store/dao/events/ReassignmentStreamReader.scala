// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawAssignEventLegacy,
  RawReassignmentEventLegacy,
  RawUnassignEventLegacy,
  SequentialIdBatch,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.PaginationInput
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
import com.digitalasset.canton.platform.{FatContract, TemplatePartiesFilter}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{NameTypeConRef, Party}
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
    contractStore: ContractStore,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val dbMetrics = metrics.index.db

  private val directEC = DirectExecutionContext(logger)

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
        streamName: String,
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        maxOutputBatchCount: Int,
        metric: DatabaseMetrics,
        idDbQuery: IdDbQuery,
    ): Source[Iterable[Long], NotUsed] =
      decomposedFilters
        .map { filter =>
          paginatingAsyncStream.streamIdsFromSeekPaginationWithoutIdFilter(
            idStreamName = s"Update IDs for $streamName $filter",
            idPageSizing = idPageSizing,
            idPageBufferSize = maxPagesPerIdPagesBuffer,
            initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
            initialEndInclusive = queryRange.endInclusiveEventSeqId,
          )(
            idDbQuery.fetchIds(
              stakeholder = filter.party,
              templateIdO = filter.templateId,
            )
          )(
            executeIdQuery = f =>
              maxParallelIdQueriesLimiter.execute {
                globalIdQueriesLimiter.execute {
                  dbDispatcher.executeSql(metric)(f)
                }
              }
          )
        }
        .pipe(EventIdsUtils.sortAndDeduplicateIds)
        .batchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = maxOutputBatchCount,
        )

    def fetchPayloads[T <: RawReassignmentEventLegacy](
        ids: Source[Iterable[Long], NotUsed],
        maxParallelPayloadQueries: Int,
        dbMetric: DatabaseMetrics,
        payloadDbQuery: PayloadDbQuery[Entry[T]],
        deserialize: Seq[(Entry[T], Option[FatContract])] => Future[Option[Reassignment]],
    ): Source[Reassignment, NotUsed] = {
      // Pekko requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      val serializedPayloads = ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              queryValidRange.withRangeNotPruned(
                minOffsetInclusive = queryRange.startInclusiveOffset,
                maxOffsetInclusive = queryRange.endInclusiveOffset,
                errorPruning = (prunedOffset: Offset) =>
                  s"Reassignment request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
                errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                  s"Reassignment request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                      .fold(0L)(_.unwrap)}",
              ) {
                dbDispatcher
                  .executeSql(dbMetric)(
                    payloadDbQuery.fetchPayloads(
                      eventSequentialIds = Ids(ids),
                      allFilterParties = filteringConstraints.allFilterParties,
                    )
                  )
                  .flatMap { payloads =>
                    val internalContractIds =
                      payloads.map(_.event).collect { case assign: RawAssignEventLegacy =>
                        assign.rawCreatedEvent.internalContractId
                      }
                    for {
                      contractsM <- contractStore
                        .lookupBatchedNonCached(internalContractIds)
                        .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
                    } yield payloads.map { payload =>
                      payload.event match {
                        case assign: RawAssignEventLegacy =>
                          payload -> contractsM
                            .get(assign.rawCreatedEvent.internalContractId)
                            .map(_.inst)
                        case _: RawUnassignEventLegacy =>
                          payload -> None
                      }
                    }
                  }
              }
            }
          }
        )
        .mapConcat(identity)
      UpdateReader
        .groupContiguous(serializedPayloads)(by = _._1.updateId)
        .mapAsync(deserializationProcessingParallelism)(t =>
          deserializationQueriesLimiter.execute(
            deserialize(t)
          )
        )
        .mapConcat(_.toList)
    }

    val idsAssign =
      fetchIds(
        streamName = "assigned events",
        maxParallelIdQueriesLimiter = assignedEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadAssignQueries + 1,
        metric = dbMetrics.reassignmentStream.fetchEventAssignIdsStakeholderLegacy,
        idDbQuery = eventStorageBackend.fetchAssignEventIdsForStakeholderLegacy,
      )
    val idsUnassign =
      fetchIds(
        streamName = "unassigned events",
        maxParallelIdQueriesLimiter = unassignedEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadUnassignQueries + 1,
        metric = dbMetrics.reassignmentStream.fetchEventUnassignIdsStakeholderLegacy,
        idDbQuery = eventStorageBackend.fetchUnassignEventIdsForStakeholderLegacy,
      )
    val payloadsAssign =
      fetchPayloads(
        ids = idsAssign,
        maxParallelPayloadQueries = maxParallelPayloadAssignQueries,
        dbMetric = dbMetrics.reassignmentStream.fetchEventAssignPayloadsLegacy,
        payloadDbQuery = eventStorageBackend.assignEventBatchLegacy,
        deserialize = toApiAssigned(eventProjectionProperties),
      )
    val payloadsUnassign =
      fetchPayloads(
        ids = idsUnassign,
        maxParallelPayloadQueries = maxParallelPayloadUnassignQueries,
        dbMetric = dbMetrics.reassignmentStream.fetchEventUnassignPayloadsLegacy,
        payloadDbQuery = eventStorageBackend.unassignEventBatchLegacy,
        deserialize = toApiUnassigned,
      )

    payloadsAssign
      .mergeSorted(payloadsUnassign)(Ordering.by(_.offset))
      .map(response => Offset.tryFromLong(response.offset) -> response)
  }

  private def toApiUnassigned(
      rawUnassignEntries: Seq[(Entry[RawUnassignEventLegacy], Option[FatContract])]
  ): Future[Option[Reassignment]] =
    Timed.future(
      future = Future.successful(UpdateReader.toApiUnassigned(rawUnassignEntries.map(_._1))),
      timer = dbMetrics.reassignmentStream.translationTimer,
    )

  private def toApiAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawAssignEntries: Seq[(Entry[RawAssignEventLegacy], Option[FatContract])]
  )(implicit lc: LoggingContextWithTrace): Future[Option[Reassignment]] =
    Timed.future(
      future = Future.delegate {
        implicit val executionContext: ExecutionContext =
          directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
        UpdateReader.toApiAssigned(eventProjectionProperties, lfValueTranslation)(rawAssignEntries)
      },
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
        templateIdO: Option[NameTypeConRef],
    ): Connection => PaginationInput => Vector[Long]
  }

  @FunctionalInterface
  trait PayloadDbQuery[T] {
    def fetchPayloads(
        eventSequentialIds: SequentialIdBatch,
        allFilterParties: Option[Set[Ref.Party]],
    ): Connection => Vector[T]
  }
}
