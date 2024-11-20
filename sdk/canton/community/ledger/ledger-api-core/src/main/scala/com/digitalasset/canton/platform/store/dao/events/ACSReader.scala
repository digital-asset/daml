// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.state_service.{
  ActiveContract,
  GetActiveContractsResponse,
  IncompleteAssigned,
  IncompleteUnassigned,
}
import com.daml.metrics.Timed
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.{SpanAttribute, Spans}
import com.digitalasset.canton.data.AbsoluteOffset
import com.digitalasset.canton.ledger.error.CommonErrors.ServerIsShuttingDown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawActiveContract,
  RawAssignEvent,
  RawCreatedEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForFlatTx
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.TransactionsReader.endSpanOnTermination
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
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Identifier
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

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
    config: ActiveContractsServiceStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    incompleteOffsets: (
        AbsoluteOffset,
        Option[Set[Ref.Party]],
        TraceContext,
    ) => FutureUnlessShutdown[Vector[AbsoluteOffset]],
    metrics: LedgerApiServerMetrics,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val dbMetrics = metrics.index.db

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  def streamActiveContracts(
      filteringConstraints: TemplatePartiesFilter,
      activeAt: (AbsoluteOffset, Long),
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, activeAtLong) = activeAt
    val span =
      Telemetry.Transactions.createSpan(tracer, activeAtOffset)(qualifiedNameOfCurrentFunc)
    val event =
      tracing.Event("contract", Map((SpanAttribute.Offset, activeAtLong.toString)))
    Spans.addEventToSpan(event, span)
    logger.debug(
      s"getActiveContracts($activeAtOffset, $filteringConstraints, $eventProjectionProperties)"
    )
    doStreamActiveContracts(
      filteringConstraints,
      activeAt,
      eventProjectionProperties,
    )
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamActiveContracts(
      filter: TemplatePartiesFilter,
      activeAt: (AbsoluteOffset, Long),
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, activeAtEventSeqId) = activeAt
    def withValidatedActiveAt[T](query: => T)(implicit connection: Connection) =
      queryValidRange.withOffsetNotBeforePruning(
        activeAtOffset,
        pruned =>
          ACSReader.acsBeforePruningErrorReason(
            acsOffset = activeAtOffset,
            prunedUpToOffset = pruned,
          ),
        ledgerEnd =>
          ACSReader.acsAfterLedgerEndErrorReason(
            acsOffset = activeAtOffset,
            ledgerEndOffset = ledgerEnd,
          ),
      )(query)

    val allFilterParties = filter.allFilterParties
    val decomposedFilters = FilterUtils.decomposeFilters(filter).toVector
    val createIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelIdCreateQueries, executionContext)
    val assignIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelIdCreateQueries, executionContext)
    val localPayloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelPayloadCreateQueries, executionContext)
    val idQueryPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = config.maxIdsPerIdPage,
      workingMemoryInBytesForIdPages =
        // to accommodate for the fact that we have queues for Assign and Create events as well
        config.maxWorkingMemoryInBytesForIdPages / 2,
      numOfDecomposedFilters = decomposedFilters.size,
      numOfPagesInIdPageBuffer = config.maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    def fetchCreateIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      paginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
      )((state: IdPaginationState) =>
        createIdQueriesLimiter.execute(
          globalIdQueriesLimiter.execute(
            dispatcher.executeSql(metrics.index.db.getActiveContractIdsForCreated) { connection =>
              val ids =
                eventStorageBackend.transactionStreamingQueries
                  .fetchIdsOfCreateEventsForStakeholder(
                    stakeholderO = filter.party,
                    templateIdO = filter.templateId,
                    startExclusive = state.fromIdExclusive,
                    endInclusive = activeAtEventSeqId,
                    limit = state.pageSize,
                  )(connection)
              logger.debug(
                s"ActiveContractIds for create events $filter returned #${ids.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              ids
            }
          )
        )
      )

    def fetchAssignIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      paginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
      )((state: IdPaginationState) =>
        assignIdQueriesLimiter.execute(
          globalIdQueriesLimiter.execute(
            dispatcher.executeSql(metrics.index.db.getActiveContractIdsForAssigned) { connection =>
              val ids =
                eventStorageBackend.fetchAssignEventIdsForStakeholder(
                  stakeholderO = filter.party,
                  templateId = filter.templateId,
                  startExclusive = state.fromIdExclusive,
                  endInclusive = activeAtEventSeqId,
                  limit = state.pageSize,
                )(connection)
              logger.debug(
                s"ActiveContractIds for assign events $filter returned #${ids.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              ids
            }
          )
        )
      )

    def fetchActiveCreatePayloads(
        ids: Iterable[Long]
    ): Future[Vector[RawActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(metrics.index.db.getActiveContractBatchForCreated) {
            implicit connection =>
              val result = withValidatedActiveAt(
                eventStorageBackend.activeContractCreateEventBatch(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAtEventSeqId,
                )(connection)
              )
              logger.debug(
                s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              result
          }
        )
      )

    def fetchActiveAssignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[RawActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(metrics.index.db.getActiveContractBatchForAssigned) {
            implicit connection =>
              val result = withValidatedActiveAt(
                eventStorageBackend.activeContractAssignEventBatch(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAtEventSeqId,
                )(connection)
              )
              logger.debug(
                s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              result
          }
        )
      )

    def fetchAssignIdsForOffsets(
        offsets: Iterable[AbsoluteOffset]
    ): Future[Vector[Long]] =
      globalIdQueriesLimiter.execute(
        dispatcher.executeSql(metrics.index.db.getAssingIdsForOffsets) { connection =>
          val ids =
            eventStorageBackend
              .lookupAssignSequentialIdByOffset(offsets.map(_.toHexString))(connection)
          logger.debug(
            s"Assign Ids for offsets returned #${ids.size} (from ${offsets.size}) ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          ids
        }
      )

    def fetchUnassignIdsForOffsets(
        offsets: Iterable[AbsoluteOffset]
    ): Future[Vector[Long]] =
      globalIdQueriesLimiter.execute(
        dispatcher.executeSql(metrics.index.db.getUnassingIdsForOffsets) { connection =>
          val ids =
            eventStorageBackend
              .lookupUnassignSequentialIdByOffset(offsets.map(_.toHexString))(connection)
          logger.debug(
            s"Unassign Ids for offsets returned #${ids.size} (from ${offsets.size}) ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          ids
        }
      )

    def fetchAssignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[Entry[RawAssignEvent]]] =
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        localPayloadQueriesLimiter.execute(
          globalPayloadQueriesLimiter.execute(
            dispatcher.executeSql(
              metrics.index.db.reassignmentStream.fetchEventAssignPayloads
            ) { implicit connection =>
              val result = withValidatedActiveAt(
                eventStorageBackend.assignEventBatch(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                )(connection)
              )
              logger.debug(
                s"assignEventBatch returned ${ids.size}/${result.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              result
            }
          )
        )

    def fetchUnassignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[Entry[RawUnassignEvent]]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(
            metrics.index.db.reassignmentStream.fetchEventUnassignPayloads
          ) { implicit connection =>
            val result = withValidatedActiveAt(
              eventStorageBackend.unassignEventBatch(
                eventSequentialIds = ids,
                allFilterParties = allFilterParties,
              )(connection)
            )
            logger.debug(
              s"assignEventBatch returned ${ids.size}/${result.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
            result
          }
        )
      )

    def fetchCreateIdsForContractIds(
        contractIds: Iterable[String]
    ): Future[Vector[Long]] =
      globalIdQueriesLimiter.execute(
        dispatcher.executeSql(metrics.index.db.getCreateIdsForContractIds) { connection =>
          val ids =
            eventStorageBackend.lookupCreateSequentialIdByContractId(contractIds)(connection)
          logger.debug(
            s"Create Ids for contract IDs returned #${ids.size} (from ${contractIds.size}) ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          ids
        }
      )

    def fetchAssignIdsForContractIds(
        contractIds: Iterable[String]
    ): Future[Vector[Long]] =
      if (contractIds.isEmpty) Future.successful(Vector.empty)
      else
        globalIdQueriesLimiter.execute(
          dispatcher.executeSql(metrics.index.db.getAssignIdsForContractIds) { connection =>
            val ids =
              eventStorageBackend.lookupAssignSequentialIdByContractId(contractIds)(connection)
            logger.debug(
              s"Assign Ids for contract IDs returned #${ids.size} (from ${contractIds.size}) ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
            ids
          }
        )

    def fetchCreatePayloads(
        ids: Iterable[Long]
    ): Future[Vector[RawCreatedEvent]] =
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        globalPayloadQueriesLimiter.execute(
          dispatcher
            .executeSql(metrics.index.db.flatTxStream.fetchEventCreatePayloads) {
              implicit connection =>
                val result = withValidatedActiveAt(
                  eventStorageBackend.transactionStreamingQueries
                    .fetchEventPayloadsFlat(EventPayloadSourceForFlatTx.Create)(
                      eventSequentialIds = ids,
                      allFilterParties = allFilterParties,
                    )(connection)
                )
                logger.debug(
                  s"fetchEventPayloadsFlat for Create returned ${ids.size}/${result.size} ${ids.lastOption
                      .map(last => s"until $last")
                      .getOrElse("")}"
                )
                result.view
                  .map(_.event)
                  .collect { case created: RawCreatedEvent =>
                    created
                  }
                  .toVector
            }
        )

    def fetchCreatedEventsForUnassignedBatch(batch: Seq[Entry[RawUnassignEvent]]): Future[
      Seq[(Entry[RawUnassignEvent], RawCreatedEvent)]
    ] =
      for {
        createdIds <- fetchCreateIdsForContractIds(batch.map(_.event.contractId).distinct)
        createdPayloads <- fetchCreatePayloads(createdIds)
        rawCreatedFromCreatedResults = createdPayloads.iterator
          .map(event => event.contractId -> event)
          .toMap
        missingContractIds = batch
          .map(_.event.contractId)
          .filterNot(rawCreatedFromCreatedResults.contains)
          .distinct
        assignedIds <- fetchAssignIdsForContractIds(missingContractIds)
        assignedPayloads <- fetchAssignPayloads(assignedIds)
        rawCreatedFromAssignedResults = assignedPayloads.iterator
          .map(_.event.rawCreatedEvent)
          .map(rawCreatedEvent => rawCreatedEvent.contractId -> rawCreatedEvent)
          .toMap
      } yield batch.flatMap { rawUnassignEntry =>
        val contractId = rawUnassignEntry.event.contractId
        rawCreatedFromCreatedResults
          .get(contractId)
          .orElse {
            logger.debug(
              s"For an IncompleteUnassigned event (offset:${rawUnassignEntry.offset} workflow-id:${rawUnassignEntry.workflowId} contract-id:${rawUnassignEntry.event.contractId} template-id:${rawUnassignEntry.event.templateId} reassignment-counter:${rawUnassignEntry.event.reassignmentCounter}) there is no CreatedEvent available."
            )
            rawCreatedFromAssignedResults.get(contractId)
          }
          .orElse {
            logger.warn(
              s"For an IncompleteUnassigned event (offset:${rawUnassignEntry.offset} workflow-id:${rawUnassignEntry.workflowId} contract-id:${rawUnassignEntry.event.contractId} template-id:${rawUnassignEntry.event.templateId} reassignment-counter:${rawUnassignEntry.event.reassignmentCounter}) there is no CreatedEvent or AssignedEvent available. This entry will be dropped from the result."
            )
            None
          }
          .toList
          .map(rawUnassignEntry -> _)
      }

    val stringWildcardParties = filter.templateWildcardParties.map(_.map(_.toString))
    val stringTemplateFilters = filter.relation.map { case (key, value) =>
      key -> value
    }
    def eventMeetsConstraints(templateId: Identifier, witnesses: Set[String]): Boolean =
      stringWildcardParties.fold(true)(_.exists(witnesses)) || (
        stringTemplateFilters.get(templateId) match {
          case Some(Some(filterParties)) => filterParties.exists(witnesses)
          case Some(None) => true // party wildcard
          case None =>
            false // templateId is not in the filter
        }
      )

    def unassignMeetsConstraints(rawUnassignEntry: Entry[RawUnassignEvent]): Boolean =
      eventMeetsConstraints(
        rawUnassignEntry.event.templateId,
        rawUnassignEntry.event.witnessParties,
      )
    def assignMeetsConstraints(rawAssignEntry: Entry[RawAssignEvent]): Boolean =
      eventMeetsConstraints(
        rawAssignEntry.event.rawCreatedEvent.templateId,
        rawAssignEntry.event.rawCreatedEvent.witnessParties,
      )

    // Pekko requires for this buffer's size to be a power of two.
    val inputBufferSize =
      Utils.largestSmallerOrEqualPowerOfTwo(config.maxParallelPayloadCreateQueries)

    val activeFromCreatePipe =
      decomposedFilters
        .map(fetchCreateIds)
        .pipe(EventIdsUtils.sortAndDeduplicateIds)
        .batchN(
          maxBatchSize = config.maxPayloadsPerPayloadsPage,
          maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
        )
        .async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(config.maxParallelPayloadCreateQueries)(fetchActiveCreatePayloads)
        .mapConcat(identity)
    val activeFromAssignPipe =
      decomposedFilters
        .map(fetchAssignIds)
        .pipe(EventIdsUtils.sortAndDeduplicateIds)
        .batchN(
          maxBatchSize = config.maxPayloadsPerPayloadsPage,
          maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
        )
        .async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(config.maxParallelPayloadCreateQueries)(fetchActiveAssignPayloads)
        .mapConcat(identity)

    activeFromCreatePipe
      .mergeSorted(activeFromAssignPipe)(Ordering.by(_.eventSequentialId))
      .mapAsync(config.contractProcessingParallelism)(
        toApiResponseActiveContract(_, eventProjectionProperties)
      )
      .concatLazy(
        Source.lazyFutureSource(() =>
          incompleteOffsets(
            activeAtOffset,
            filter.allFilterParties,
            loggingContext.traceContext,
          ).map { offsets =>
            def incompleteOffsetPages: () => Iterator[Vector[AbsoluteOffset]] =
              () => offsets.sliding(config.maxIncompletePageSize, config.maxIncompletePageSize)

            val incompleteAssigned: Source[(String, GetActiveContractsResponse), NotUsed] =
              Source
                .fromIterator(incompleteOffsetPages)
                .mapAsync(config.maxParallelIdCreateQueries)(
                  fetchAssignIdsForOffsets
                )
                .mapConcat(identity)
                .grouped(config.maxIncompletePageSize)
                .mapAsync(config.maxParallelPayloadCreateQueries)(
                  fetchAssignPayloads
                )
                .mapConcat(_.filter(assignMeetsConstraints))
                .mapAsync(config.contractProcessingParallelism)(
                  toApiResponseIncompleteAssigned(eventProjectionProperties)
                )

            val incompleteUnassigned: Source[(String, GetActiveContractsResponse), NotUsed] =
              Source
                .fromIterator(incompleteOffsetPages)
                .mapAsync(config.maxParallelIdCreateQueries)(
                  fetchUnassignIdsForOffsets
                )
                .mapConcat(identity)
                .grouped(config.maxIncompletePageSize)
                .mapAsync(config.maxParallelPayloadCreateQueries)(
                  fetchUnassignPayloads
                )
                .mapConcat(_.filter(unassignMeetsConstraints))
                .grouped(config.maxIncompletePageSize)
                .mapAsync(config.maxParallelPayloadCreateQueries)(
                  fetchCreatedEventsForUnassignedBatch
                )
                .mapConcat(identity)
                .mapAsync(config.contractProcessingParallelism)(
                  toApiResponseIncompleteUnassigned(eventProjectionProperties)
                )

            incompleteAssigned
              .mergeSorted(incompleteUnassigned)(Ordering.by(_._1))
              .map(_._2)
          }.onShutdown {
            Source.failed(
              ServerIsShuttingDown.Reject().asGrpcError
            )
          }
        )
      )
  }

  private def toApiResponseActiveContract(
      rawActiveContract: RawActiveContract,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[GetActiveContractsResponse] =
    Timed.future(
      future = Future.delegate(
        lfValueTranslation
          .deserializeRaw(eventProjectionProperties)(
            rawActiveContract.rawCreatedEvent
          )
          .map(createdEvent =>
            GetActiveContractsResponse(
              workflowId = rawActiveContract.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.ActiveContract(
                ActiveContract(
                  createdEvent = Some(createdEvent),
                  domainId = rawActiveContract.domainId,
                  reassignmentCounter = rawActiveContract.reassignmentCounter,
                )
              ),
            )
          )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )

  private def toApiResponseIncompleteAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawAssignEntry: Entry[RawAssignEvent]
  )(implicit lc: LoggingContextWithTrace): Future[(String, GetActiveContractsResponse)] =
    Timed.future(
      future = Future.delegate(
        lfValueTranslation
          .deserializeRaw(eventProjectionProperties)(
            rawAssignEntry.event.rawCreatedEvent
          )
          .map(createdEvent =>
            rawAssignEntry.offset -> GetActiveContractsResponse(
              workflowId = rawAssignEntry.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
                IncompleteAssigned(
                  Some(TransactionsReader.toAssignedEvent(rawAssignEntry.event, createdEvent))
                )
              ),
            )
          )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )

  private def toApiResponseIncompleteUnassigned(
      eventProjectionProperties: EventProjectionProperties
  )(
      rawUnassignEntryWithCreate: (Entry[RawUnassignEvent], RawCreatedEvent)
  )(implicit lc: LoggingContextWithTrace): Future[(String, GetActiveContractsResponse)] = {
    val (rawUnassignEntry, rawCreate) = rawUnassignEntryWithCreate
    Timed.future(
      future = lfValueTranslation
        .deserializeRaw(eventProjectionProperties)(rawCreate)
        .map(createdEvent =>
          rawUnassignEntry.offset -> GetActiveContractsResponse(
            workflowId = rawUnassignEntry.workflowId.getOrElse(""),
            contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteUnassigned(
              IncompleteUnassigned(
                createdEvent = Some(createdEvent),
                unassignedEvent = Some(TransactionsReader.toUnassignedEvent(rawUnassignEntry.event)),
              )
            ),
          )
        ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )
  }

}

object ACSReader {

  def acsBeforePruningErrorReason(
      acsOffset: AbsoluteOffset,
      prunedUpToOffset: AbsoluteOffset,
  ): String =
    s"Active contracts request at offset ${acsOffset.unwrap} precedes pruned offset ${prunedUpToOffset.unwrap}"

  def acsAfterLedgerEndErrorReason(
      acsOffset: AbsoluteOffset,
      ledgerEndOffset: Option[AbsoluteOffset],
  ): String =
    s"Active contracts request at offset ${acsOffset.unwrap} preceded by ledger end offset ${ledgerEndOffset
        .fold(0L)(_.unwrap)}"

}
