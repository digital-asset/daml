// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.state_service.{
  ActiveContract,
  GetActiveContractsResponse,
  IncompleteAssigned,
  IncompleteUnassigned,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.NodeId
import com.daml.metrics.Timed
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.{SpanAttribute, Spans}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawActiveContract,
  RawAssignEvent,
  RawCreatedEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForFlatTx
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.TransactionsReader.{
  deserializeEntry,
  endSpanOnTermination,
}
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
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

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
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    incompleteOffsets: (Offset, Set[Ref.Party], TraceContext) => Future[Vector[Offset]],
    metrics: Metrics,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val dbMetrics = metrics.index.db

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  def streamActiveContracts(
      filteringConstraints: TemplatePartiesFilter,
      activeAt: (Offset, Long),
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, _) = activeAt
    val span =
      Telemetry.Transactions.createSpan(tracer, activeAtOffset)(qualifiedNameOfCurrentFunc)
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
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, activeAtEventSeqId) = activeAt

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
        // to accomodate for the fact that we have queues for Assign and Create events as well
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
                    stakeholder = filter.party,
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
                  stakeholder = filter.party,
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
    ): Future[Vector[EventStorageBackend.RawActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(metrics.index.db.getActiveContractBatchForCreated) { connection =>
            val result = queryNonPruned.executeSql(
              eventStorageBackend.activeContractCreateEventBatchV2(
                eventSequentialIds = ids,
                allFilterParties = allFilterParties,
                endInclusive = activeAtEventSeqId,
              )(connection),
              activeAtOffset,
              pruned =>
                ACSReader.acsBeforePruningErrorReason(
                  acsOffset = activeAtOffset.toHexString,
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
      )

    def fetchActiveAssignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.RawActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(metrics.index.db.getActiveContractBatchForAssigned) { connection =>
            val result = queryNonPruned.executeSql(
              eventStorageBackend.activeContractAssignEventBatch(
                eventSequentialIds = ids,
                allFilterParties = allFilterParties,
                endInclusive = activeAtEventSeqId,
              )(connection),
              activeAtOffset,
              pruned =>
                ACSReader.acsBeforePruningErrorReason(
                  acsOffset = activeAtOffset.toHexString,
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
      )

    def fetchAssignIdsForOffsets(
        offsets: Iterable[Offset]
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
        offsets: Iterable[Offset]
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
    ): Future[Vector[EventStorageBackend.RawAssignEvent]] =
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        localPayloadQueriesLimiter.execute(
          globalPayloadQueriesLimiter.execute(
            dispatcher.executeSql(
              metrics.index.db.reassignmentStream.fetchEventAssignPayloads
            ) { connection =>
              val result = queryNonPruned.executeSql(
                eventStorageBackend.assignEventBatch(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                )(connection),
                activeAtOffset,
                pruned =>
                  ACSReader.acsBeforePruningErrorReason(
                    acsOffset = activeAtOffset.toHexString,
                    prunedUpToOffset = pruned.toHexString,
                  ),
              )(connection, implicitly)
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
    ): Future[Vector[EventStorageBackend.RawUnassignEvent]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(
            metrics.index.db.reassignmentStream.fetchEventUnassignPayloads
          ) { connection =>
            val result = queryNonPruned.executeSql(
              eventStorageBackend.unassignEventBatch(
                eventSequentialIds = ids,
                allFilterParties = allFilterParties,
              )(connection),
              activeAtOffset,
              pruned =>
                ACSReader.acsBeforePruningErrorReason(
                  acsOffset = activeAtOffset.toHexString,
                  prunedUpToOffset = pruned.toHexString,
                ),
            )(connection, implicitly)
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
    ): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] = {
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        globalPayloadQueriesLimiter.execute(
          dispatcher
            .executeSql(metrics.index.db.flatTxStream.fetchEventCreatePayloads) { connection =>
              val result = queryNonPruned.executeSql(
                eventStorageBackend.transactionStreamingQueries
                  .fetchEventPayloadsFlat(EventPayloadSourceForFlatTx.Create)(
                    eventSequentialIds = ids,
                    allFilterParties = allFilterParties,
                  )(connection),
                activeAtOffset,
                pruned =>
                  ACSReader.acsBeforePruningErrorReason(
                    acsOffset = activeAtOffset.toHexString,
                    prunedUpToOffset = pruned.toHexString,
                  ),
              )(connection, implicitly)
              logger.debug(
                s"fetchEventPayloadsFlat for Create returned ${ids.size}/${result.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              result
            }
        )
    }

    def fetchCreatedEventsForUnassignedBatch(batch: Seq[RawUnassignEvent]): Future[
      Seq[(RawUnassignEvent, Either[RawCreatedEvent, EventStorageBackend.Entry[Raw.FlatEvent]])]
    ] =
      for {
        createdIds <- fetchCreateIdsForContractIds(batch.map(_.contractId).distinct)
        createdPayloads <- fetchCreatePayloads(createdIds)
        createdPayloadResults = createdPayloads.iterator
          .flatMap(entry =>
            entry.event match {
              case created: Raw.FlatEvent.Created => Iterator(created.partial.contractId -> entry)
              case _ => Iterator.empty
            }
          )
          .toMap
        missingContractIds = batch
          .map(_.contractId)
          .filterNot(createdPayloadResults.contains)
          .distinct
        assignedIds <- fetchAssignIdsForContractIds(missingContractIds)
        assignedPayloads <- fetchAssignPayloads(assignedIds)
        rawCreatedResults = assignedPayloads.iterator
          .map(rawAssignEvent =>
            rawAssignEvent.rawCreatedEvent.contractId -> rawAssignEvent.rawCreatedEvent
          )
          .toMap
      } yield batch.flatMap(rawUnassignEvent =>
        createdPayloadResults
          .get(rawUnassignEvent.contractId)
          .map(Right(_))
          .orElse {
            logger.debug(
              s"For an IncompleteUnassigned event (offset:${rawUnassignEvent.offset} workflow-id:${rawUnassignEvent.workflowId} contract-id:${rawUnassignEvent.contractId} template-id:${rawUnassignEvent.templateId} reassignment-counter:${rawUnassignEvent.reassignmentCounter}) there is no CreatedEvent available."
            )
            rawCreatedResults.get(rawUnassignEvent.contractId).map(Left(_))
          }
          .orElse {
            logger.warn(
              s"For an IncompleteUnassigned event (offset:${rawUnassignEvent.offset} workflow-id:${rawUnassignEvent.workflowId} contract-id:${rawUnassignEvent.contractId} template-id:${rawUnassignEvent.templateId} reassignment-counter:${rawUnassignEvent.reassignmentCounter}) there is no CreatedEvent or AssignedEvent available. This entry will be dropped from the result."
            )
            None
          }
          .toList
          .map(rawUnassignEvent -> _)
      )

    val stringWildcardParties = filter.wildcardParties.map(_.toString)
    val stringTemplateFilters = filter.relation.map { case (key, value) =>
      key -> value
    }
    def eventMeetsConstraints(templateId: Identifier, witnesses: Set[String]): Boolean =
      witnesses.exists(stringWildcardParties) ||
        stringTemplateFilters.get(templateId).exists(_.exists(witnesses))
    def unassignMeetsConstraints(rawUnassignEvent: RawUnassignEvent): Boolean =
      eventMeetsConstraints(
        rawUnassignEvent.templateId,
        rawUnassignEvent.witnessParties,
      )
    def assignMeetsConstraints(rawAssignEvent: RawAssignEvent): Boolean =
      eventMeetsConstraints(
        rawAssignEvent.rawCreatedEvent.templateId,
        rawAssignEvent.rawCreatedEvent.witnessParties,
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
            def incompleteOffsetPages: () => Iterator[Vector[Offset]] =
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
        TransactionsReader
          .deserializeRawCreatedEvent(lfValueTranslation, eventProjectionProperties)(
            rawActiveContract.rawCreatedEvent
          )
          .map(createdEvent =>
            GetActiveContractsResponse(
              offset = "", // empty for all data entries
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
      rawAssignEvent: RawAssignEvent
  )(implicit lc: LoggingContextWithTrace): Future[(String, GetActiveContractsResponse)] =
    Timed.future(
      future = Future.delegate(
        TransactionsReader
          .deserializeRawCreatedEvent(lfValueTranslation, eventProjectionProperties)(
            rawAssignEvent.rawCreatedEvent
          )
          .map(createdEvent =>
            rawAssignEvent.offset -> GetActiveContractsResponse(
              offset = "", // empty for all data entries
              workflowId = rawAssignEvent.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
                IncompleteAssigned(
                  Some(TransactionsReader.toAssignedEvent(rawAssignEvent, createdEvent))
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
      rawUnassignEventWithCreate: (
          RawUnassignEvent,
          Either[RawCreatedEvent, EventStorageBackend.Entry[Raw.FlatEvent]],
      )
  )(implicit lc: LoggingContextWithTrace): Future[(String, GetActiveContractsResponse)] = {
    val (rawUnassignEvent, createEither) = rawUnassignEventWithCreate
    Timed.future(
      future = Future.delegate {
        createEither.left
          .map(
            TransactionsReader
              .deserializeRawCreatedEvent(lfValueTranslation, eventProjectionProperties)
          )
          .map(entry =>
            deserializeEntry(eventProjectionProperties, lfValueTranslation)(entry)
              .map(_.event.getCreated)
          )
          .merge
          .map(createdEvent =>
            rawUnassignEvent.offset -> GetActiveContractsResponse(
              offset = "",
              workflowId = rawUnassignEvent.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteUnassigned(
                IncompleteUnassigned(
                  createdEvent = Some(
                    createdEvent.copy(
                      eventId = EventId(
                        transactionId =
                          Ref.LedgerString.assertFromString(rawUnassignEvent.updateId),
                        nodeId = NodeId(0), // all create Node ID is set synthetically to 0
                      ).toLedgerString,
                      witnessParties = rawUnassignEvent.witnessParties.toSeq,
                    )
                  ),
                  unassignedEvent = Some(TransactionsReader.toUnassignedEvent(rawUnassignEvent)),
                )
              ),
            )
          )
      },
      timer = dbMetrics.getActiveContracts.translationTimer,
    )
  }

}

object ACSReader {

  def acsBeforePruningErrorReason(acsOffset: String, prunedUpToOffset: String): String =
    s"Active contracts request at offset $acsOffset precedes pruned offset $prunedUpToOffset"

}
