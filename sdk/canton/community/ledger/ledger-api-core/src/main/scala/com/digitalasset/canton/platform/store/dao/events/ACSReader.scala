// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfig
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawActiveContractLegacy,
  RawAssignEventLegacy,
  RawCreatedEventLegacy,
  RawUnassignEventLegacy,
  UnassignProperties,
}
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForUpdatesAcsDeltaLegacy
import com.digitalasset.canton.platform.store.dao.events.UpdateReader.endSpanOnTermination
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
import com.digitalasset.canton.platform.{FatContract, TemplatePartiesFilter}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.FullIdentifier
import com.digitalasset.daml.lf.value.Value.ContractId
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import scala.util.chaining.*

/** Streams ACS events (active contracts) in a two step process consisting of:
  *   1. fetching event sequential ids of the active contracts based on the filtering constraints,
  *   1. fetching the active contracts based on the fetched event sequential ids.
  *
  * Details: An input filtering constraint (consisting of parties and template ids) is converted
  * into decomposed filtering constraints (a constraint with exactly one party and at most one
  * template id). For each decomposed filter, the matching event sequential ids are fetched in
  * parallel and then merged into a strictly increasing sequence. The elements from this sequence
  * are then batched and the batch ids serve as the input to the payload fetching step.
  */
class ACSReader(
    config: ActiveContractsServiceStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    contractStore: ContractStore,
    incompleteOffsets: (
        Offset,
        Option[Set[Ref.Party]],
        TraceContext,
    ) => FutureUnlessShutdown[Vector[Offset]],
    metrics: LedgerApiServerMetrics,
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
    val (activeAtOffset, activeAtLong) = activeAt
    val span =
      Telemetry.Updates.createSpan(tracer, activeAtOffset)(qualifiedNameOfCurrentFunc)
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
      activeAt: (Offset, Long),
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, activeAtEventSeqId) = activeAt
    def withValidatedActiveAt[T](query: => Future[T]) =
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
      paginatingAsyncStream.streamIdsFromSeekPaginationWithIdFilter(
        idStreamName = s"ActiveContractIds for create events $filter",
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
        initialEndInclusive = activeAtEventSeqId,
      )(
        eventStorageBackend.updateStreamingQueries.fetchActiveIdsOfCreateEventsForStakeholderLegacy(
          stakeholderO = filter.party,
          templateIdO = filter.templateId,
          activeAtEventSeqId = activeAtEventSeqId,
        )
      )(
        executeLastIdQuery = f =>
          createIdQueriesLimiter.execute(
            globalIdQueriesLimiter.execute(
              dispatcher.executeSql(metrics.index.db.getActiveContractIdRangesForCreatedLegacy)(f)
            )
          ),
        idFilterQueryParallelism = config.idFilterQueryParallelism,
        executeIdFilterQuery = f =>
          createIdQueriesLimiter.execute(
            globalIdQueriesLimiter.execute(
              dispatcher.executeSql(
                metrics.index.db.getFilteredActiveContractIdsForCreatedLegacy
              )(f)
            )
          ),
      )

    def fetchAssignIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      paginatingAsyncStream.streamIdsFromSeekPaginationWithIdFilter(
        idStreamName = s"ActiveContractIds for assign events $filter",
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
        initialEndInclusive = activeAtEventSeqId,
      )(
        eventStorageBackend.updateStreamingQueries.fetchActiveIdsOfAssignEventsForStakeholderLegacy(
          stakeholderO = filter.party,
          templateIdO = filter.templateId,
          activeAtEventSeqId = activeAtEventSeqId,
        )
      )(
        executeLastIdQuery = f =>
          assignIdQueriesLimiter.execute(
            globalIdQueriesLimiter.execute(
              dispatcher.executeSql(metrics.index.db.getActiveContractIdRangesForAssignedLegacy)(f)
            )
          ),
        idFilterQueryParallelism = config.idFilterQueryParallelism,
        executeIdFilterQuery = f =>
          assignIdQueriesLimiter.execute(
            globalIdQueriesLimiter.execute(
              dispatcher.executeSql(
                metrics.index.db.getFilteredActiveContractIdsForAssignedLegacy
              )(f)
            )
          ),
      )

    def withFatContracts[T](
        internalContractId: T => Long
    )(payloads: Vector[T]): Future[Vector[(T, Option[FatContract])]] =
      for {
        contractsM <- contractStore
          .lookupBatchedNonCached(
            payloads.map(internalContractId)
          )
          .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
      } yield payloads
        .map { payload =>
          val fatContractO = contractsM.get(internalContractId(payload)).map(_.inst)
          (payload, fatContractO)
        }

    def fetchActiveCreatePayloads(
        ids: Iterable[Long]
    ): Future[Vector[(RawActiveContractLegacy, Option[FatContract])]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          withValidatedActiveAt(
            dispatcher
              .executeSql(metrics.index.db.getActiveContractBatchForCreatedLegacy) {
                eventStorageBackend.activeContractCreateEventBatchLegacy(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAtEventSeqId,
                )
              }
              .flatMap(withFatContracts(_.rawCreatedEvent.internalContractId))
          ).thereafterP { case Success(result) =>
            logger.debug(
              s"getActiveContractBatch returned ${result.size}/${ids.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
          }
        )
      )

    def fetchActiveAssignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[(RawActiveContractLegacy, Option[FatContract])]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          withValidatedActiveAt(
            dispatcher
              .executeSql(metrics.index.db.getActiveContractBatchForAssignedLegacy)(
                eventStorageBackend.activeContractAssignEventBatchLegacy(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAtEventSeqId,
                )
              )
              .flatMap(withFatContracts(_.rawCreatedEvent.internalContractId))
          ).thereafterP { case Success(result) =>
            logger.debug(
              s"getActiveContractBatch returned ${result.size}/${ids.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )

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
              .lookupAssignSequentialIdByOffsetLegacy(offsets.map(_.unwrap))(connection)
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
              .lookupUnassignSequentialIdByOffsetLegacy(offsets.map(_.unwrap))(connection)
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
    ): Future[Vector[(Entry[RawAssignEventLegacy], Option[FatContract])]] =
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        localPayloadQueriesLimiter.execute(
          globalPayloadQueriesLimiter.execute(
            withValidatedActiveAt(
              dispatcher
                .executeSql(
                  metrics.index.db.reassignmentStream.fetchEventAssignPayloadsLegacy
                )(
                  eventStorageBackend.assignEventBatchLegacy(
                    eventSequentialIds = Ids(ids),
                    allFilterParties = allFilterParties,
                  )
                )
                .flatMap(withFatContracts(_.event.rawCreatedEvent.internalContractId))
            )
              .thereafterP { case Success(result) =>
                logger.debug(
                  s"assignEventBatch returned ${result.size}/${ids.size} ${ids.lastOption
                      .map(last => s"until $last")
                      .getOrElse("")}"
                )
              }
          )
        )

    def fetchUnassignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[Entry[RawUnassignEventLegacy]]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          withValidatedActiveAt(
            dispatcher.executeSql(
              metrics.index.db.reassignmentStream.fetchEventUnassignPayloadsLegacy
            )(
              eventStorageBackend.unassignEventBatchLegacy(
                eventSequentialIds = Ids(ids),
                allFilterParties = allFilterParties,
              )
            )
          ).thereafterP { case Success(result) =>
            logger.debug(
              s"unassignEventBatch returned ${result.size}/${ids.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
          }
        )
      )

    def fetchCreateIdsForContractIds(
        contractIds: Iterable[ContractId]
    ): Future[Vector[Long]] =
      globalIdQueriesLimiter.execute(
        dispatcher.executeSql(metrics.index.db.getCreateIdsForContractIdsLegacy) { connection =>
          val ids =
            eventStorageBackend.lookupCreateSequentialIdByContractIdLegacy(contractIds)(
              connection
            )
          logger.debug(
            s"Create Ids for contract IDs returned #${ids.size} (from ${contractIds.size}) ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          ids
        }
      )

    def fetchAssignIdsFor(
        unassignPropertiesSeq: Seq[UnassignProperties]
    ): Future[Map[UnassignProperties, Long]] =
      if (unassignPropertiesSeq.isEmpty) Future.successful(Map.empty)
      else
        globalIdQueriesLimiter.execute(
          dispatcher.executeSql(metrics.index.db.getAssignIdsForContractIdsLegacy) { connection =>
            val idForUnassignProperties: Map[UnassignProperties, Long] =
              eventStorageBackend.lookupAssignSequentialIdByLegacy(
                unassignPropertiesSeq
              )(connection)
            logger.debug(
              s"Assign Ids for contract IDs returned #${idForUnassignProperties.size} (from ${unassignPropertiesSeq.size})"
            )
            idForUnassignProperties
          }
        )

    def fetchCreatePayloads(
        ids: Iterable[Long]
    ): Future[Vector[(Entry[RawCreatedEventLegacy], Option[FatContract])]] =
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        globalPayloadQueriesLimiter.execute(
          withValidatedActiveAt(
            dispatcher
              .executeSql(
                metrics.index.db.updatesAcsDeltaStream.fetchEventCreatePayloadsLegacy
              )(
                eventStorageBackend.fetchEventPayloadsAcsDeltaLegacy(
                  EventPayloadSourceForUpdatesAcsDeltaLegacy.Create
                )(
                  eventSequentialIds = Ids(ids),
                  requestingParties = allFilterParties,
                )
              )
              .map(result =>
                result.view.collect { entry =>
                  entry.event match {
                    case created: RawCreatedEventLegacy =>
                      entry.copy(event = created)
                  }
                }.toVector
              )
              .flatMap(withFatContracts(_.event.internalContractId))
              .thereafterP { case Success(result) =>
                logger.debug(
                  s"fetchEventPayloads for Create returned ${result.size}/${ids.size} ${ids.lastOption
                      .map(last => s"until $last")
                      .getOrElse("")}"
                )
              }
          )
        )

    def fetchCreatedEventsForUnassignedBatch(batch: Seq[Entry[RawUnassignEventLegacy]]): Future[
      Seq[(Entry[RawUnassignEventLegacy], (Entry[RawCreatedEventLegacy], Option[FatContract]))]
    ] = {

      def extractUnassignProperties(
          unassignEntry: Entry[RawUnassignEventLegacy]
      ): UnassignProperties =
        UnassignProperties(
          contractId = unassignEntry.event.contractId,
          synchronizerId = unassignEntry.event.sourceSynchronizerId,
          sequentialId = unassignEntry.eventSequentialId,
        )

      // look for the last created event first in the assigned events
      // the assign event should match the contract id and the synchronizer id (target and source synchronizer ids correspondingly)
      // of the unassign entry and have
      // the largest sequential id that is less than the sequential id of the unassign entry
      val unassignPropertiesSeq: Seq[UnassignProperties] =
        batch.map(extractUnassignProperties).distinct
      for {
        // mapping from the request unassign properties to the corresponding assign sequential id
        unassignPropertiesToAssignedIds: Map[UnassignProperties, Long] <- fetchAssignIdsFor(
          unassignPropertiesSeq
        )
        assignedPayloads: Seq[(Entry[RawAssignEventLegacy], Option[FatContract])] <-
          fetchAssignPayloads(
            unassignPropertiesToAssignedIds.values
          )
        assignedIdsToPayloads: Map[Long, (Entry[RawAssignEventLegacy], Option[FatContract])] =
          assignedPayloads
            .map(payload => payload._1.eventSequentialId -> payload)
            .toMap
        // map the requested unassign event properties to the returned raw created events using the assign sequential id
        rawCreatedFromAssignedResults: Map[
          UnassignProperties,
          (Entry[RawCreatedEventLegacy], Option[FatContract]),
        ] =
          unassignPropertiesToAssignedIds.flatMap { case (params, assignedId) =>
            assignedIdsToPayloads
              .get(assignedId)
              .map { case (assignEntry, fatContract) =>
                (params, (assignEntry.map(_.rawCreatedEvent), fatContract))
              }
          }

        // if not found in the assigned events, search the created events
        // we can search the created events without checking the sequential id of it, since if found it will be unique
        // and less than the sequential id of the unassign event (we cannot have an unassign before the create event)
        missingContractIds = unassignPropertiesSeq
          .filterNot(rawCreatedFromAssignedResults.contains)
          .map(_.contractId)
          .distinct
        createdIds <- fetchCreateIdsForContractIds(missingContractIds)
        createdPayloads <- fetchCreatePayloads(createdIds)
        rawCreatedFromCreatedResults: Map[ContractId, Vector[
          (Entry[RawCreatedEventLegacy], Option[FatContract])
        ]] =
          createdPayloads.groupBy(_._1.event.contractId)
      } yield batch.flatMap { rawUnassignEntry =>
        val unassignProperties = extractUnassignProperties(rawUnassignEntry)
        rawCreatedFromAssignedResults
          .get(unassignProperties)
          .orElse {
            logger.debug(
              s"For an IncompleteUnassigned event (offset:${rawUnassignEntry.offset} workflow-id:${rawUnassignEntry.workflowId} contract-id:${rawUnassignEntry.event.contractId} template-id:${rawUnassignEntry.event.templateId} reassignment-counter:${rawUnassignEntry.event.reassignmentCounter} synchronizer id:${rawUnassignEntry.synchronizerId} event-sequential-id:${rawUnassignEntry.eventSequentialId}) there is no AssignedEvent available, looking for CreatedEvent."
            )
            rawCreatedFromCreatedResults
              .get(unassignProperties.contractId)
              .flatMap { candidateCreateEntries =>
                candidateCreateEntries
                  .find(createdEntry =>
                    // the created event should match the synchronizer id of the unassign entry and have a lower sequential id than it
                    createdEntry._1.synchronizerId == unassignProperties.synchronizerId && createdEntry._1.eventSequentialId < unassignProperties.sequentialId
                  )
              }
          }
          .orElse {
            logger.warn(
              s"For an IncompleteUnassigned event (offset:${rawUnassignEntry.offset} workflow-id:${rawUnassignEntry.workflowId} contract-id:${rawUnassignEntry.event.contractId} template-id:${rawUnassignEntry.event.templateId} reassignment-counter:${rawUnassignEntry.event.reassignmentCounter} synchronizer id:${rawUnassignEntry.synchronizerId} event-sequential-id:${rawUnassignEntry.eventSequentialId}) there is neither CreatedEvent nor AssignedEvent available. This entry will be dropped from the result."
            )
            None
          }
          .toList
          .map(rawUnassignEntry -> _)
      }
    }

    val stringWildcardParties = filter.templateWildcardParties.map(_.map(_.toString))
    val templateFilters = filter.relation.map { case (key, value) =>
      key -> value
    }
    def eventMeetsConstraints(templateId: FullIdentifier, witnesses: Set[String]): Boolean =
      stringWildcardParties.fold(true)(_.exists(witnesses)) || (
        templateFilters.get(templateId.toNameTypeConRef) match {
          case Some(Some(filterParties)) => filterParties.exists(witnesses)
          case Some(None) => true // party wildcard
          case None =>
            false // templateId is not in the filter
        }
      )

    def unassignMeetsConstraints(rawUnassignEntry: Entry[RawUnassignEventLegacy]): Boolean =
      eventMeetsConstraints(
        rawUnassignEntry.event.templateId,
        rawUnassignEntry.event.witnessParties,
      )
    def assignMeetsConstraints(rawAssignEntry: Entry[RawAssignEventLegacy]): Boolean =
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
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(config.maxParallelPayloadCreateQueries)(fetchActiveAssignPayloads)
        .mapConcat(identity)

    activeFromCreatePipe
      .mergeSorted(activeFromAssignPipe)(Ordering.by(_._1.eventSequentialId))
      .mapAsync(config.contractProcessingParallelism) { case (rawActiveContract, fatContractO) =>
        toApiResponseActiveContract(rawActiveContract, fatContractO, eventProjectionProperties)
      }
      .concatLazy(
        // compute incomplete reassignments
        Source.lazyFutureSource(() =>
          incompleteOffsets(
            activeAtOffset,
            filter.allFilterParties,
            loggingContext.traceContext,
          ).map { offsets =>
            def incompleteOffsetPages: () => Iterator[Vector[Offset]] =
              () => offsets.sliding(config.maxIncompletePageSize, config.maxIncompletePageSize)

            val incompleteAssigned: Source[(Long, GetActiveContractsResponse), NotUsed] =
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
                .mapConcat(_.filter(entryPair => assignMeetsConstraints(entryPair._1)))
                .mapAsync(config.contractProcessingParallelism)(
                  toApiResponseIncompleteAssigned(eventProjectionProperties)
                )

            val incompleteUnassigned: Source[(Long, GetActiveContractsResponse), NotUsed] =
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
              AbortedDueToShutdown.Error().asGrpcError
            )
          }
        )
      )
  }

  private def toApiResponseActiveContract(
      rawActiveContract: RawActiveContractLegacy,
      fatContract: Option[FatContract],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[GetActiveContractsResponse] =
    Timed.future(
      future = Future.delegate(
        lfValueTranslation
          .toApiCreatedEvent(
            eventProjectionProperties = eventProjectionProperties,
            fatContractInstance = fatContract.getOrElse(
              throw new IllegalStateException(
                s"Contract for internal contract id ${rawActiveContract.rawCreatedEvent.internalContractId} was not found in the contract store."
              )
            ),
            offset = rawActiveContract.offset,
            nodeId = rawActiveContract.nodeId,
            representativePackageId = rawActiveContract.rawCreatedEvent.representativePackageId,
            witnesses = rawActiveContract.rawCreatedEvent.witnessParties,
            acsDelta = true,
          )
          .map(createdEvent =>
            GetActiveContractsResponse(
              workflowId = rawActiveContract.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.ActiveContract(
                ActiveContract(
                  createdEvent = Some(createdEvent),
                  synchronizerId = rawActiveContract.synchronizerId,
                  reassignmentCounter = rawActiveContract.reassignmentCounter,
                )
              ),
            )
          )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )

  private def toApiResponseIncompleteAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawAssignEntryFatContract: (Entry[RawAssignEventLegacy], Option[FatContract])
  )(implicit lc: LoggingContextWithTrace): Future[(Long, GetActiveContractsResponse)] =
    rawAssignEntryFatContract match {
      case (rawAssignEntry, fatContract) =>
        Timed.future(
          future = Future.delegate(
            lfValueTranslation
              .toApiCreatedEvent(
                eventProjectionProperties = eventProjectionProperties,
                fatContractInstance = fatContract.getOrElse(
                  throw new IllegalStateException(
                    s"Contract for internal contract id ${rawAssignEntry.event.rawCreatedEvent.internalContractId} was not found in the contract store."
                  )
                ),
                offset = rawAssignEntry.offset,
                nodeId = rawAssignEntry.nodeId,
                representativePackageId =
                  rawAssignEntry.event.rawCreatedEvent.representativePackageId,
                witnesses = rawAssignEntry.event.rawCreatedEvent.witnessParties,
                acsDelta = true,
              )
              .map(createdEvent =>
                rawAssignEntry.offset -> GetActiveContractsResponse(
                  workflowId = rawAssignEntry.workflowId.getOrElse(""),
                  contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
                    IncompleteAssigned(
                      Some(UpdateReader.toAssignedEvent(rawAssignEntry.event, createdEvent))
                    )
                  ),
                )
              )
          ),
          timer = dbMetrics.getActiveContracts.translationTimer,
        )
    }

  private def toApiResponseIncompleteUnassigned(
      eventProjectionProperties: EventProjectionProperties
  )(
      rawUnassignEntryWithCreate: (
          Entry[RawUnassignEventLegacy],
          (Entry[RawCreatedEventLegacy], Option[FatContract]),
      )
  )(implicit lc: LoggingContextWithTrace): Future[(Long, GetActiveContractsResponse)] = {
    val (rawUnassignEntry, (rawCreate, fatContract)) = rawUnassignEntryWithCreate
    Timed.future(
      future = lfValueTranslation
        .toApiCreatedEvent(
          eventProjectionProperties = eventProjectionProperties,
          fatContractInstance = fatContract.getOrElse(
            throw new IllegalStateException(
              s"Contract for internal contract id ${rawCreate.event.internalContractId} was not found in the contract store."
            )
          ),
          offset = rawCreate.offset,
          nodeId = rawCreate.nodeId,
          representativePackageId = rawCreate.event.representativePackageId,
          witnesses = rawCreate.event.witnessParties,
          acsDelta = true,
        )
        .map(createdEvent =>
          rawUnassignEntry.offset -> GetActiveContractsResponse(
            workflowId = rawUnassignEntry.workflowId.getOrElse(""),
            contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteUnassigned(
              IncompleteUnassigned(
                createdEvent = Some(createdEvent),
                unassignedEvent = Some(
                  UpdateReader.toUnassignedEvent(rawUnassignEntry.offset, rawUnassignEntry)
                ),
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
      acsOffset: Offset,
      prunedUpToOffset: Offset,
  ): String =
    s"Active contracts request at offset ${acsOffset.unwrap} precedes pruned offset ${prunedUpToOffset.unwrap}"

  def acsAfterLedgerEndErrorReason(
      acsOffset: Offset,
      ledgerEndOffset: Option[Offset],
  ): String =
    s"Active contracts request at offset ${acsOffset.unwrap} preceded by ledger end offset ${ledgerEndOffset
        .fold(0L)(_.unwrap)}"

}
