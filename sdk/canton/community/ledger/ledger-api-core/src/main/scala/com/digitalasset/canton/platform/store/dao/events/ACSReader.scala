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
  FatCreatedEventProperties,
  RawFatActiveContract,
  RawFatAssignEvent,
  RawThinAssignEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForUpdatesAcsDelta
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
    val activeIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelActiveIdQueries, executionContext)
    val localPayloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelPayloadCreateQueries, executionContext)
    val idQueryPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = config.maxIdsPerIdPage,
      workingMemoryInBytesForIdPages = config.maxWorkingMemoryInBytesForIdPages,
      numOfDecomposedFilters = decomposedFilters.size,
      numOfPagesInIdPageBuffer = config.maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    def fetchActiveIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      paginatingAsyncStream.streamIdsFromSeekPaginationWithIdFilter(
        idStreamName = s"ActiveContractIds $filter",
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
        initialEndInclusive = activeAtEventSeqId,
      )(
        eventStorageBackend.updateStreamingQueries.fetchActiveIds(
          stakeholderO = filter.party,
          templateIdO = filter.templateId,
          activeAtEventSeqId = activeAtEventSeqId,
        )
      )(
        executeLastIdQuery = f =>
          activeIdQueriesLimiter.execute(
            globalIdQueriesLimiter.execute(
              dispatcher.executeSql(metrics.index.db.getActiveContractIdRanges)(f)
            )
          ),
        idFilterQueryParallelism = config.idFilterQueryParallelism,
        executeIdFilterQuery = f =>
          activeIdQueriesLimiter.execute(
            globalIdQueriesLimiter.execute(
              dispatcher.executeSql(
                metrics.index.db.getFilteredActiveContractIds
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
      } yield payloads.map { payload =>
        payload -> contractsM
          .get(internalContractId(payload))
          .map(_.inst)
      }

    def resolveFatInstance[T, U](
        internalContractId: T => Long,
        toFatInstance: (T, FatContract) => U,
    )(payloads: Vector[(T, Option[FatContract])]): Vector[U] =
      payloads.map {
        case (payload, None) =>
          throw new IllegalStateException(
            s"Contract for internal contract id ${internalContractId(payload)} was not found in the contract store."
          )
        case (payload, Some(fatContract)) =>
          toFatInstance(payload, fatContract)
      }

    def fetchActivePayloads(
        ids: Iterable[Long]
    ): Future[Vector[RawFatActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          withValidatedActiveAt(
            dispatcher
              .executeSql(metrics.index.db.getActiveContractBatch) {
                eventStorageBackend.activeContractBatch(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                )
              }
              .flatMap(
                withFatContracts(_.thinCreatedEventProperties.internalContractId)
              )
          ).map(
            resolveFatInstance(
              internalContractId = _.thinCreatedEventProperties.internalContractId,
              toFatInstance = (thin, fatContract) =>
                RawFatActiveContract(
                  commonEventProperties = thin.commonEventProperties,
                  fatCreatedEventProperties = FatCreatedEventProperties(
                    thinCreatedEventProperties = thin.thinCreatedEventProperties,
                    fatContract = fatContract,
                  ),
                ),
            )
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
              .lookupAssignSequentialIdByOffset(offsets.map(_.unwrap))(connection)
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
              .lookupUnassignSequentialIdByOffset(offsets.map(_.unwrap))(connection)
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
    ): Future[Vector[RawFatAssignEvent]] =
      if (ids.isEmpty) Future.successful(Vector.empty)
      else
        localPayloadQueriesLimiter.execute(
          globalPayloadQueriesLimiter.execute(
            withValidatedActiveAt(
              dispatcher
                .executeSql(
                  metrics.index.db.updatesAcsDeltaStream.fetchEventActivatePayloads
                )(
                  eventStorageBackend.fetchEventPayloadsAcsDelta(
                    EventPayloadSourceForUpdatesAcsDelta.Activate
                  )(
                    eventSequentialIds = Ids(ids),
                    requestingPartiesForTx = None,
                    requestingPartiesForReassignment = allFilterParties,
                  )
                )
                .map(_.collect { case raw: RawThinAssignEvent =>
                  raw
                })
                .flatMap(withFatContracts(_.thinCreatedEventProperties.internalContractId))
            ).map(
              resolveFatInstance(
                internalContractId = _.thinCreatedEventProperties.internalContractId,
                toFatInstance = (thin, fatContract) =>
                  RawFatAssignEvent(
                    reassignmentProperties = thin.reassignmentProperties,
                    fatCreatedEventProperties = FatCreatedEventProperties(
                      thinCreatedEventProperties = thin.thinCreatedEventProperties,
                      fatContract = fatContract,
                    ),
                    sourceSynchronizerId = thin.sourceSynchronizerId,
                  ),
              )
            ).thereafterP { case Success(result) =>
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
    ): Future[Vector[RawUnassignEvent]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          withValidatedActiveAt(
            dispatcher
              .executeSql(
                metrics.index.db.updatesAcsDeltaStream.fetchEventDeactivatePayloads
              )(
                eventStorageBackend.fetchEventPayloadsAcsDelta(
                  EventPayloadSourceForUpdatesAcsDelta.Deactivate
                )(
                  eventSequentialIds = Ids(ids),
                  requestingPartiesForTx = None,
                  requestingPartiesForReassignment = allFilterParties,
                )
              )
              .map(_.collect { case raw: RawUnassignEvent =>
                raw
              })
          ).thereafterP { case Success(result) =>
            logger.debug(
              s"unassignEventBatch returned ${result.size}/${ids.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
          }
        )
      )

    def fetchActivationEventsForUnassignedBatch(
        batch: Seq[RawUnassignEvent]
    ): Future[Seq[(RawUnassignEvent, RawFatActiveContract)]] = {
      val (unassignEventWithDeactivationRef, deactivatedEventSeqIds) = batch.view
        .flatMap(rawUnassignEvent =>
          rawUnassignEvent.deactivatedEventSeqId match {
            case Some(deactivatedEventSeqId) => Some(rawUnassignEvent -> deactivatedEventSeqId)
            case None =>
              logger.warn(
                s"For an IncompleteUnassigned event (offset:${rawUnassignEvent.reassignmentProperties.commonEventProperties.offset} workflow-id:${rawUnassignEvent.reassignmentProperties.commonEventProperties.workflowId} contract-id:${rawUnassignEvent.contractId} template-id:${rawUnassignEvent.templateId} reassignment-counter:${rawUnassignEvent.reassignmentProperties.reassignmentCounter} synchronizer id:${rawUnassignEvent.reassignmentProperties.commonEventProperties.synchronizerId} event-sequential-id:${rawUnassignEvent.reassignmentProperties.commonEventProperties.eventSequentialId}) there is neither CreatedEvent nor AssignedEvent available. This entry will be dropped from the result."
              )
              None
          }
        )
        .toVector
        .unzip
      fetchActivePayloads(deactivatedEventSeqIds)
        .map(unassignEventWithDeactivationRef.zip)
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

    def unassignMeetsConstraints(rawUnassignEvent: RawUnassignEvent): Boolean =
      eventMeetsConstraints(
        rawUnassignEvent.templateId,
        rawUnassignEvent.witnessParties,
      )
    def assignMeetsConstraints(rawAssignEvent: RawFatAssignEvent): Boolean =
      eventMeetsConstraints(
        rawAssignEvent.templateId,
        rawAssignEvent.witnessParties,
      )

    // Pekko requires for this buffer's size to be a power of two.
    val inputBufferSize =
      Utils.largestSmallerOrEqualPowerOfTwo(config.maxParallelPayloadCreateQueries)

    decomposedFilters
      .map(fetchActiveIds)
      .pipe(EventIdsUtils.sortAndDeduplicateIds)
      .batchN(
        maxBatchSize = config.maxPayloadsPerPayloadsPage,
        maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
      )
      .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
      .mapAsync(config.maxParallelPayloadCreateQueries)(fetchActivePayloads)
      .mapConcat(identity)
      .mapAsync(config.contractProcessingParallelism)(
        toApiResponseActiveContract(eventProjectionProperties)
      )
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
                .mapAsync(config.maxParallelActiveIdQueries)(
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

            val incompleteUnassigned: Source[(Long, GetActiveContractsResponse), NotUsed] =
              Source
                .fromIterator(incompleteOffsetPages)
                .mapAsync(config.maxParallelActiveIdQueries)(
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
                  fetchActivationEventsForUnassignedBatch
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
      eventProjectionProperties: EventProjectionProperties
  )(
      rawActiveContract: RawFatActiveContract
  )(implicit lc: LoggingContextWithTrace): Future[GetActiveContractsResponse] =
    Timed.future(
      future = Future.delegate(
        lfValueTranslation
          .toApiCreatedEvent(
            eventProjectionProperties = eventProjectionProperties,
            fatContractInstance = rawActiveContract.fatCreatedEventProperties.fatContract,
            offset = rawActiveContract.commonEventProperties.offset,
            nodeId = rawActiveContract.commonEventProperties.nodeId,
            representativePackageId = Ref.PackageId.assertFromString(
              rawActiveContract.fatCreatedEventProperties.thinCreatedEventProperties.representativePackageId
            ),
            witnesses = rawActiveContract.witnessParties,
            acsDelta = true,
          )
          .map(createdEvent =>
            GetActiveContractsResponse(
              workflowId = rawActiveContract.commonEventProperties.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.ActiveContract(
                ActiveContract(
                  createdEvent = Some(createdEvent),
                  synchronizerId = rawActiveContract.commonEventProperties.synchronizerId,
                  reassignmentCounter =
                    rawActiveContract.fatCreatedEventProperties.thinCreatedEventProperties.reassignmentCounter,
                )
              ),
            )
          )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )

  private def toApiResponseIncompleteAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawFatAssign: RawFatAssignEvent
  )(implicit lc: LoggingContextWithTrace): Future[(Long, GetActiveContractsResponse)] =
    Timed.future(
      future = Future.delegate(
        lfValueTranslation
          .toApiCreatedEvent(
            eventProjectionProperties = eventProjectionProperties,
            fatContractInstance = rawFatAssign.fatCreatedEventProperties.fatContract,
            offset = rawFatAssign.reassignmentProperties.commonEventProperties.offset,
            nodeId = rawFatAssign.reassignmentProperties.commonEventProperties.nodeId,
            representativePackageId = Ref.PackageId.assertFromString(
              rawFatAssign.fatCreatedEventProperties.thinCreatedEventProperties.representativePackageId
            ),
            witnesses = rawFatAssign.witnessParties,
            acsDelta = true,
          )
          .map(createdEvent =>
            rawFatAssign.reassignmentProperties.commonEventProperties.offset -> GetActiveContractsResponse(
              workflowId =
                rawFatAssign.reassignmentProperties.commonEventProperties.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
                IncompleteAssigned(
                  Some(UpdateReader.toAssignedEvent(rawFatAssign, createdEvent))
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
      rawUnassignEventWithActive: (RawUnassignEvent, RawFatActiveContract)
  )(implicit lc: LoggingContextWithTrace): Future[(Long, GetActiveContractsResponse)] = {
    val (rawUnassignEvent, rawFatActiveContract) = rawUnassignEventWithActive
    Timed.future(
      future = lfValueTranslation
        .toApiCreatedEvent(
          eventProjectionProperties = eventProjectionProperties,
          fatContractInstance = rawFatActiveContract.fatCreatedEventProperties.fatContract,
          offset = rawFatActiveContract.commonEventProperties.offset,
          nodeId = rawFatActiveContract.commonEventProperties.nodeId,
          representativePackageId = Ref.PackageId.assertFromString(
            rawFatActiveContract.fatCreatedEventProperties.thinCreatedEventProperties.representativePackageId
          ),
          witnesses = rawFatActiveContract.witnessParties,
          acsDelta = true,
        )
        .map(createdEvent =>
          rawUnassignEvent.reassignmentProperties.commonEventProperties.offset -> GetActiveContractsResponse(
            workflowId = rawUnassignEvent.reassignmentProperties.commonEventProperties.workflowId
              .getOrElse(""),
            contractEntry = GetActiveContractsResponse.ContractEntry.IncompleteUnassigned(
              IncompleteUnassigned(
                createdEvent = Some(createdEvent),
                unassignedEvent = Some(
                  UpdateReader.toUnassignedEvent(rawUnassignEvent)
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
