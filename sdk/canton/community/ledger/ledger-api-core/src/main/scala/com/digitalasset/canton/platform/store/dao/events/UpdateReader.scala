// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent,
  Reassignment,
  ReassignmentEvent,
  UnassignedEvent,
}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.trace_context.TraceContext as DamlTraceContext
import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesResponse}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawAcsDeltaEventLegacy,
  RawArchivedEventLegacy,
  RawAssignEventLegacy,
  RawCreatedEventLegacy,
  RawEventLegacy,
  RawExercisedEventLegacy,
  RawLedgerEffectsEventLegacy,
  RawUnassignEventLegacy,
}
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.{FatContract, InternalUpdateFormat, TemplatePartiesFilter}
import com.digitalasset.canton.util.MonadUtil
import io.opentelemetry.api.trace.Span
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param updatesStreamReader
  *   Knows how to stream updates
  * @param updatePointwiseReader
  *   Knows how to fetch an update by its id or its offset
  * @param treeTransactionsStreamReader
  *   Knows how to stream tree transactions
  * @param transactionPointwiseReader
  *   Knows how to fetch a transaction by its id or its offset
  * @param treeTransactionPointwiseReader
  *   Knows how to fetch a tree transaction by its id or its offset
  * @param dispatcher
  *   Executes the queries prepared by this object
  * @param queryValidRange
  * @param eventStorageBackend
  * @param metrics
  * @param acsReader
  *   Knows how to streams ACS
  * @param executionContext
  *   Runs transformations on data fetched from the database, including Daml-LF value
  *   deserialization
  */
private[dao] final class UpdateReader(
    updatesStreamReader: UpdatesStreamReader,
    updatePointwiseReader: UpdatePointwiseReader,
    dispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
    acsReader: ACSReader,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoUpdateReader {

  private val dbMetrics = metrics.index.db

  override def getUpdates(
      startInclusive: Offset,
      endInclusive: Offset,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val futureSource =
      getEventSeqIdRange(startInclusive, endInclusive)
        .map(queryRange =>
          updatesStreamReader.streamUpdates(
            queryRange = queryRange,
            internalUpdateFormat = internalUpdateFormat,
          )
        )
    Source
      .futureSource(futureSource)
      .mapMaterializedValue((_: Future[NotUsed]) => NotUsed)
  }

  override def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] =
    updatePointwiseReader.lookupUpdateBy(
      lookupKey = lookupKey,
      internalUpdateFormat = internalUpdateFormat,
    )

  override def getActiveContracts(
      activeAt: Option[Offset],
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] =
    activeAt match {
      case None => Source.empty
      case Some(offset) =>
        val futureSource = getMaxAcsEventSeqId(offset)
          .map(maxSeqId =>
            acsReader.streamActiveContracts(
              filteringConstraints = filter,
              activeAt = offset -> maxSeqId,
              eventProjectionProperties = eventProjectionProperties,
            )
          )
        Source
          .futureSource(futureSource)
          .mapMaterializedValue((_: Future[NotUsed]) => NotUsed)
    }

  private def getMaxAcsEventSeqId(activeAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Long] =
    queryValidRange.withOffsetNotBeforePruning(
      offset = activeAt,
      errorPruning = pruned =>
        ACSReader.acsBeforePruningErrorReason(
          acsOffset = activeAt,
          prunedUpToOffset = pruned,
        ),
      errorLedgerEnd = ledgerEnd =>
        ACSReader.acsAfterLedgerEndErrorReason(
          acsOffset = activeAt,
          ledgerEndOffset = ledgerEnd,
        ),
    )(
      dispatcher.executeSql(dbMetrics.getAcsEventSeqIdRange)(
        eventStorageBackend.maxEventSequentialId(Some(activeAt))
      )
    )

  private def getEventSeqIdRange(
      startInclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Future[EventsRange] =
    queryValidRange.withRangeNotPruned(
      minOffsetInclusive = startInclusive,
      maxOffsetInclusive = endInclusive,
      errorPruning = (prunedOffset: Offset) =>
        s"Transactions request from ${startInclusive.unwrap} to ${endInclusive.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
      errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
        s"Transactions request from ${startInclusive.unwrap} to ${endInclusive.unwrap} is beyond ledger end offset ${ledgerEndOffset
            .fold(0L)(_.unwrap)}",
    )(dispatcher.executeSql(dbMetrics.getEventSeqIdRange) { connection =>
      EventsRange(
        startInclusiveOffset = startInclusive,
        startInclusiveEventSeqId =
          eventStorageBackend.maxEventSequentialId(startInclusive.decrement)(connection),
        endInclusiveOffset = endInclusive,
        endInclusiveEventSeqId =
          eventStorageBackend.maxEventSequentialId(Some(endInclusive))(connection),
      )
    })

}

private[dao] object UpdateReader {

  def endSpanOnTermination[Mat](
      span: Span
  )(mat: Mat, done: Future[Done])(implicit ec: ExecutionContext): Mat = {
    done.onComplete {
      case Failure(exception) =>
        span.recordException(exception)
        span.end()
      case Success(_) =>
        span.end()
    }
    mat
  }

  /** Groups together items of type [[A]] that share an attribute [[K]] over a contiguous stretch of
    * the input [[Source]]. Well suited to perform group-by operations of streams where [[K]]
    * attributes are either sorted or at least show up in blocks.
    *
    * Implementation detail: this method _must_ use concatSubstreams instead of mergeSubstreams to
    * prevent the substreams to be processed in parallel, potentially causing the outputs to be
    * delivered in a different order.
    *
    * Docs: https://doc.akka.io/docs/akka/2.6.10/stream/stream-substream.html#groupby
    */
  def groupContiguous[A, K, Mat](
      source: Source[A, Mat]
  )(by: A => K): Source[Vector[A], Mat] =
    source
      .statefulMapConcat { () =>
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var previousSegmentKey: Option[K] = None
        entry => {
          val keyForEntry = by(entry)
          val entryWithSplit = entry -> !previousSegmentKey.contains(keyForEntry)
          previousSegmentKey = Some(keyForEntry)
          List(entryWithSplit)
        }
      }
      .splitWhen(_._2)
      .map(_._1)
      .fold(Vector.empty[A])(_ :+ _)
      .concatSubstreams

  def toUnassignedEvent(
      offset: Long,
      rawUnassignEvent: Entry[RawUnassignEventLegacy],
  ): UnassignedEvent =
    UnassignedEvent(
      offset = offset,
      reassignmentId = rawUnassignEvent.event.reassignmentId,
      contractId = rawUnassignEvent.event.contractId.coid,
      templateId =
        Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.event.templateId.toIdentifier)),
      packageName = rawUnassignEvent.event.templateId.pkgName,
      source = rawUnassignEvent.event.sourceSynchronizerId,
      target = rawUnassignEvent.event.targetSynchronizerId,
      submitter = rawUnassignEvent.event.submitter.getOrElse(""),
      reassignmentCounter = rawUnassignEvent.event.reassignmentCounter,
      assignmentExclusivity =
        rawUnassignEvent.event.assignmentExclusivity.map(TimestampConversion.fromLf),
      witnessParties = rawUnassignEvent.event.witnessParties.toSeq,
      nodeId = rawUnassignEvent.nodeId,
    )

  def toApiUnassigned(
      rawUnassignEntries: Seq[Entry[RawUnassignEventLegacy]]
  ): Option[Reassignment] =
    rawUnassignEntries.headOption map { first =>
      Reassignment(
        updateId = first.updateId,
        commandId = first.commandId.getOrElse(""),
        workflowId = first.workflowId.getOrElse(""),
        offset = first.offset,
        events = rawUnassignEntries.map(entry =>
          ReassignmentEvent(
            ReassignmentEvent.Event.Unassigned(
              UpdateReader.toUnassignedEvent(first.offset, entry)
            )
          )
        ),
        recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
        traceContext = first.traceContext.map(DamlTraceContext.parseFrom),
        synchronizerId = first.synchronizerId,
      )
    }

  def toAssignedEvent(
      rawAssignEvent: RawAssignEventLegacy,
      createdEvent: CreatedEvent,
  ): AssignedEvent =
    AssignedEvent(
      source = rawAssignEvent.sourceSynchronizerId,
      target = rawAssignEvent.targetSynchronizerId,
      reassignmentId = rawAssignEvent.reassignmentId,
      submitter = rawAssignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawAssignEvent.reassignmentCounter,
      createdEvent = Some(createdEvent),
    )

  def toApiAssigned(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawAssignEntries: Seq[(Entry[RawAssignEventLegacy], Option[FatContract])]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Reassignment]] =
    MonadUtil
      .sequentialTraverse(rawAssignEntries) { case (rawAssignEntry, fatContractO) =>
        lfValueTranslation
          .toApiCreatedEvent(
            eventProjectionProperties = eventProjectionProperties,
            fatContractInstance = fatContractO.getOrElse(
              throw new IllegalStateException(
                s"Contract for internal contract id ${rawAssignEntry.event.rawCreatedEvent.internalContractId} was not found in the contract store."
              )
            ),
            offset = rawAssignEntry.offset,
            nodeId = rawAssignEntry.nodeId,
            representativePackageId = rawAssignEntry.event.rawCreatedEvent.representativePackageId,
            witnesses = rawAssignEntry.event.rawCreatedEvent.witnessParties,
            acsDelta = rawAssignEntry.event.rawCreatedEvent.flatEventWitnesses.nonEmpty,
          )

      }
      .map(createdEvents =>
        rawAssignEntries.headOption.map { case (first, _) =>
          Reassignment(
            updateId = first.updateId,
            commandId = first.commandId.getOrElse(""),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = rawAssignEntries.zip(createdEvents).map { case ((entry, _), created) =>
              ReassignmentEvent(
                ReassignmentEvent.Event.Assigned(
                  UpdateReader.toAssignedEvent(entry.event, created)
                )
              )
            },
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            traceContext = first.traceContext.map(DamlTraceContext.parseFrom),
            synchronizerId = first.synchronizerId,
          )
        }
      )

  def deserializeRawAcsDeltaEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawFlatEntryFatContract: (Entry[RawAcsDeltaEventLegacy], Option[FatContract])
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] =
    rawFlatEntryFatContract match {
      case (rawFlatEntry, fatContractO) =>
        rawFlatEntry.event match {
          case rawCreated: RawCreatedEventLegacy =>
            lfValueTranslation
              .toApiCreatedEvent(
                eventProjectionProperties = eventProjectionProperties,
                fatContractInstance = fatContractO.getOrElse(
                  throw new IllegalStateException(
                    s"Contract for internal contract id ${rawCreated.internalContractId} was not found in the contract store."
                  )
                ),
                offset = rawFlatEntry.offset,
                nodeId = rawFlatEntry.nodeId,
                representativePackageId = rawCreated.representativePackageId,
                witnesses = rawCreated.witnessParties,
                acsDelta = rawCreated.flatEventWitnesses.nonEmpty,
              )
              .map(createdEvent => rawFlatEntry.withEvent(Event(Event.Event.Created(createdEvent))))

          case rawArchived: RawArchivedEventLegacy =>
            Future.successful(
              rawFlatEntry.withEvent(
                Event(
                  Event.Event.Archived(
                    lfValueTranslation.deserializeRawArchived(
                      eventProjectionProperties,
                      rawFlatEntry.withEvent(rawArchived),
                    )
                  )
                )
              )
            )
        }
    }

  def deserializeRawLedgerEffectsEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTreeEntryFatContract: (Entry[RawLedgerEffectsEventLegacy], Option[FatContract])
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] =
    rawTreeEntryFatContract match {
      case (rawTreeEntry, fatContractO) =>
        rawTreeEntry.event match {
          case rawCreated: RawCreatedEventLegacy =>
            lfValueTranslation
              .toApiCreatedEvent(
                eventProjectionProperties = eventProjectionProperties,
                fatContractInstance = fatContractO.getOrElse(
                  throw new IllegalStateException(
                    s"Contract for internal contract id ${rawCreated.internalContractId} was not found in the contract store."
                  )
                ),
                offset = rawTreeEntry.offset,
                nodeId = rawTreeEntry.nodeId,
                representativePackageId = rawCreated.representativePackageId,
                witnesses = rawCreated.witnessParties,
                acsDelta = rawCreated.flatEventWitnesses.nonEmpty,
              )
              .map(createdEvent =>
                rawTreeEntry.withEvent(
                  Event(Event.Event.Created(createdEvent))
                )
              )

          case rawExercised: RawExercisedEventLegacy =>
            lfValueTranslation
              .deserializeRawExercised(
                eventProjectionProperties,
                rawTreeEntry.withEvent(rawExercised),
              )
              .map(exercisedEvent =>
                rawTreeEntry.copy(
                  event = Event(Event.Event.Exercised(exercisedEvent))
                )
              )
        }
    }
  def filterRawEvents[T <: RawEventLegacy](templatePartiesFilter: TemplatePartiesFilter)(
      rawEvents: Seq[Entry[T]]
  ): Seq[Entry[T]] = {
    val templateWildcardPartiesO = templatePartiesFilter.templateWildcardParties
    val templateSpecifiedPartiesMap = templatePartiesFilter.relation.map {
      case (identifier, partiesO) => (identifier, partiesO.map(_.map(_.toString)))
    }

    templateWildcardPartiesO match {
      // the filter allows all parties for all templates (wildcard)
      case None => rawEvents
      case Some(templateWildcardParties) =>
        val templateWildcardPartiesStrings: Set[String] = templateWildcardParties.toSet[String]
        rawEvents.filter(entry =>
          // at least one of the witnesses exist in the template wildcard filter
          entry.event.witnessParties.exists(templateWildcardPartiesStrings) ||
            (templateSpecifiedPartiesMap.get(entry.event.templateId.toNameTypeConRef) match {
              // the event's template id was not found in the filters
              case None => false
              case Some(partiesO) => partiesO.fold(true)(entry.event.witnessParties.exists)
            })
        )
    }
  }

}
