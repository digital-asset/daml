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
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.data
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawArchivedEvent,
  RawAssignEvent,
  RawCreatedEvent,
  RawEvent,
  RawExercisedEvent,
  RawFlatEvent,
  RawTreeEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.{
  InternalTransactionFormat,
  InternalUpdateFormat,
  Party,
  TemplatePartiesFilter,
}
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
    treeTransactionsStreamReader: TransactionsTreeStreamReader,
    treeTransactionPointwiseReader: TransactionTreePointwiseReader,
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

  override def lookupTransactionById(
      updateId: data.UpdateId,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    updatePointwiseReader
      .lookupUpdateBy(
        lookupKey = LookupKey.UpdateId(updateId),
        internalUpdateFormat = InternalUpdateFormat(
          includeTransactions = Some(internalTransactionFormat),
          includeReassignments = None,
          includeTopologyEvents = None,
        ),
      )
      .map(_.flatMap(_.update.transaction))
      .map(_.map(tx => GetTransactionResponse(transaction = Some(tx))))

  override def lookupTransactionByOffset(
      offset: data.Offset,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    updatePointwiseReader
      .lookupUpdateBy(
        lookupKey = LookupKey.Offset(offset),
        internalUpdateFormat = InternalUpdateFormat(
          includeTransactions = Some(internalTransactionFormat),
          includeReassignments = None,
          includeTopologyEvents = None,
        ),
      )
      .map(_.flatMap(_.update.transaction))
      .map(_.map(tx => GetTransactionResponse(transaction = Some(tx))))

  override def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] =
    updatePointwiseReader.lookupUpdateBy(
      lookupKey = lookupKey,
      internalUpdateFormat = internalUpdateFormat,
    )

  override def lookupTransactionTreeById(
      updateId: data.UpdateId,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[GetTransactionTreeResponse]] =
    treeTransactionPointwiseReader.lookupTransactionBy(
      lookupKey = LookupKey.UpdateId(updateId),
      requestingParties = requestingParties,
      eventProjectionProperties = eventProjectionProperties,
    )

  override def lookupTransactionTreeByOffset(
      offset: data.Offset,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[GetTransactionTreeResponse]] =
    treeTransactionPointwiseReader.lookupTransactionBy(
      lookupKey = LookupKey.Offset(offset),
      requestingParties = requestingParties,
      eventProjectionProperties = eventProjectionProperties,
    )

  override def getTransactionTrees(
      startInclusive: Offset,
      endInclusive: Offset,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
    val futureSource =
      getEventSeqIdRange(startInclusive, endInclusive)
        .map(queryRange =>
          treeTransactionsStreamReader.streamTreeTransaction(
            queryRange = queryRange,
            requestingParties = requestingParties,
            eventProjectionProperties = eventProjectionProperties,
          )
        )
    Source
      .futureSource(futureSource)
      .mapMaterializedValue((_: Future[NotUsed]) => NotUsed)
  }

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
    dispatcher
      .executeSql(dbMetrics.getAcsEventSeqIdRange)(implicit connection =>
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
          eventStorageBackend.maxEventSequentialId(Some(activeAt))(connection)
        )
      )

  private def getEventSeqIdRange(
      startInclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Future[EventsRange] =
    dispatcher
      .executeSql(dbMetrics.getEventSeqIdRange)(implicit connection =>
        queryValidRange.withRangeNotPruned(
          minOffsetInclusive = startInclusive,
          maxOffsetInclusive = endInclusive,
          errorPruning = (prunedOffset: Offset) =>
            s"Transactions request from ${startInclusive.unwrap} to ${endInclusive.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
          errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
            s"Transactions request from ${startInclusive.unwrap} to ${endInclusive.unwrap} is beyond ledger end offset ${ledgerEndOffset
                .fold(0L)(_.unwrap)}",
        ) {
          EventsRange(
            startInclusiveOffset = startInclusive,
            startInclusiveEventSeqId =
              eventStorageBackend.maxEventSequentialId(startInclusive.decrement)(connection),
            endInclusiveOffset = endInclusive,
            endInclusiveEventSeqId =
              eventStorageBackend.maxEventSequentialId(Some(endInclusive))(connection),
          )
        }
      )

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

  def toUnassignedEvent(offset: Long, rawUnassignEvent: RawUnassignEvent): UnassignedEvent =
    UnassignedEvent(
      offset = offset,
      unassignId = rawUnassignEvent.unassignId,
      contractId = rawUnassignEvent.contractId.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId)),
      packageName = rawUnassignEvent.packageName,
      source = rawUnassignEvent.sourceSynchronizerId,
      target = rawUnassignEvent.targetSynchronizerId,
      submitter = rawUnassignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawUnassignEvent.reassignmentCounter,
      assignmentExclusivity =
        rawUnassignEvent.assignmentExclusivity.map(TimestampConversion.fromLf),
      witnessParties = rawUnassignEvent.witnessParties.toSeq,
      nodeId = rawUnassignEvent.nodeId,
    )

  def toApiUnassigned(
      rawUnassignEntries: Seq[Entry[RawUnassignEvent]]
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
              UpdateReader.toUnassignedEvent(first.offset, entry.event)
            )
          )
        ),
        recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
        traceContext = first.traceContext.map(DamlTraceContext.parseFrom),
      )
    }

  def toAssignedEvent(
      rawAssignEvent: RawAssignEvent,
      createdEvent: CreatedEvent,
  ): AssignedEvent =
    AssignedEvent(
      source = rawAssignEvent.sourceSynchronizerId,
      target = rawAssignEvent.targetSynchronizerId,
      unassignId = rawAssignEvent.unassignId,
      submitter = rawAssignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawAssignEvent.reassignmentCounter,
      createdEvent = Some(createdEvent),
    )

  def toApiAssigned(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawAssignEntries: Seq[Entry[RawAssignEvent]]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Reassignment]] =
    MonadUtil
      .sequentialTraverse(rawAssignEntries) { rawAssignEntry =>
        lfValueTranslation
          .deserializeRaw(
            eventProjectionProperties,
            rawAssignEntry.event.rawCreatedEvent,
          )
      }
      .map(createdEvents =>
        rawAssignEntries.headOption.map(first =>
          Reassignment(
            updateId = first.updateId,
            commandId = first.commandId.getOrElse(""),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = rawAssignEntries.zip(createdEvents).map { case (entry, created) =>
              ReassignmentEvent(
                ReassignmentEvent.Event.Assigned(
                  UpdateReader.toAssignedEvent(entry.event, created)
                )
              )
            },
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            traceContext = first.traceContext.map(DamlTraceContext.parseFrom),
          )
        )
      )

  def deserializeRawFlatEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawFlatEntry: Entry[RawFlatEvent]
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] = rawFlatEntry.event match {
    case rawCreated: RawCreatedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties, rawCreated)
        .map(createdEvent => rawFlatEntry.copy(event = Event(Event.Event.Created(createdEvent))))

    case rawArchived: RawArchivedEvent =>
      Future.successful(
        rawFlatEntry.copy(
          event = Event(
            Event.Event.Archived(
              lfValueTranslation.deserializeRaw(eventProjectionProperties, rawArchived)
            )
          )
        )
      )
  }

  // TODO(#23504) cleanup
  def deserializeTreeEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTreeEntry: Entry[RawTreeEvent]
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[TreeEvent]] = rawTreeEntry.event match {
    case rawCreated: RawCreatedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties, rawCreated)
        .map(createdEvent =>
          rawTreeEntry.copy(
            event = TreeEvent(TreeEvent.Kind.Created(createdEvent))
          )
        )

    case rawExercised: RawExercisedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties, rawExercised)
        .map(exercisedEvent =>
          rawTreeEntry.copy(
            event = TreeEvent(TreeEvent.Kind.Exercised(exercisedEvent))
          )
        )
  }

  def deserializeRawTreeEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTreeEntry: Entry[RawTreeEvent]
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] = rawTreeEntry.event match {
    case rawCreated: RawCreatedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties, rawCreated)
        .map(createdEvent =>
          rawTreeEntry.copy(
            event = Event(Event.Event.Created(createdEvent))
          )
        )

    case rawExercised: RawExercisedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties, rawExercised)
        .map(exercisedEvent =>
          rawTreeEntry.copy(
            event = Event(Event.Event.Exercised(exercisedEvent))
          )
        )
  }

  def filterRawEvents[T <: RawEvent](templatePartiesFilter: TemplatePartiesFilter)(
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
            (templateSpecifiedPartiesMap.get(entry.event.templateId) match {
              // the event's template id was not found in the filters
              case None => false
              case Some(partiesO) => partiesO.fold(true)(entry.event.witnessParties.exists)
            })
        )
    }
  }

}
