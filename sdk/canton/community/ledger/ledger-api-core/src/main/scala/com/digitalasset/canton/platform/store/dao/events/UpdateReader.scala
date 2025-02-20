// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, UnassignedEvent}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
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
  RawExercisedEvent,
  RawFlatEvent,
  RawTreeEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.common.TransactionPointwiseQueries.LookupKey
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
import io.opentelemetry.api.trace.Span
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param updatesStreamReader
  *   Knows how to stream updates
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
    treeTransactionsStreamReader: TransactionsTreeStreamReader,
    transactionPointwiseReader: TransactionPointwiseReader,
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
    transactionPointwiseReader.lookupTransactionBy(
      lookupKey = LookupKey.UpdateId(updateId),
      internalTransactionFormat = internalTransactionFormat,
    )

  override def lookupTransactionByOffset(
      offset: data.Offset,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    transactionPointwiseReader.lookupTransactionBy(
      lookupKey = LookupKey.Offset(offset),
      internalTransactionFormat = internalTransactionFormat,
    )

  override def lookupTransactionTreeById(
      updateId: data.UpdateId,
      requestingParties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[GetTransactionTreeResponse]] =
    treeTransactionPointwiseReader.lookupTransactionBy(
      lookupKey = LookupKey.UpdateId(updateId),
      requestingParties = requestingParties,
      eventProjectionProperties = EventProjectionProperties(
        verbose = true,
        templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
      ),
    )

  override def lookupTransactionTreeByOffset(
      offset: data.Offset,
      requestingParties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[GetTransactionTreeResponse]] =
    treeTransactionPointwiseReader.lookupTransactionBy(
      lookupKey = LookupKey.Offset(offset),
      requestingParties = requestingParties,
      eventProjectionProperties = EventProjectionProperties(
        verbose = true,
        templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
      ),
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

  def toUnassignedEvent(rawUnassignEvent: RawUnassignEvent): UnassignedEvent =
    UnassignedEvent(
      unassignId = rawUnassignEvent.unassignId,
      contractId = rawUnassignEvent.contractId,
      templateId = Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId)),
      packageName = rawUnassignEvent.packageName,
      source = rawUnassignEvent.sourceSynchronizerId,
      target = rawUnassignEvent.targetSynchronizerId,
      submitter = rawUnassignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawUnassignEvent.reassignmentCounter,
      assignmentExclusivity =
        rawUnassignEvent.assignmentExclusivity.map(TimestampConversion.fromLf),
      witnessParties = rawUnassignEvent.witnessParties.toSeq,
    )

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
        .deserializeRaw(eventProjectionProperties)(rawCreated)
        .map(createdEvent => rawFlatEntry.copy(event = Event(Event.Event.Created(createdEvent))))

    case rawArchived: RawArchivedEvent =>
      Future.successful(
        rawFlatEntry.copy(
          event = Event(Event.Event.Archived(lfValueTranslation.deserializeRaw(rawArchived)))
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
        .deserializeRaw(eventProjectionProperties)(rawCreated)
        .map(createdEvent =>
          rawTreeEntry.copy(
            event = TreeEvent(TreeEvent.Kind.Created(createdEvent))
          )
        )

    case rawExercised: RawExercisedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties.verbose)(rawExercised)
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
        .deserializeRaw(eventProjectionProperties)(rawCreated)
        .map(createdEvent =>
          rawTreeEntry.copy(
            event = Event(Event.Event.Created(createdEvent))
          )
        )

    case rawExercised: RawExercisedEvent =>
      lfValueTranslation
        .deserializeRaw(eventProjectionProperties.verbose)(rawExercised)
        .map(exercisedEvent =>
          rawTreeEntry.copy(
            event = Event(Event.Event.Exercised(exercisedEvent))
          )
        )
  }
}
