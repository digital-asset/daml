// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent,
  Reassignment,
  ReassignmentEvent,
  UnassignedEvent,
}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.trace_context.TraceContext as DamlTraceContext
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesResponse}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  FatCreatedEventProperties,
  RawArchivedEvent,
  RawEvent,
  RawExercisedEvent,
  RawFatActiveContract,
  RawFatAssignEvent,
  RawFatCreatedEvent,
  RawReassignmentEvent,
  RawThinActiveContract,
  RawThinAssignEvent,
  RawThinCreatedEvent,
  RawThinEvent,
  RawTransactionEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.{FatContract, InternalUpdateFormat, TemplatePartiesFilter}
import com.digitalasset.canton.util.MonadUtil
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Span
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param updatesStreamReader
  *   Knows how to stream updates
  * @param updatePointwiseReader
  *   Knows how to fetch a tree transaction by its id or its offset
  * @param dispatcher
  *   Executes the queries prepared by this object
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
      rawUnassignEvent: RawUnassignEvent
  ): UnassignedEvent =
    UnassignedEvent(
      offset = rawUnassignEvent.offset,
      reassignmentId = rawUnassignEvent.reassignmentId,
      contractId = rawUnassignEvent.contractId.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId.toIdentifier)),
      packageName = rawUnassignEvent.templateId.pkgName,
      source = rawUnassignEvent.sourceSynchronizerId,
      target = rawUnassignEvent.targetSynchronizerId,
      submitter = rawUnassignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawUnassignEvent.reassignmentCounter,
      assignmentExclusivity =
        rawUnassignEvent.assignmentExclusivity.map(TimestampConversion.fromLf),
      witnessParties = rawUnassignEvent.witnessParties.toSeq,
      nodeId = rawUnassignEvent.nodeId,
    )

  def toAssignedEvent(
      rawAssignEvent: RawFatAssignEvent,
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

  def toApiReassignment(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawReassignmentEvents: Seq[RawReassignmentEvent]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Reassignment]] =
    MonadUtil
      .sequentialTraverse(rawReassignmentEvents) {
        case rawAssignEvent: RawFatAssignEvent =>
          lfValueTranslation
            .toApiCreatedEvent(
              eventProjectionProperties = eventProjectionProperties,
              fatContractInstance = rawAssignEvent.fatContract,
              offset = rawAssignEvent.offset,
              nodeId = rawAssignEvent.nodeId,
              representativePackageId = rawAssignEvent.representativePackageId,
              witnesses = rawAssignEvent.witnessParties,
              acsDelta = rawAssignEvent.acsDeltaForWitnesses,
            )
            .map(createdEvent =>
              ReassignmentEvent(
                ReassignmentEvent.Event.Assigned(
                  UpdateReader.toAssignedEvent(rawAssignEvent, createdEvent)
                )
              )
            )

        case rawUnassignEvent: RawUnassignEvent =>
          Future.successful(
            ReassignmentEvent(
              ReassignmentEvent.Event.Unassigned(
                UnassignedEvent(
                  offset = rawUnassignEvent.offset,
                  reassignmentId = rawUnassignEvent.reassignmentId,
                  contractId = rawUnassignEvent.contractId.coid,
                  templateId =
                    Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId.toIdentifier)),
                  packageName = rawUnassignEvent.templateId.pkgName,
                  source = rawUnassignEvent.sourceSynchronizerId,
                  target = rawUnassignEvent.targetSynchronizerId,
                  submitter = rawUnassignEvent.submitter.getOrElse(""),
                  reassignmentCounter = rawUnassignEvent.reassignmentCounter,
                  assignmentExclusivity =
                    rawUnassignEvent.assignmentExclusivity.map(TimestampConversion.fromLf),
                  witnessParties = rawUnassignEvent.witnessParties.toSeq,
                  nodeId = rawUnassignEvent.nodeId,
                )
              )
            )
          )
      }
      .map(reassignments =>
        rawReassignmentEvents.headOption.map { first =>
          Reassignment(
            updateId = first.updateId,
            commandId = first.commandId.getOrElse(""),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = reassignments,
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            traceContext = Some(DamlTraceContext.parseFrom(first.traceContext)),
            synchronizerId = first.synchronizerId,
          )
        }
      )

  def archivedEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(rawArchived: RawArchivedEvent): ArchivedEvent =
    ArchivedEvent(
      offset = rawArchived.offset,
      nodeId = rawArchived.nodeId,
      contractId = rawArchived.contractId.coid,
      templateId = Some(
        LfEngineToApi.toApiIdentifier(rawArchived.templateId.toIdentifier)
      ),
      witnessParties = rawArchived.witnessParties.toSeq,
      packageName = rawArchived.templateId.pkgName,
      implementedInterfaces = lfValueTranslation.implementedInterfaces(
        eventProjectionProperties,
        rawArchived.witnessParties,
        rawArchived.templateId,
      ),
    )

  def deserializeRawTransactionEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTransactionEvent: RawTransactionEvent
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Event] = rawTransactionEvent match {
    case rawCreated: RawFatCreatedEvent =>
      lfValueTranslation
        .toApiCreatedEvent(
          eventProjectionProperties = eventProjectionProperties,
          fatContractInstance = rawCreated.fatContract,
          offset = rawCreated.offset,
          nodeId = rawCreated.nodeId,
          representativePackageId = rawCreated.representativePackageId,
          witnesses = rawCreated.witnessParties,
          acsDelta = rawCreated.acsDeltaForWitnesses,
        )
        .map(createdEvent => Event(Event.Event.Created(createdEvent)))

    case rawArchived: RawArchivedEvent =>
      Future.successful(
        Event(
          Event.Event.Archived(
            archivedEvent(eventProjectionProperties, lfValueTranslation)(
              rawArchived
            )
          )
        )
      )

    case rawExercisedEvent: RawExercisedEvent =>
      lfValueTranslation
        .deserializeRawExercised(
          eventProjectionProperties,
          rawExercisedEvent,
        )
        .map(exercisedEvent => Event(Event.Event.Exercised(exercisedEvent)))
  }

  def toApiTransaction(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTransactionEvents: Seq[RawTransactionEvent]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Transaction]] =
    MonadUtil
      .sequentialTraverse(rawTransactionEvents)(
        deserializeRawTransactionEvent(
          eventProjectionProperties = eventProjectionProperties,
          lfValueTranslation = lfValueTranslation,
        )
      )
      .map(events =>
        rawTransactionEvents.headOption.map { first =>
          Transaction(
            updateId = first.updateId,
            commandId = first.commandId.getOrElse(""),
            effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = events,
            synchronizerId = first.synchronizerId,
            traceContext = Some(DamlTraceContext.parseFrom(first.traceContext)),
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            externalTransactionHash = first.externalTransactionHash.map(ByteString.copyFrom),
          )
        }
      )

  def toApiUpdate[T](
      reassignmentEventProjectionProperties: Option[EventProjectionProperties],
      transactionEventProjectionProperties: Option[EventProjectionProperties],
      lfValueTranslation: LfValueTranslation,
  )(rawEvents: Seq[RawEvent])(
      convertReassignment: Reassignment => T,
      convertTransaction: Transaction => T,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Option[T]] = {
    val reassignmentEvents = rawEvents.collect { case raw: RawReassignmentEvent =>
      raw
    }
    val transactionEvents = rawEvents.collect { case raw: RawTransactionEvent =>
      raw
    }
    if (transactionEvents.nonEmpty && reassignmentEvents.isEmpty) {
      transactionEventProjectionProperties match {
        case Some(eventProjectionProperties) =>
          UpdateReader
            .toApiTransaction(
              eventProjectionProperties = eventProjectionProperties,
              lfValueTranslation = lfValueTranslation,
            )(transactionEvents)
            .map(_.map(convertTransaction))

        case None =>
          throw new IllegalStateException(
            s"no internal event format for transactions and transaction event returned from the stream"
          )
      }
    } else if (reassignmentEvents.nonEmpty && transactionEvents.isEmpty) {
      reassignmentEventProjectionProperties match {
        case Some(eventProjectionProperties) =>
          UpdateReader
            .toApiReassignment(
              eventProjectionProperties = eventProjectionProperties,
              lfValueTranslation = lfValueTranslation,
            )(reassignmentEvents)
            .map(_.map(convertReassignment))

        case None =>
          throw new IllegalStateException(
            s"no internal event format for transactions and transaction event returned from the stream"
          )
      }
    } else if (transactionEvents.isEmpty && reassignmentEvents.isEmpty) {
      Future.successful(None)
    } else {
      throw new IllegalStateException(s"reassignment and transaction events mixed for one offset")
    }
  }

  def eventFilter(templatePartiesFilterO: Option[TemplatePartiesFilter]): RawEvent => Boolean =
    templatePartiesFilterO
      .map { templatePartiesFilter =>
        val templateWildcardPartiesO = templatePartiesFilter.templateWildcardParties
        val templateSpecifiedPartiesMap = templatePartiesFilter.relation.map {
          case (identifier, partiesO) => (identifier, partiesO.map(_.map(_.toString)))
        }
        templateWildcardPartiesO match {
          // the filter allows all parties for all templates (wildcard)
          case None => (_: RawEvent) => true

          case Some(templateWildcardParties) =>
            val templateWildcardPartiesStrings: Set[String] = templateWildcardParties.toSet[String]
            (event: RawEvent) =>
              // at least one of the witnesses exist in the template wildcard filter
              event.witnessParties.exists(templateWildcardPartiesStrings) ||
                (templateSpecifiedPartiesMap.get(event.templateId.toNameTypeConRef) match {
                  // the event's template id was not found in the filters
                  case None => false
                  case Some(partiesO) => partiesO.fold(true)(event.witnessParties.exists)
                })
        }
      }
      .getOrElse(_ => false)

  def filterRawEvents(
      internalUpdateFormat: InternalUpdateFormat
  )(
      rawEvents: Seq[RawEvent]
  ): Seq[RawEvent] = {
    val reassignmentFilter = eventFilter(
      internalUpdateFormat.includeReassignments.map(_.templatePartiesFilter)
    )
    val transactionFilter = eventFilter(
      internalUpdateFormat.includeTransactions.map(_.internalEventFormat.templatePartiesFilter)
    )
    rawEvents.collect {
      case tx: RawTransactionEvent if transactionFilter(tx) => tx
      case r: RawReassignmentEvent if reassignmentFilter(r) => r
    }
  }

  val getInternalContractIdO: RawThinEvent => Option[Long] = {
    case raw: RawThinAssignEvent => Some(raw.thinCreatedEventProperties.internalContractId)
    case raw: RawThinCreatedEvent => Some(raw.thinCreatedEventProperties.internalContractId)
    case raw: RawThinActiveContract => Some(raw.thinCreatedEventProperties.internalContractId)
    case _: RawArchivedEvent => None
    case _: RawUnassignEvent => None
    case _: RawExercisedEvent => None
  }

  val toFatInstance: (RawThinEvent, Option[FatContract]) => RawEvent = {
    case (raw: RawThinAssignEvent, fatContractO) =>
      RawFatAssignEvent(
        reassignmentProperties = raw.reassignmentProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
        sourceSynchronizerId = raw.sourceSynchronizerId,
      )
    case (raw: RawThinCreatedEvent, fatContractO) =>
      RawFatCreatedEvent(
        transactionProperties = raw.transactionProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
      )
    case (raw: RawThinActiveContract, fatContractO) =>
      RawFatActiveContract(
        commonEventProperties = raw.commonEventProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
      )
    case (raw: RawArchivedEvent, _) => raw
    case (raw: RawUnassignEvent, _) => raw
    case (raw: RawExercisedEvent, _) => raw
  }

  def tryFatContract(
      internalContractId: Long,
      fatContractO: Option[FatContract],
  ): FatContract = fatContractO.getOrElse(
    throw new IllegalStateException(
      s"Contract for internal contract id $internalContractId was not found in the contract store."
    )
  )

  def withFatContractIfNeeded(contractStore: LedgerApiContractStore)(
      rawEvents: Vector[RawThinEvent]
  )(implicit
      ec: ExecutionContext,
      ecl: ErrorLoggingContext,
  ): Future[Vector[(RawThinEvent, Option[FatContract])]] =
    contractStore
      .lookupBatchedNonCached(rawEvents.flatMap(getInternalContractIdO))(ecl.traceContext)
      .map(contracts =>
        rawEvents.map(event =>
          event -> getInternalContractIdO(event)
            .flatMap(contracts.get)
            .map(_.inst)
        )
      )

  def tryToResolveFatInstance(
      thinEventsWithFatContract: Vector[(RawThinEvent, Option[FatContract])]
  ): Vector[RawEvent] =
    thinEventsWithFatContract.map(toFatInstance.tupled)
}
