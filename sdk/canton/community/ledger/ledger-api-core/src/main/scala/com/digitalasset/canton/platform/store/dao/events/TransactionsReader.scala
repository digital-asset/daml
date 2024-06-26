// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, UnassignedEvent}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawAssignEvent,
  RawCreatedEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoTransactionsReader,
}
import com.digitalasset.canton.platform.store.serialization.Compression
import com.digitalasset.canton.platform.{Party, TemplatePartiesFilter}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.ledger.EventId
import com.digitalasset.daml.lf.transaction.NodeId
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Span
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param flatTransactionsStreamReader Knows how to stream flat transactions
  * @param treeTransactionsStreamReader Knows how to stream tree transactions
  * @param flatTransactionPointwiseReader Knows how to fetch a flat transaction by its id
  * @param treeTransactionPointwiseReader Knows how to fetch a tree transaction by its id
  * @param dispatcher Executes the queries prepared by this object
  * @param queryValidRange
  * @param eventStorageBackend
  * @param metrics
  * @param acsReader Knows how to streams ACS
  * @param executionContext Runs transformations on data fetched from the database, including Daml-LF value deserialization
  */
private[dao] final class TransactionsReader(
    flatTransactionsStreamReader: TransactionsFlatStreamReader,
    treeTransactionsStreamReader: TransactionsTreeStreamReader,
    flatTransactionPointwiseReader: TransactionFlatPointwiseReader,
    treeTransactionPointwiseReader: TransactionTreePointwiseReader,
    dispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
    acsReader: ACSReader,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  private val dbMetrics = metrics.index.db

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val futureSource = getEventSeqIdRange(startExclusive, endInclusive)
      .map(queryRange =>
        flatTransactionsStreamReader.streamFlatTransactions(
          queryRange,
          filter,
          eventProjectionProperties,
        )
      )
    Source
      .futureSource(futureSource)
      .mapMaterializedValue((_: Future[NotUsed]) => NotUsed)
  }

  override def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] = {
    flatTransactionPointwiseReader.lookupTransactionById(
      transactionId = transactionId,
      requestingParties = requestingParties,
      eventProjectionProperties = EventProjectionProperties(
        verbose = true,
        templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
      ),
    )
  }

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[GetTransactionTreeResponse]] = {
    treeTransactionPointwiseReader.lookupTransactionById(
      transactionId = transactionId,
      requestingParties = requestingParties,
      eventProjectionProperties = EventProjectionProperties(
        verbose = true,
        templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
      ),
    )
  }

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
    val futureSource = getEventSeqIdRange(startExclusive, endInclusive)
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
      activeAt: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val futureSource = getMaxAcsEventSeqId(activeAt)
      .map(maxSeqId =>
        acsReader.streamActiveContracts(
          filter,
          activeAt -> maxSeqId,
          eventProjectionProperties,
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
          eventStorageBackend.maxEventSequentialId(activeAt)(connection)
        )
      )

  private def getEventSeqIdRange(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Future[EventsRange] =
    dispatcher
      .executeSql(dbMetrics.getEventSeqIdRange)(implicit connection =>
        queryValidRange.withRangeNotPruned(
          minOffsetExclusive = startExclusive,
          maxOffsetInclusive = endInclusive,
          errorPruning = (prunedOffset: Offset) =>
            s"Transactions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} precedes pruned offset ${prunedOffset.toHexString}",
          errorLedgerEnd = (ledgerEndOffset: Offset) =>
            s"Transactions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} is beyond ledger end offset ${ledgerEndOffset.toHexString}",
        ) {
          EventsRange(
            startExclusiveOffset = startExclusive,
            startExclusiveEventSeqId = eventStorageBackend.maxEventSequentialId(
              startExclusive
            )(connection),
            endInclusiveOffset = endInclusive,
            endInclusiveEventSeqId =
              eventStorageBackend.maxEventSequentialId(endInclusive)(connection),
          )
        }
      )

}

private[dao] object TransactionsReader {

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

  def deserializeEntry[E](
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      entry: EventStorageBackend.Entry[Raw[E]]
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[EventStorageBackend.Entry[E]] =
    deserializeEvent(eventProjectionProperties, lfValueTranslation)(entry).map(event =>
      entry.copy(event = event)
    )

  private def deserializeEvent[E](
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(entry: EventStorageBackend.Entry[Raw[E]])(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[E] =
    entry.event.applyDeserialization(lfValueTranslation, eventProjectionProperties)

  /** Groups together items of type [[A]] that share an attribute [[K]] over a
    * contiguous stretch of the input [[Source]]. Well suited to perform group-by
    * operations of streams where [[K]] attributes are either sorted or at least
    * show up in blocks.
    *
    * Implementation detail: this method _must_ use concatSubstreams instead of
    * mergeSubstreams to prevent the substreams to be processed in parallel,
    * potentially causing the outputs to be delivered in a different order.
    *
    * Docs: https://doc.akka.io/docs/akka/2.6.10/stream/stream-substream.html#groupby
    */
  def groupContiguous[A, K, Mat](
      source: Source[A, Mat]
  )(by: A => K): Source[Vector[A], Mat] =
    source
      .statefulMapConcat(() => {
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var previousSegmentKey: Option[K] = None
        entry => {
          val keyForEntry = by(entry)
          val entryWithSplit = entry -> !previousSegmentKey.contains(keyForEntry)
          previousSegmentKey = Some(keyForEntry)
          List(entryWithSplit)
        }
      })
      .splitWhen(_._2)
      .map(_._1)
      .fold(Vector.empty[A])(_ :+ _)
      .concatSubstreams

  def deserializeRawCreatedEvent(
      lfValueTranslation: LfValueTranslation,
      eventProjectionProperties: EventProjectionProperties,
  )(rawCreatedEvent: RawCreatedEvent)(implicit
      lc: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[CreatedEvent] =
    lfValueTranslation
      .deserializeRaw(
        createdEvent = rawCreatedEvent,
        createArgument = rawCreatedEvent.createArgument,
        createArgumentCompression = Compression.Algorithm
          .assertLookup(rawCreatedEvent.createArgumentCompression),
        createKeyValue = rawCreatedEvent.createKeyValue,
        createKeyValueCompression = Compression.Algorithm
          .assertLookup(rawCreatedEvent.createKeyValueCompression),
        templateId = rawCreatedEvent.templateId,
        witnesses = rawCreatedEvent.witnessParties,
        eventProjectionProperties = eventProjectionProperties,
      )
      .map(apiContractData =>
        CreatedEvent(
          eventId = EventId(
            Ref.LedgerString.assertFromString(rawCreatedEvent.updateId),
            NodeId(0), // all create Node ID is set synthetically to 0
          ).toLedgerString,
          contractId = rawCreatedEvent.contractId,
          templateId = Some(
            LfEngineToApi.toApiIdentifier(rawCreatedEvent.templateId)
          ),
          contractKey = apiContractData.contractKey,
          createArguments = apiContractData.createArguments,
          createdEventBlob = apiContractData.createdEventBlob.getOrElse(ByteString.EMPTY),
          interfaceViews = apiContractData.interfaceViews,
          witnessParties = rawCreatedEvent.witnessParties.toList,
          signatories = rawCreatedEvent.signatories.toList,
          observers = rawCreatedEvent.observers.toList,
          createdAt = Some(TimestampConversion.fromLf(rawCreatedEvent.ledgerEffectiveTime)),
          packageName = rawCreatedEvent.packageName,
        )
      )

  def toUnassignedEvent(rawUnassignEvent: RawUnassignEvent): UnassignedEvent =
    UnassignedEvent(
      unassignId = rawUnassignEvent.unassignId,
      contractId = rawUnassignEvent.contractId,
      templateId = Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId)),
      packageName = rawUnassignEvent.packageName,
      source = rawUnassignEvent.sourceDomainId,
      target = rawUnassignEvent.targetDomainId,
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
      source = rawAssignEvent.sourceDomainId,
      target = rawAssignEvent.targetDomainId,
      unassignId = rawAssignEvent.unassignId,
      submitter = rawAssignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawAssignEvent.reassignmentCounter,
      createdEvent = Some(createdEvent),
    )
}
