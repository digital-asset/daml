// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics._
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform._
import com.daml.platform.ApiOffset
import com.daml.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoTransactionsReader,
}
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.dao.events.EventsTable.TransactionConversions
import com.daml.platform.store.utils.Telemetry
import com.daml.telemetry
import com.daml.telemetry.{SpanAttribute, Spans}
import io.opentelemetry.api.trace.Span

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param dbDispatcher Executes the queries prepared by this object
  * @param executionContext Runs transformations on data fetched from the database, including Daml-LF value deserialization
  * @param payloadProcessingParallelism The parallelism for loading and decoding event payloads
  * @param lfValueTranslation The delegate in charge of translating serialized Daml-LF values
  */
private[dao] final class TransactionsReader(
    flatTransactionsStreamReader: FlatTransactionsStreamReader,
    treeTransactionsStreamReader: TreeTransactionsStreamReader,
    flatTransactionPointwiseReader: FlatTransactionPointwiseReader,
    treeTransactionPointwiseReader: TreeTransactionPointwiseReader,
    dbDispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    payloadProcessingParallelism: Int,
    metrics: Metrics,
    lfValueTranslation: LfValueTranslation,
    acsReader: ACSReader,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  import TransactionsReader._

  private val dbMetrics = metrics.daml.index.db
  private val eventSeqIdReader =
    new EventsRange.EventSeqIdReader(eventStorageBackend.maxEventSequentialIdOfAnObservableEvent)

  private val logger = ContextualizedLogger.get(this.getClass)

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
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
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] = {
    flatTransactionPointwiseReader.lookupTransactionById(
      transactionId = transactionId,
      requestingParties = requestingParties,
      eventProjectionProperties = EventProjectionProperties(
        verbose = true,
        witnessTemplateIdFilter = requestingParties.map(_.toString -> Set.empty[Identifier]).toMap,
      ),
    )
  }

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] = {
    treeTransactionPointwiseReader.lookupTransactionById(
      transactionId = transactionId,
      requestingParties = requestingParties,
      eventProjectionProperties = EventProjectionProperties(
        verbose = true,
        witnessTemplateIdFilter = requestingParties.map(_.toString -> Set.empty[Identifier]).toMap,
      ),
    )
  }

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val requestedRangeF: Future[EventsRange[(Offset, Long)]] =
      getEventSeqIdRange(startExclusive, endInclusive)
    val futureSource = requestedRangeF.map(queryRange =>
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
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] = {
    val contextualizedErrorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)
    val span =
      Telemetry.Transactions.createSpan(activeAt)(qualifiedNameOfCurrentFunc)

    logger.debug(
      s"getActiveContracts($activeAt, $filter, $eventProjectionProperties)"
    )

    Source
      .futureSource(
        getAcsEventSeqIdRange(activeAt)
          .map(requestedRange => acsReader.acsStream(filter, requestedRange.endInclusive))
      )
      .mapAsync(payloadProcessingParallelism) {
        rawResult: Vector[EventStorageBackend.Entry[Raw.FlatEvent]] =>
          Timed.future(
            future = Future(
              Future.traverse(rawResult)(
                deserializeEntry(eventProjectionProperties, lfValueTranslation)
              )
            ).flatMap(identity),
            timer = dbMetrics.getActiveContracts.translationTimer,
          )
      }
      .mapConcat(TransactionConversions.toGetActiveContractsResponse(_)(contextualizedErrorLogger))
      .wireTap(response => {
        Spans.addEventToSpan(
          telemetry.Event("contract", Map((SpanAttribute.Offset, response.offset))),
          span,
        )
      })
      .mapMaterializedValue(_ => NotUsed)
      .watchTermination()(endSpanOnTermination(span))
  }

  private def getAcsEventSeqIdRange(activeAt: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[EventsRange[(Offset, Long)]] =
    dbDispatcher
      .executeSql(dbMetrics.getAcsEventSeqIdRange)(implicit connection =>
        queryNonPruned.executeSql(
          eventSeqIdReader.readEventSeqIdRange(activeAt)(connection),
          activeAt,
          pruned =>
            s"Active contracts request after ${activeAt.toHexString} precedes pruned offset ${pruned.toHexString}",
        )
      )
      .map { x =>
        EventsRange(
          startExclusive = (Offset.beforeBegin, 0),
          endInclusive = (activeAt, x.endInclusive),
        )
      }

  private def getEventSeqIdRange(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Future[EventsRange[(Offset, Long)]] =
    dbDispatcher
      .executeSql(dbMetrics.getEventSeqIdRange)(implicit connection =>
        queryNonPruned.executeSql(
          eventSeqIdReader.readEventSeqIdRange(EventsRange(startExclusive, endInclusive))(
            connection
          ),
          startExclusive,
          pruned =>
            s"Transactions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} precedes pruned offset ${pruned.toHexString}",
        )
      )
      .map(x =>
        EventsRange(
          startExclusive = (startExclusive, x.startExclusive),
          endInclusive = (endInclusive, x.endInclusive),
        )
      )

}

private[dao] object TransactionsReader {

  def offsetFor(response: GetTransactionsResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  def offsetFor(response: GetTransactionTreesResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  def endSpanOnTermination[Mat, Out](
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
      loggingContext: LoggingContext,
      ec: ExecutionContext,
  ): Future[EventStorageBackend.Entry[E]] =
    deserializeEvent(eventProjectionProperties, lfValueTranslation)(entry).map(event =>
      entry.copy(event = event)
    )

  private def deserializeEvent[E](
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(entry: EventStorageBackend.Entry[Raw[E]])(implicit
      loggingContext: LoggingContext,
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
}
