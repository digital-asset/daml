// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.server.damlonx.services.backport.EventFilter.TemplateAwareFilter
import com.daml.ledger.api.server.damlonx.services.backport._
import com.daml.ledger.participant.state.index.v1.{IndexService, TransactionAccepted}
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Transaction.NodeId
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain._
import com.digitalasset.ledger.api.messages.transaction.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesRequest,
  GetTransactionsRequest
}
import com.digitalasset.ledger.api.v1.transaction.{Transaction => PTransaction}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetTransactionsResponse,
  TransactionServiceGrpc,
  TransactionServiceLogging
}
import com.digitalasset.ledger.api.validation.PartyNameChecker
import com.digitalasset.platform.server.api._
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.services.transaction.{_}
import io.grpc._
import org.slf4j.LoggerFactory
import scalaz.Tag
import scalaz.syntax.tag._

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object DamlOnXTransactionService {

  def create(ledgerId: String, indexService: IndexService)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory): TransactionServiceGrpc.TransactionService
    with BindableService
    with TransactionServiceLogging =
    new GrpcTransactionService(
      new DamlOnXTransactionService(indexService),
      LedgerId(ledgerId),
      PartyNameChecker.AllowAllParties
    ) with TransactionServiceLogging
}

class DamlOnXTransactionService private (val indexService: IndexService, parallelism: Int = 4)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    esf: ExecutionSequencerFactory)
    extends TransactionService
    with AutoCloseable
    with ErrorFactories {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  def getTransactions(request: GetTransactionsRequest): Source[GetTransactionsResponse, NotUsed] = {
    val subscriptionId = subscriptionIdCounter.incrementAndGet().toString
    logger.debug(
      "Received request for transaction subscription {}: {}",
      subscriptionId: Any,
      request)

    val eventFilter = EventFilter.byTemplates(request.filter)
    runTransactionPipeline(request.ledgerId, request.begin, request.end, request.filter)
      .mapConcat {
        case (offset, (trans, blindingInfo)) =>
          acceptedToFlat(offset, trans, blindingInfo, request.verbose, eventFilter) match {
            case Some(transaction) =>
              val response = GetTransactionsResponse(Seq(transaction))
              logger.debug(
                "Serving item {} (offset: {}) in transaction subscription {} to client",
                transaction.transactionId,
                transaction.offset,
                subscriptionId)
              List(response)

            case None =>
              logger.trace(
                "Not serving item {} for transaction subscription {} as no events are visible",
                trans.transactionId,
                subscriptionId: Any)
              Nil
          }
      }
  }

  private def acceptedToFlat(
      offset: Offset,
      trans: TransactionAccepted,
      blindingInfo: BlindingInfo,
      verbose: Boolean,
      eventFilter: TemplateAwareFilter) = {
    val transactionWithEventIds =
      trans.transaction.mapNodeId(nodeIdToEventId(trans.transactionId, _))
    val events =
      TransactionConversion
        .genToFlatTransaction(
          transactionWithEventIds,
          blindingInfo.explicitDisclosure.map {
            case (nodeId, parties) =>
              nodeIdToEventId(trans.transactionId, nodeId) -> parties
          },
          verbose
        )
        .flatMap(eventFilter.filterEvent(_).toList)

    val submitterIsSubscriber =
      trans.optSubmitterInfo
        .map(_.submitter)
        .fold(false)(eventFilter.isSubmitterSubscriber)
    if (events.nonEmpty || submitterIsSubscriber) {
      Some(
        PTransaction(
          transactionId = trans.transactionId,
          commandId =
            if (submitterIsSubscriber)
              trans.optSubmitterInfo.map(_.commandId).getOrElse("")
            else "",
          workflowId = trans.transactionMeta.workflowId.getOrElse(""),
          effectiveAt = Some(fromInstant(trans.transactionMeta.ledgerEffectiveTime.toInstant)), // FIXME(JM): conversion
          events = events,
          offset = offset.toLedgerString,
        ))
    } else {
      None
    }
  }

  override def getTransactionTrees(request: GetTransactionTreesRequest)
    : Source[WithOffset[String, VisibleTransaction], NotUsed] = {
    logger.debug("Received {}", request)
    val filter = TransactionFilter(request.parties.map(_ -> Filters.noFilter)(breakOut))

    runTransactionPipeline(
      request.ledgerId,
      request.begin,
      request.end,
      filter,
    ).mapConcat {
      case (offset, (trans, _blindingInfo)) =>
        toResponseIfVisible(request, request.parties, trans)
          .fold(List.empty[WithOffset[String, VisibleTransaction]])(e =>
            List(WithOffset(offset.toLedgerString, e)))
    }
  }

  private def toResponseIfVisible(
      request: GetTransactionTreesRequest,
      subscribingParties: Set[Ref.Party],
      trans: TransactionAccepted) = {
    val eventFilter = TransactionFilter(request.parties.map(_ -> Filters.noFilter)(breakOut))
    val withMeta = toTransactionWithMeta(trans)
    VisibleTransaction.toVisibleTransaction(eventFilter, withMeta)
  }

  private def isMultiPartySubscription(filter: domain.TransactionFilter): Boolean = {
    filter.filtersByParty.size > 1
  }

  def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[Option[VisibleTransaction]] = {
    logger.debug("Received {}", request)
    eventIdToTransactionId(request.eventId) match {
      case None => Future.successful(None)
      case Some(txId) =>
        lookupTransactionTreeById(request.ledgerId, txId, request.requestingParties)
    }
  }

  def getTransactionById(request: GetTransactionByIdRequest): Future[Option[VisibleTransaction]] = {
    logger.debug("Received {}", request)

    lookupTransactionTreeById(
      request.ledgerId,
      request.transactionId.unwrap,
      request.requestingParties)
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[Option[PTransaction]] = {
    eventIdToTransactionId(request.eventId) match {
      case None => Future.successful(None)
      case Some(txId) =>
        lookupFlatTransactionById(request.ledgerId, txId, request.requestingParties)
    }
  }

  override def getFlatTransactionById(
      req: GetTransactionByIdRequest): Future[Option[PTransaction]] = {
    lookupFlatTransactionById(req.ledgerId, req.transactionId.unwrap, req.requestingParties)
  }

  private def lookupTransactionTreeById[A](
      ledgerId: LedgerId,
      txId: String,
      requestingParties: Set[Ref.Party]): Future[Option[VisibleTransaction]] = {
    val filter = TransactionFilter.allForParties(requestingParties)

    // FIXME(JM): Move to IndexService

    runTransactionPipeline(ledgerId, LedgerOffset.LedgerBegin, Some(LedgerOffset.LedgerEnd), filter)
      .collect {
        case (o, (t: TransactionAccepted, bi)) if t.transactionId == txId => (o, t, bi)
      }
      .runWith(Sink.headOption)
      .flatMap {
        case Some((_, trans, _)) =>
          val result = VisibleTransaction.toVisibleTransaction(
            filter,
            toTransactionWithMeta(trans)
          )
          Future.successful(result)

        case None =>
          logger.error("lookupTransactionTreeById: Failing with NOT_FOUND")
          Future.failed(
            Status.NOT_FOUND
              .withDescription(s"$txId could not be found")
              .asRuntimeException)
      }
  }

  private def lookupFlatTransactionById[A](
      ledgerId: LedgerId,
      txId: String,
      requestingParties: Set[Ref.Party]): Future[Option[PTransaction]] = {
    val filter = TransactionFilter.allForParties(requestingParties)

    // FIXME(JM): Move to IndexService

    runTransactionPipeline(ledgerId, LedgerOffset.LedgerBegin, Some(LedgerOffset.LedgerEnd), filter)
      .collect {
        case (o, (t: TransactionAccepted, bi)) if t.transactionId == txId => (o, t, bi)
      }
      .runWith(Sink.headOption)
      .flatMap {
        case Some((offset, trans, blindingInfo)) =>
          val eventFilter = EventFilter.byTemplates(
            TransactionFilter(requestingParties.map(_ -> Filters.noFilter)(breakOut)))
          Future.successful(
            acceptedToFlat(offset, trans, blindingInfo, verbose = true, eventFilter))

        case None =>
          logger.error("lookupTransactionTreeById: Failing with NOT_FOUND")
          Future.failed(
            Status.NOT_FOUND
              .withDescription(s"$txId could not be found")
              .asRuntimeException)
      }
  }

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    indexService.getLedgerEnd
      .map(offset =>
        LedgerOffset.Absolute(Ref.LedgerString.assertFromString(offset.toLedgerString)))

  override lazy val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by(abs => Offset.assertFromString(abs.value))

  private def runTransactionPipeline(
      ledgerId: LedgerId,
      begin: LedgerOffset,
      end: Option[LedgerOffset],
      filter: TransactionFilter): Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed] = {

    logger.trace(s"runTransactionPipeline: begin=$begin, end=$end, filter=$filter")

    val ledgerBounds =
      indexService.getLedgerBeginning
        .flatMap { b =>
          indexService.getLedgerEnd
            .map(e => (b, e))
        }

    Source
      .fromFuture(ledgerBounds)
      .flatMapConcat {
        case (ledgerBegin, ledgerEnd) =>
          OffsetSection(begin, end)(getOffsetHelper(ledgerBegin, ledgerEnd)) match {
            case Failure(exception) =>
              logger.error(s"offset section fail: $exception")
              Source.failed(exception)
            case Success(value) =>
              value match {
                case OffsetSection.Empty =>
                  logger.warn(
                    s"runTransactionPipeline: Empty OffsetSection from $begin to $end, given bounds $ledgerBegin and $ledgerEnd")
                  Source.empty
                case OffsetSection.NonEmpty(subscribeFrom, subscribeUntil) =>
                  logger.trace(
                    s"runTransactionPipeline: getAcceptedTransactions with $subscribeFrom to $subscribeUntil")
                  indexService
                    .getAcceptedTransactions(Some(subscribeFrom), subscribeUntil, filter)

              }
          }
      }
  }

  private def getOffsetHelper(ledgerBeginning: Offset, ledgerEnd: Offset) = {
    new OffsetHelper[Offset] {
      override def fromOpaque(opaque: Ref.LedgerString): Try[Offset] =
        Try(Offset.assertFromString(opaque))

      override def getLedgerBeginning(): Offset = ledgerBeginning

      override def getLedgerEnd(): Offset = ledgerEnd

      override def compare(o1: Offset, o2: Offset): Int =
        o1.compare(o2)
    }
  }

  private val `:` = Ref.LedgerString.assertFromString(":")

  private def nodeIdToEventId(txId: Ref.TransactionIdString, nodeId: NodeId): Ref.LedgerString =
    Ref.LedgerString.concat(txId, `:`, nodeId.name)

  private def eventIdToTransactionId(eventId: EventId): Option[String] =
    eventId.unwrap.split(':').headOption

  private def toTransactionWithMeta(trans: TransactionAccepted) =
    TransactionWithMeta(
      trans.transaction.mapNodeId(nodeIdToEventId(trans.transactionId, _)),
      extractMeta(trans)
    )

  private def extractMeta(trans: TransactionAccepted): TransactionMeta =
    TransactionMeta(
      TransactionId(trans.transactionId),
      Tag.subst(trans.optSubmitterInfo.map(_.commandId)),
      Tag.subst(trans.optSubmitterInfo.map(_.applicationId)),
      trans.optSubmitterInfo.map(_.submitter),
      trans.transactionMeta.workflowId.map(WorkflowId(_)),
      trans.transactionMeta.ledgerEffectiveTime.toInstant,
      None
    )

  override def close(): Unit = ()

}
