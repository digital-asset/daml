// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.daml.lf.data.Ref.{Party, SimpleString}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain._
import com.digitalasset.ledger.api.messages.transaction._
import com.digitalasset.ledger.api.v1.transaction.{Transaction => PTransaction}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetTransactionsResponse,
  TransactionServiceLogging
}
import com.digitalasset.ledger.api.validation.PartyNameChecker
import com.digitalasset.ledger.backend.api.v1.LedgerBackend
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.AcceptedTransaction
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter.TransactionIdWithIndex
import com.digitalasset.platform.server.api._
import com.digitalasset.platform.server.api.services.domain.TransactionService
import com.digitalasset.platform.server.api.services.grpc.GrpcTransactionService
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolver}
import com.digitalasset.platform.server.services.transaction._
import io.grpc._
import org.slf4j.LoggerFactory
import scalaz.Tag
import scalaz.syntax.tag._

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

object SandboxTransactionService {

  def createApiService(ledgerBackend: LedgerBackend, identifierResolver: IdentifierResolver)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : GrpcTransactionService with BindableService with TransactionServiceLogging =
    new GrpcTransactionService(
      new SandboxTransactionService(ledgerBackend),
      ledgerBackend.ledgerId,
      PartyNameChecker.AllowAllParties,
      identifierResolver) with TransactionServiceLogging
}

class SandboxTransactionService private (val ledgerBackend: LedgerBackend, parallelism: Int = 4)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    esf: ExecutionSequencerFactory)
    extends TransactionService
    with AutoCloseable
    with ErrorFactories {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  private val transactionPipeline = TransactionPipeline(ledgerBackend)

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  def getTransactions(request: GetTransactionsRequest): Source[GetTransactionsResponse, NotUsed] = {
    val subscriptionId = subscriptionIdCounter.incrementAndGet().toString
    logger.debug(
      "Received request for transaction subscription {}: {}",
      subscriptionId: Any,
      request)

    val eventFilter = EventFilter.byTemplates(request.filter)
    val requestingParties = request.filter.filtersByParty.keys.toList

    transactionPipeline
      .run(request.begin, request.end)
      .mapConcat { trans =>
        val events =
          TransactionConversion
            .genToFlatTransaction(
              trans.transaction,
              trans.explicitDisclosure.mapValues(set => set.map(_.underlyingString)),
              request.verbose)
            .flatMap(eventFilter.filterEvent _)

        val submitterIsSubscriber =
          trans.submitter
            .map(SimpleString.assertFromString)
            .fold(false)(eventFilter.isSubmitterSubscriber)
        if (events.nonEmpty || submitterIsSubscriber) {
          val transaction = PTransaction(
            transactionId = trans.transactionId,
            commandId = if (submitterIsSubscriber) trans.commandId.getOrElse("") else "",
            workflowId = trans.workflowId,
            effectiveAt = Some(fromInstant(trans.recordTime)),
            events = events,
            offset = trans.offset
          )
          val response = GetTransactionsResponse(Seq(transaction))
          logger.debug(
            "Serving item {} (offset: {}) in transaction subscription {} to client",
            transaction.transactionId,
            transaction.offset,
            subscriptionId)
          List(response)
        } else {
          logger.trace(
            "Not serving item {} for transaction subscription {} as no events are visible",
            trans.transactionId,
            subscriptionId: Any)
          Nil
        }

      }
  }

  override def getTransactionTrees(request: GetTransactionTreesRequest)
    : Source[WithOffset[String, VisibleTransaction], NotUsed] = {
    logger.debug("Received {}", request)
    transactionPipeline
      .run(
        request.begin,
        request.end
      )
      .mapConcat { trans =>
        toResponseIfVisible(request, request.parties, trans.offset, trans)
          .fold(List.empty[WithOffset[String, VisibleTransaction]])(e =>
            List(WithOffset(trans.offset, e)))

      }
  }

  private def toResponseIfVisible(
      request: GetTransactionTreesRequest,
      subscribingParties: Set[Party],
      offset: String,
      trans: AcceptedTransaction) = {

    val eventFilter = TransactionFilter(request.parties.map(_ -> Filters.noFilter)(breakOut))
    val withMeta = toTransactionWithMeta(trans)
    VisibleTransaction.toVisibleTransaction(eventFilter, withMeta)
  }

  def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[Option[VisibleTransaction]] = {
    logger.debug("Received {}", request)
    SandboxEventIdFormatter
      .split(request.eventId.unwrap)
      .fold(
        Future.failed[Option[VisibleTransaction]](
          Status.INVALID_ARGUMENT
            .withDescription(s"invalid eventId: ${request.eventId}")
            .asRuntimeException())) {
        case TransactionIdWithIndex(transactionId, index) =>
          ledgerBackend.getCurrentLedgerEnd.flatMap(
            le =>
              lookUpByTransactionId(
                TransactionId(transactionId),
                request.requestingParties,
                le,
                true))
      }
  }

  def getTransactionById(request: GetTransactionByIdRequest): Future[Option[VisibleTransaction]] = {
    logger.debug("Received {}", request)
    ledgerBackend.getCurrentLedgerEnd.flatMap(le =>
      lookUpByTransactionId(request.transactionId, request.requestingParties, le, true))
  }

  def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    ledgerBackend.getCurrentLedgerEnd.map(LedgerOffset.Absolute)

  override lazy val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by(abs => BigInt(abs.value))

  private def lookUpByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
      ledgerEnd: String,
      verbose: Boolean): Future[Option[VisibleTransaction]] =
    //TODO: very inefficient especially with Postgres, see https://github.com/digital-asset/daml/issues/831
    transactionPipeline
      .run(LedgerOffset.LedgerBegin, Some(LedgerOffset.Absolute(ledgerEnd)))
      .collect {
        case t: AcceptedTransaction if t.transactionId == transactionId => t
      }
      .runWith(Sink.headOption)
      .flatMap {
        case Some(trans) =>
          val result = VisibleTransaction.toVisibleTransaction(
            TransactionFilter.allForParties(requestingParties),
            toTransactionWithMeta(trans)
          )
          Future.successful(result)

        case None =>
          Future.failed(
            Status.INVALID_ARGUMENT
              .withDescription(s"$transactionId could not be found")
              .asRuntimeException())
      }

  private def toTransactionWithMeta(trans: AcceptedTransaction) =
    TransactionWithMeta(
      trans.transaction,
      extractMeta(trans)
    )

  private def extractMeta(trans: AcceptedTransaction): TransactionMeta =
    TransactionMeta(
      TransactionId(trans.transactionId),
      Tag.subst(trans.commandId),
      Tag.subst(trans.applicationId),
      trans.submitter.map(Party.assertFromString),
      WorkflowId(trans.workflowId),
      trans.recordTime,
      None
    )

  override def close(): Unit = ()

}
