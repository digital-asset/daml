// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.IndexTransactionsService
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain._
import com.digitalasset.ledger.api.messages.transaction._
import com.digitalasset.ledger.api.validation.PartyNameChecker
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter.TransactionIdWithIndex
import com.digitalasset.platform.server.api.services.domain.TransactionService
import com.digitalasset.platform.server.api.services.grpc.GrpcTransactionService
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolver}
import io.grpc._
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._
import com.digitalasset.ledger.api.v1.transaction_service.{TransactionServiceLogging}

import scala.concurrent.{ExecutionContext, Future}

object ApiTransactionService {

  def create(
      ledgerId: LedgerId,
      transactionsService: IndexTransactionsService,
      identifierResolver: IdentifierResolver)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : GrpcTransactionService with BindableService with TransactionServiceLogging =
    new GrpcTransactionService(
      new ApiTransactionService(transactionsService),
      ledgerId,
      PartyNameChecker.AllowAllParties,
      identifierResolver) with TransactionServiceLogging
}

class ApiTransactionService private (
    transactionsService: IndexTransactionsService,
    parallelism: Int = 4)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    esf: ExecutionSequencerFactory)
    extends TransactionService
    with ErrorFactories {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  override def getTransactions(request: GetTransactionsRequest): Source[Transaction, NotUsed] = {
    val subscriptionId = subscriptionIdCounter.incrementAndGet().toString
    logger.debug(
      "Received request for transaction subscription {}: {}",
      subscriptionId: Any,
      request)

    transactionsService
      .transactions(request.begin, request.end, request.filter)

  }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest): Source[TransactionTree, NotUsed] = {
    logger.debug("Received {}", request)
    transactionsService
      .transactionTrees(
        request.begin,
        request.end,
        TransactionFilter(request.parties.map(p => p -> Filters.noFilter).toMap)
      )
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[TransactionTree] = {
    logger.debug("Received {}", request)
    SandboxEventIdFormatter
      .split(request.eventId.unwrap)
      .fold(
        Future.failed[TransactionTree](
          Status.INVALID_ARGUMENT
            .withDescription(s"invalid eventId: ${request.eventId}")
            .asRuntimeException())) {
        case TransactionIdWithIndex(transactionId, index) =>
          lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)
      }
  }

  override def getTransactionById(request: GetTransactionByIdRequest): Future[TransactionTree] = {
    logger.debug("Received {}", request)
    lookUpTreeByTransactionId(request.transactionId, request.requestingParties)
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[Transaction] =
    SandboxEventIdFormatter
      .split(request.eventId.unwrap)
      .fold(
        Future.failed[Transaction](
          Status.INVALID_ARGUMENT
            .withDescription(s"invalid eventId: ${request.eventId}")
            .asRuntimeException())) {
        case TransactionIdWithIndex(transactionId, _) =>
          lookUpFlatByTransactionId(TransactionId(transactionId), request.requestingParties)
      }

  override def getFlatTransactionById(request: GetTransactionByIdRequest): Future[Transaction] =
    lookUpFlatByTransactionId(request.transactionId, request.requestingParties)

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    transactionsService.currentLedgerEnd()

  override lazy val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by(abs => BigInt(abs.value))

  private def lookUpTreeByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party]): Future[TransactionTree] =
    transactionsService
      .getTransactionTreeById(transactionId, requestingParties)
      .flatMap {
        case Some(trans) =>
          Future.successful(trans)

        case None =>
          Future.failed(
            Status.INVALID_ARGUMENT
              .withDescription("Transaction not found, or not visible.")
              .asRuntimeException())
      }

  private def lookUpFlatByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party]): Future[Transaction] =
    transactionsService
      .getTransactionById(transactionId, requestingParties)
      .flatMap {
        case Some(trans) =>
          Future.successful(trans)

        case None =>
          Future.failed(
            Status.INVALID_ARGUMENT
              .withDescription("Transaction not found, or not visible.")
              .asRuntimeException())
      }

}
