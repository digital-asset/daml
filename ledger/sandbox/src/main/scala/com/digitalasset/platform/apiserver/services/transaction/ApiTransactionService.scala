// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services.transaction

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
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.sandbox.EventIdFormatter
import com.digitalasset.platform.sandbox.EventIdFormatter.TransactionIdWithIndex
import com.digitalasset.platform.server.api.services.domain.TransactionService
import com.digitalasset.platform.server.api.services.grpc.GrpcTransactionService
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

object ApiTransactionService {

  def create(
      ledgerId: LedgerId,
      transactionsService: IndexTransactionsService,
  )(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      logCtx: LoggingContext): GrpcTransactionService with BindableService =
    new GrpcTransactionService(
      new ApiTransactionService(transactionsService),
      ledgerId,
      PartyNameChecker.AllowAllParties
    )
}

final class ApiTransactionService private (
    transactionsService: IndexTransactionsService,
    parallelism: Int = 4)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    esf: ExecutionSequencerFactory,
    logCtx: LoggingContext)
    extends TransactionService
    with ErrorFactories {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  override def getTransactions(request: GetTransactionsRequest): Source[Transaction, NotUsed] = {
    val subscriptionId = subscriptionIdCounter.incrementAndGet().toString
    logger.debug(s"Received request for transaction subscription $subscriptionId: $request")
    transactionsService
      .transactions(request.begin, request.end, request.filter)
      .via(logger.logErrorsOnStream)
  }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest): Source[TransactionTree, NotUsed] = {
    logger.debug(s"Received $request")
    transactionsService
      .transactionTrees(
        request.begin,
        request.end,
        TransactionFilter(request.parties.map(p => p -> Filters.noFilter).toMap)
      )
      .via(logger.logErrorsOnStream)
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[TransactionTree] = {
    logger.debug(s"Received $request")
    EventIdFormatter
      .split(request.eventId.unwrap)
      .fold(
        Future.failed[TransactionTree](
          Status.NOT_FOUND
            .withDescription(s"invalid eventId: ${request.eventId}")
            .asRuntimeException())) {
        case TransactionIdWithIndex(transactionId, _) =>
          lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)
      }
      .andThen(logger.logErrorsOnCall[TransactionTree])
  }

  override def getTransactionById(request: GetTransactionByIdRequest): Future[TransactionTree] = {
    logger.debug(s"Received $request")
    lookUpTreeByTransactionId(request.transactionId, request.requestingParties)
      .andThen(logger.logErrorsOnCall[TransactionTree])
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[Transaction] =
    EventIdFormatter
      .split(request.eventId.unwrap)
      .fold(
        Future.failed[Transaction](
          Status.NOT_FOUND
            .withDescription(s"invalid eventId: ${request.eventId}")
            .asRuntimeException())) {
        case TransactionIdWithIndex(transactionId, _) =>
          lookUpFlatByTransactionId(TransactionId(transactionId), request.requestingParties)
      }
      .andThen(logger.logErrorsOnCall[Transaction])

  override def getFlatTransactionById(request: GetTransactionByIdRequest): Future[Transaction] =
    lookUpFlatByTransactionId(request.transactionId, request.requestingParties)
      .andThen(logger.logErrorsOnCall[Transaction])

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    transactionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

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
            Status.NOT_FOUND
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
            Status.NOT_FOUND
              .withDescription("Transaction not found, or not visible.")
              .asRuntimeException())
      }

}
