// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.IndexTransactionsService
import com.daml.lf.data.Ref.Party
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain._
import com.daml.ledger.api.messages.transaction._
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext, ThreadLogger}
import com.daml.platform.apiserver.services.logging
import com.daml.ledger
import com.daml.platform.server.api.services.domain.TransactionService
import com.daml.platform.server.api.services.grpc.GrpcTransactionService
import com.daml.platform.server.api.validation.ErrorFactories
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

  override def getTransactions(
      request: GetTransactionsRequest): Source[GetTransactionsResponse, NotUsed] = {
    ThreadLogger.traceThread("ApiTransactionService.getTransactions")
    withEnrichedLoggingContext(
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.filter.filtersByParty.keys),
    ) { implicit logCtx =>
      val subscriptionId = subscriptionIdCounter.incrementAndGet().toString
      logger.debug(s"Received request for transaction subscription $subscriptionId: $request")
      transactionsService
        .transactions(request.startExclusive, request.endInclusive, request.filter, request.verbose)
        .via(logger.logErrorsOnStream)
    }
  }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest): Source[GetTransactionTreesResponse, NotUsed] = {
    ThreadLogger.traceThread("ApiTransactionService.getTransactionTrees")
    withEnrichedLoggingContext(
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.parties),
    ) { implicit logCtx =>
      logger.debug(s"Received $request")
      transactionsService
        .transactionTrees(
          request.startExclusive,
          request.endInclusive,
          TransactionFilter(request.parties.map(p => p -> Filters.noFilter).toMap),
          request.verbose
        )
        .via(logger.logErrorsOnStream)
    }
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetTransactionResponse] = {
    ThreadLogger.traceThread("ApiTransactionService.getTransactionByEventId")
    withEnrichedLoggingContext(
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit logCtx =>
      logger.debug(s"Received $request")
      ledger.EventId
        .fromString(request.eventId.unwrap)
        .map {
          case ledger.EventId(transactionId, _) =>
            lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)
        }
        .getOrElse(
          Future.failed(
            Status.NOT_FOUND
              .withDescription(s"invalid eventId: ${request.eventId}")
              .asRuntimeException()))
        .andThen(logger.logErrorsOnCall[GetTransactionResponse])
    }
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest): Future[GetTransactionResponse] =
    withEnrichedLoggingContext(
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit logCtx =>
      logger.debug(s"Received $request")
      lookUpTreeByTransactionId(request.transactionId, request.requestingParties)
        .andThen(logger.logErrorsOnCall[GetTransactionResponse])
    }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetFlatTransactionResponse] = {
    ThreadLogger.traceThread("ApiTransactionService.getFlatTransactionByEventId")
    withEnrichedLoggingContext(
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit logCtx =>
      ledger.EventId
        .fromString(request.eventId.unwrap)
        .fold(
          err =>
            Future.failed[GetFlatTransactionResponse](
              Status.NOT_FOUND.withDescription(s"invalid eventId: $err").asRuntimeException()),
          eventId =>
            lookUpFlatByTransactionId(
              TransactionId(eventId.transactionId),
              request.requestingParties)
        )
        .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
    }
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest): Future[GetFlatTransactionResponse] = {
    ThreadLogger.traceThread("ApiTransactionService.getFlatTransactionById")
    withEnrichedLoggingContext(
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties)) { implicit logCtx =>
      lookUpFlatByTransactionId(request.transactionId, request.requestingParties)
        .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
    }
  }

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] = {
    ThreadLogger.traceThread("ApiTransactionService.getLedgerEnd")
    transactionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])
  }

  override lazy val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by[LedgerOffset.Absolute, String](_.value)

  private def lookUpTreeByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party]): Future[GetTransactionResponse] =
    transactionsService
      .getTransactionTreeById(transactionId, requestingParties)
      .map(getOrElseThrowNotFound)

  private def lookUpFlatByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party]): Future[GetFlatTransactionResponse] =
    transactionsService
      .getTransactionById(transactionId, requestingParties)
      .map(getOrElseThrowNotFound)

  @throws[StatusRuntimeException]
  private def getOrElseThrowNotFound[A](a: Option[A]): A =
    a.getOrElse(
      throw Status.NOT_FOUND
        .withDescription("Transaction not found, or not visible.")
        .asRuntimeException())

}
