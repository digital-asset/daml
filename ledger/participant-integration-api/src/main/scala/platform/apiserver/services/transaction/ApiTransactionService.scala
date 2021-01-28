// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.daml.ledger.participant.state.index.v2.IndexTransactionsService
import com.daml.lf.data.Ref.Party
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain._
import com.daml.ledger.api.messages.transaction._
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.logging
import com.daml.ledger
import com.daml.platform.server.api.services.domain.TransactionService
import com.daml.platform.server.api.services.grpc.GrpcTransactionService
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object ApiTransactionService {

  def create(
      ledgerId: LedgerId,
      transactionsService: IndexTransactionsService,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      loggingContext: LoggingContext,
  ): GrpcTransactionService with BindableService =
    new GrpcTransactionService(
      new ApiTransactionService(transactionsService),
      ledgerId,
      PartyNameChecker.AllowAllParties,
    )
}

private[apiserver] final class ApiTransactionService private (
    transactionsService: IndexTransactionsService
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends TransactionService
    with ErrorFactories {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def getTransactions(
      request: GetTransactionsRequest
  ): Source[GetTransactionsResponse, NotUsed] =
    withEnrichedLoggingContext(
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.filter.filtersByParty.keys),
    ) { implicit loggingContext =>
      val subscriptionId = subscriptionIdCounter.incrementAndGet().toString
      logger.debug(s"Received request for transaction subscription $subscriptionId: $request")
      transactionsService
        .transactions(request.startExclusive, request.endInclusive, request.filter, request.verbose)
        .via(logFlowingTransactions)
        .via(logger.logErrorsOnStream)
    }

  private def logFlowingTransactions(implicit
      loggingContext: LoggingContext
  ): Flow[GetTransactionsResponse, GetTransactionsResponse, NotUsed] =
    Flow[GetTransactionsResponse].map { response =>
      val transactionIds = response.transactions.map(_.transactionId)
      logger.debug(s"Responding with transactions: ${transactionIds.mkString("[", ", ", "]")}")
      response
    }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest
  ): Source[GetTransactionTreesResponse, NotUsed] =
    withEnrichedLoggingContext(
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.parties),
    ) { implicit loggingContext =>
      logger.debug(s"Received $request")
      transactionsService
        .transactionTrees(
          request.startExclusive,
          request.endInclusive,
          TransactionFilter(request.parties.map(p => p -> Filters.noFilter).toMap),
          request.verbose,
        )
        .via(logFlowingTransactionTrees)
        .via(logger.logErrorsOnStream)
    }

  private def logFlowingTransactionTrees(implicit
      loggingContext: LoggingContext
  ): Flow[GetTransactionTreesResponse, GetTransactionTreesResponse, NotUsed] =
    Flow[GetTransactionTreesResponse].map { response =>
      val transactionIds = response.transactions.map(_.transactionId)
      logger.debug(s"Responding with transaction trees: ${transactionIds.mkString("[", ", ", "]")}")
      response
    }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] =
    withEnrichedLoggingContext(
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.debug(s"Received $request")
      ledger.EventId
        .fromString(request.eventId.unwrap)
        .map { case ledger.EventId(transactionId, _) =>
          lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)
        }
        .getOrElse(
          Future.failed(
            Status.NOT_FOUND
              .withDescription(s"invalid eventId: ${request.eventId}")
              .asRuntimeException()
          )
        )
        .andThen(logger.logErrorsOnCall[GetTransactionResponse])
    }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] =
    withEnrichedLoggingContext(
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.debug(s"Received $request")
      lookUpTreeByTransactionId(request.transactionId, request.requestingParties)
        .andThen(logger.logErrorsOnCall[GetTransactionResponse])
    }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] =
    withEnrichedLoggingContext(
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      ledger.EventId
        .fromString(request.eventId.unwrap)
        .fold(
          err =>
            Future.failed[GetFlatTransactionResponse](
              Status.NOT_FOUND.withDescription(s"invalid eventId: $err").asRuntimeException()
            ),
          eventId =>
            lookUpFlatByTransactionId(
              TransactionId(eventId.transactionId),
              request.requestingParties,
            ),
        )
        .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
    }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] =
    withEnrichedLoggingContext(
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      lookUpFlatByTransactionId(request.transactionId, request.requestingParties)
        .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
    }

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    transactionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

  override lazy val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by[LedgerOffset.Absolute, String](_.value)

  private def lookUpTreeByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[GetTransactionResponse] =
    transactionsService
      .getTransactionTreeById(transactionId, requestingParties)
      .map(getOrElseThrowNotFound)

  private def lookUpFlatByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[GetFlatTransactionResponse] =
    transactionsService
      .getTransactionById(transactionId, requestingParties)
      .map(getOrElseThrowNotFound)

  @throws[StatusRuntimeException]
  private def getOrElseThrowNotFound[A](a: Option[A]): A =
    a.getOrElse(
      throw Status.NOT_FOUND
        .withDescription("Transaction not found, or not visible.")
        .asRuntimeException()
    )

}
