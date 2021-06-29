// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger
import com.daml.ledger.api.domain._
import com.daml.ledger.api.messages.transaction._
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.ledger.participant.state.index.v2.IndexTransactionsService
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext, LoggingEntries}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.services.transaction.ApiTransactionService._
import com.daml.platform.apiserver.services.{StreamMetrics, logging}
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
      metrics: Metrics,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      loggingContext: LoggingContext,
  ): GrpcTransactionService with BindableService =
    new GrpcTransactionService(
      new ApiTransactionService(transactionsService, metrics),
      ledgerId,
      PartyNameChecker.AllowAllParties,
    )

  @throws[StatusRuntimeException]
  private def getOrElseThrowNotFound[A](a: Option[A]): A =
    a.getOrElse(
      throw Status.NOT_FOUND
        .withDescription("Transaction not found, or not visible.")
        .asRuntimeException()
    )
}

private[apiserver] final class ApiTransactionService private (
    transactionsService: IndexTransactionsService,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends TransactionService
    with ErrorFactories {
  private val logger = ContextualizedLogger.get(this.getClass)

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    transactionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

  override def getTransactions(
      request: GetTransactionsRequest
  ): Source[GetTransactionsResponse, NotUsed] =
    withEnrichedLoggingContext(
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.filter.filtersByParty.keys),
    ) { implicit loggingContext =>
      logger.info("Received request for transactions.")
      logger.trace(s"Transaction request: $request")
      transactionsService
        .transactions(request.startExclusive, request.endInclusive, request.filter, request.verbose)
        .via(logger.debugStream(transactionsLoggable))
        .via(logger.logErrorsOnStream)
        .via(StreamMetrics.countElements(metrics.daml.lapi.streams.transactions))
    }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest
  ): Source[GetTransactionTreesResponse, NotUsed] = {
    withEnrichedLoggingContext(
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.parties),
    ) { implicit loggingContext =>
      logger.info("Received request for transaction trees.")
      logger.trace(s"Transaction tree request: $request")
      transactionsService
        .transactionTrees(
          request.startExclusive,
          request.endInclusive,
          TransactionFilter(request.parties.map(p => p -> Filters.noFilter).toMap),
          request.verbose,
        )
        .via(logger.debugStream(transactionTreesLoggable))
        .via(logger.logErrorsOnStream)
        .via(StreamMetrics.countElements(metrics.daml.lapi.streams.transactionTrees))
    }
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] =
    withEnrichedLoggingContext(
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for transaction by event ID.")
      logger.trace(s"Transaction by event ID: $request")
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
      logger.info("Received request for transaction by ID.")
      logger.trace(s"Transaction by ID: $request")
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
      logger.info("Received request for flat transaction by event ID.")
      logger.trace(s"Flat transaction by event ID: $request")
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
      logger.info("Received request for flat transaction by ID.")
      logger.trace(s"Flat transaction by ID: $request")
      lookUpFlatByTransactionId(request.transactionId, request.requestingParties)
        .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
    }

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

  private def transactionsLoggable(transactions: GetTransactionsResponse): String =
    s"Responding with transactions: ${transactions.transactions.toList
      .map(t => entityLoggable(t.commandId, t.transactionId, t.workflowId, t.offset))}"

  private def transactionTreesLoggable(trees: GetTransactionTreesResponse): String =
    s"Responding with transaction trees: ${trees.transactions.toList
      .map(t => entityLoggable(t.commandId, t.transactionId, t.workflowId, t.offset))}"

  private def entityLoggable(
      commandId: String,
      transactionId: String,
      workflowId: String,
      offset: String,
  ): LoggingEntries =
    LoggingEntries(
      logging.commandId(commandId),
      logging.transactionId(transactionId),
      logging.workflowId(workflowId),
      "offset" -> offset,
    )
}
