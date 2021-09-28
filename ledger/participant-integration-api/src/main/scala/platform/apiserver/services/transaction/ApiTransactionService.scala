// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{
  Filters,
  LedgerId,
  LedgerOffset,
  TransactionFilter,
  TransactionId,
}
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
import com.daml.lf.ledger.{EventId => LfEventId}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.LoggingEntries
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.ErrorCodesVersionSwitcher
import com.daml.platform.apiserver.error.{CorrelationId, LedgerApiErrors}
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
      errorsVersionsSwitcher: ErrorCodesVersionSwitcher,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      loggingContext: LoggingContext,
  ): GrpcTransactionService with BindableService =
    new GrpcTransactionService(
      new ApiTransactionService(transactionsService, metrics, errorsVersionsSwitcher),
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
    errorsVersionsSwitcher: ErrorCodesVersionSwitcher,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends TransactionService
    with ErrorFactories {
  private val logger = ContextualizedLogger.get(this.getClass)

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    transactionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

  override def getTransactions(
      request: GetTransactionsRequest
  ): Source[GetTransactionsResponse, NotUsed] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.filters(request.filter),
      logging.verbose(request.verbose),
    ) { implicit loggingContext =>
      logger.info("Received request for transactions.")
    }
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
      logging.ledgerId(request.ledgerId),
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.parties),
      logging.verbose(request.verbose),
    ) { implicit loggingContext =>
      logger.info("Received request for transaction trees.")
    }
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

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for transaction by event ID.")
    }
    logger.trace(s"Transaction by event ID request: $request")
    LfEventId
      .fromString(request.eventId.unwrap)
      .map { case LfEventId(transactionId, _) =>
        lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)
      }
      .getOrElse {
        val msg = s"invalid eventId: ${request.eventId}"
        Future.failed(
          errorsVersionsSwitcher.choose(
            v1 = Status.NOT_FOUND
              .withDescription(msg)
              .asRuntimeException(),
            v2 = LedgerApiErrors.CommandValidation.InvalidArgument
              .Reject(msg)(
                correlationId = CorrelationId.none,
                loggingContext = implicitly[LoggingContext],
                logger = logger,
              )
              .asGrpcErrorFromContext(
                correlationId = None,
                logger = logger,
              )(
                loggingContext = implicitly[LoggingContext]
              ),
          )
        )
      }
      .andThen(logger.logErrorsOnCall[GetTransactionResponse])
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for transaction by ID.")
    }
    logger.trace(s"Transaction by ID request: $request")
    lookUpTreeByTransactionId(request.transactionId, request.requestingParties)
      .andThen(logger.logErrorsOnCall[GetTransactionResponse])
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for flat transaction by event ID.")
    }
    logger.trace(s"Flat transaction by event ID request: $request")
    LfEventId
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
  ): Future[GetFlatTransactionResponse] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for flat transaction by ID.")
    }
    logger.trace(s"Flat transaction by ID request: $request")
    lookUpFlatByTransactionId(request.transactionId, request.requestingParties)
      .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
  }

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
      logging.offset(offset),
    )
}
