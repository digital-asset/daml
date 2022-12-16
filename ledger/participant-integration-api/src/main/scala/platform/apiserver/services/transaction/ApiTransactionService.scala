// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
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
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.ledger.api.validation.ValidationErrors.invalidArgument
import com.daml.ledger.participant.state.index.v2.IndexTransactionsService
import com.daml.lf.data.Ref.Party
import com.daml.lf.ledger.{EventId => LfEventId}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.{LoggingEntries, LoggingValue}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.services.{StreamMetrics, logging}
import com.daml.platform.server.api.services.domain.TransactionService
import com.daml.platform.server.api.services.grpc.GrpcTransactionService
import com.daml.tracing.Telemetry
import io.grpc._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object ApiTransactionService {
  def create(
      ledgerId: LedgerId,
      transactionsService: IndexTransactionsService,
      metrics: Metrics,
      telemetry: Telemetry,
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
      telemetry,
    )
}

private[apiserver] final class ApiTransactionService private (
    transactionsService: IndexTransactionsService,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends TransactionService {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute] =
    transactionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

  override def getTransactions(
      request: GetTransactionsRequest
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] = {
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
      .via(logger.enrichedDebugStream("Responding with transactions.", transactionsLoggable))
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.transactions))
  }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed] = {
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
      .via(
        logger.enrichedDebugStream("Responding with transaction trees.", transactionTreesLoggable)
      )
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.transactionTrees))
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetTransactionResponse] = {
    implicit val enrichedLoggingContext: LoggingContext = LoggingContext.enriched(
      logging.ledgerId(request.ledgerId),
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    )(loggingContext)
    logger.info("Received request for transaction by event ID.")(enrichedLoggingContext)
    implicit val errorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, enrichedLoggingContext, None)
    logger.trace(s"Transaction by event ID request: $request")(loggingContext)
    LfEventId
      .fromString(request.eventId.unwrap)
      .map { case LfEventId(transactionId, _) =>
        lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)
      }
      .getOrElse {
        Future.failed {
          invalidArgument(s"invalid eventId: ${request.eventId}")
        }
      }
      .andThen(logger.logErrorsOnCall[GetTransactionResponse](loggingContext))
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetTransactionResponse] = {
    val errorLogger: DamlContextualizedErrorLogger = withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for transaction by ID.")
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    }
    logger.trace(s"Transaction by ID request: $request")

    lookUpTreeByTransactionId(request.transactionId, request.requestingParties)(errorLogger)
      .andThen(logger.logErrorsOnCall[GetTransactionResponse])
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetFlatTransactionResponse] = {
    implicit val errorLogger: DamlContextualizedErrorLogger = withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for flat transaction by event ID.")
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    }
    logger.trace(s"Flat transaction by event ID request: $request")

    LfEventId
      .fromString(request.eventId.unwrap)
      .map { case LfEventId(transactionId, _) =>
        lookUpFlatByTransactionId(TransactionId(transactionId), request.requestingParties)
      }
      .getOrElse {
        val msg = s"eventId: ${request.eventId}"
        Future.failed(invalidArgument(msg))
      }
      .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetFlatTransactionResponse] = {
    val errorLogger = withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for flat transaction by ID.")
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    }
    logger.trace(s"Flat transaction by ID request: $request")

    lookUpFlatByTransactionId(request.transactionId, request.requestingParties)(errorLogger)
      .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
  }

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = {

    withEnrichedLoggingContext(
      logging.contractId(request.contractId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info("Received request for events by contract ID")
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    }
    logger.trace(s"Events by contract ID request: $request")

    transactionsService
      .getEventsByContractId(
        request.contractId,
        request.requestingParties,
      )
      .andThen(logger.logErrorsOnCall[GetEventsByContractIdResponse])
  }

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse] = {

    withEnrichedLoggingContext(
      logging.contractKey(request.contractKey),
      logging.templateId(request.templateId),
      logging.parties(request.requestingParties),
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(Some(request.endInclusive)),
    ) { implicit loggingContext =>
      logger.info("Received request for events by contract key")
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    }
    logger.trace(s"Events by contract key request: $request")

    transactionsService
      .getEventsByContractKey(
        request.contractKey,
        request.templateId,
        request.requestingParties,
        request.maxEvents,
        request.startExclusive,
        request.endInclusive,
      )
      .andThen(logger.logErrorsOnCall[GetEventsByContractKeyResponse])
  }

  private def lookUpTreeByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit errorLogger: ContextualizedErrorLogger): Future[GetTransactionResponse] =
    transactionsService
      .getTransactionTreeById(transactionId, requestingParties)
      .flatMap {
        case None =>
          Future.failed(
            LedgerApiErrors.RequestValidation.NotFound.Transaction
              .Reject(transactionId.unwrap)
              .asGrpcError
          )
        case Some(transaction) => Future.successful(transaction)
      }

  private def lookUpFlatByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit errorLogger: ContextualizedErrorLogger): Future[GetFlatTransactionResponse] =
    transactionsService
      .getTransactionById(transactionId, requestingParties)
      .flatMap {
        case None =>
          Future.failed(
            LedgerApiErrors.RequestValidation.NotFound.Transaction
              .Reject(transactionId.unwrap)
              .asGrpcError
          )
        case Some(transaction) => Future.successful(transaction)
      }

  private def transactionTreesLoggable(trees: GetTransactionTreesResponse): LoggingEntries =
    LoggingEntries(
      "transactions" -> LoggingValue.OfIterable(
        trees.transactions.toList.map(t =>
          entityLoggable(t.commandId, t.transactionId, t.workflowId, t.offset)
        )
      )
    )

  private def transactionsLoggable(trees: GetTransactionsResponse): LoggingEntries = LoggingEntries(
    "transactions" -> LoggingValue.OfIterable(
      trees.transactions.toList.map(t =>
        entityLoggable(t.commandId, t.transactionId, t.workflowId, t.offset)
      )
    )
  )

  private def entityLoggable(
      commandId: String,
      transactionId: String,
      workflowId: String,
      offset: String,
  ): LoggingValue.Nested =
    LoggingValue.Nested.fromEntries(
      logging.commandId(commandId),
      logging.transactionId(transactionId),
      logging.workflowId(workflowId),
      logging.offset(offset),
    )
}
