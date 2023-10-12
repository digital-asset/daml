// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.transaction

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetLatestPrunedOffsetsResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.lf.data.Ref.Party
import com.daml.lf.ledger.EventId as LfEventId
import com.daml.logging.entries.{LoggingEntries, LoggingValue}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{
  Filters,
  LedgerId,
  TransactionFilter,
  TransactionId,
}
import com.digitalasset.canton.ledger.api.messages.transaction.*
import com.digitalasset.canton.ledger.api.services.TransactionService
import com.digitalasset.canton.ledger.api.validation.TransactionServiceRequestValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexTransactionsService
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.{
  ApiConversions,
  ApiTransactionService,
  StreamMetrics,
  logging,
}
import io.grpc.*
import scalaz.syntax.tag.*

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object TransactionServiceImpl {
  def create(
      ledgerId: LedgerId,
      transactionsService: IndexTransactionsService,
      metrics: Metrics,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
      validator: TransactionServiceRequestValidator,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
  ): ApiTransactionService with BindableService =
    new ApiTransactionService(
      new TransactionServiceImpl(transactionsService, metrics, loggerFactory),
      ledgerId,
      telemetry,
      validator,
      loggerFactory,
    )
}

private[apiserver] final class TransactionServiceImpl private (
    transactionsService: IndexTransactionsService,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TransactionService
    with NamedLogging {

  override def getLedgerEnd(
      ledgerId: String
  )(implicit loggingContext: LoggingContextWithTrace): Future[domain.LedgerOffset.Absolute] =
    transactionsService
      .currentLedgerEnd()
      .andThen(logger.logErrorsOnCall[domain.LedgerOffset.Absolute])

  override def getTransactions(
      request: GetTransactionsRequest
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetTransactionsResponse, NotUsed] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.filters(request.filter),
      logging.verbose(request.verbose),
    ) { implicit loggingContext =>
      logger.info(s"Received request for transactions, ${loggingContext
          .serializeFiltered("ledgerId", "startExclusive", "endInclusive", "filters", "verbose")}.")
    }
    logger.trace(s"Transaction request: $request.")
    transactionsService
      .transactions(
        request.startExclusive,
        request.endInclusive,
        request.filter,
        request.verbose,
        false,
      )
      .mapConcat(ApiConversions.toV1)
      .via(logger.enrichedDebugStream("Responding with transactions.", transactionsLoggable))
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.transactions))
  }

  override def getTransactionTrees(
      request: GetTransactionTreesRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetTransactionTreesResponse, NotUsed] = {
    withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.startExclusive(request.startExclusive),
      logging.endInclusive(request.endInclusive),
      logging.parties(request.parties),
      logging.verbose(request.verbose),
    ) { implicit loggingContext =>
      logger.info(s"Received request for transaction trees, ${loggingContext
          .serializeFiltered("ledgerId", "startExclusive", "endInclusive", "parties", "verbose")}.")
    }
    logger.trace(s"Transaction tree request: $request")
    transactionsService
      .transactionTrees(
        request.startExclusive,
        request.endInclusive,
        TransactionFilter(request.parties.map(p => p -> Filters.noFilter).toMap),
        request.verbose,
        false,
      )
      .mapConcat(ApiConversions.toV1)
      .via(
        logger.enrichedDebugStream("Responding with transaction trees.", transactionTreesLoggable)
      )
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.transactionTrees))
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetTransactionResponse] = {
    implicit val enrichedLoggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.enriched(
        logging.ledgerId(request.ledgerId),
        logging.eventId(request.eventId),
        logging.parties(request.requestingParties),
      )(loggingContext)
    logger.info(s"Received request for transaction by event ID, ${enrichedLoggingContext
        .serializeFiltered("ledgerId", "eventId", "parties")}.")(loggingContext.traceContext)
    implicit val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext(logger, enrichedLoggingContext)
    logger.trace(s"Transaction by event ID request: $request")(loggingContext.traceContext)
    LfEventId
      .fromString(request.eventId.unwrap)
      .map { case LfEventId(transactionId, _) =>
        lookUpTreeByTransactionId(TransactionId(transactionId), request.requestingParties)(
          enrichedLoggingContext,
          errorLogger,
        )
      }
      .getOrElse {
        Future.failed {
          invalidArgument(s"invalid eventId: ${request.eventId}")
        }
      }
      .andThen(logger.logErrorsOnCall[GetTransactionResponse](loggingContext.traceContext))
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetTransactionResponse] = {
    val errorLogger: ErrorLoggingContext = withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info(
        s"Received request for transaction by ID, ${loggingContext.serializeFiltered("ledgerId", "transactionId", "parties")}."
      )
      ErrorLoggingContext(logger, loggingContext)
    }
    logger.trace(s"Transaction by ID request: $request.")

    lookUpTreeByTransactionId(request.transactionId, request.requestingParties)(
      loggingContext,
      errorLogger,
    )
      .andThen(logger.logErrorsOnCall[GetTransactionResponse])
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetFlatTransactionResponse] = {
    implicit val errorLoggingContext: ErrorLoggingContext = withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.eventId(request.eventId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info(
        s"Received request for flat transaction by event ID, ${loggingContext
            .serializeFiltered("ledgerId", "eventId", "parties")}."
      )
      ErrorLoggingContext(logger, loggingContext)
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
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetFlatTransactionResponse] = {
    val errorLogger = withEnrichedLoggingContext(
      logging.ledgerId(request.ledgerId),
      logging.transactionId(request.transactionId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info(s"Received request for flat transaction by ID, ${loggingContext
          .serializeFiltered("ledgerId", "transactionId", "parties")}.")
      ErrorLoggingContext(logger, loggingContext)
    }
    logger.trace(s"Flat transaction by ID request: $request")

    lookUpFlatByTransactionId(request.transactionId, request.requestingParties)(
      loggingContext,
      errorLogger,
    )
      .andThen(logger.logErrorsOnCall[GetFlatTransactionResponse])
  }

  private def lookUpTreeByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[GetTransactionResponse] =
    transactionsService
      .getTransactionTreeById(transactionId, requestingParties)
      .flatMap {
        case None =>
          Future.failed(
            RequestValidationErrors.NotFound.Transaction
              .Reject(transactionId.unwrap)
              .asGrpcError
          )
        case Some(transaction) => Future.successful(ApiConversions.toV1(transaction))
      }

  private def lookUpFlatByTransactionId(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[GetFlatTransactionResponse] = {
    transactionsService
      .getTransactionById(transactionId, requestingParties)
      .flatMap {
        case None =>
          Future.failed(
            RequestValidationErrors.NotFound.Transaction
              .Reject(transactionId.unwrap)
              .asGrpcError
          )
        case Some(transaction) => Future.successful(ApiConversions.toV1(transaction))
      }
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

  override def getLatestPrunedOffsets()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetLatestPrunedOffsetsResponse] =
    transactionsService.latestPrunedOffsets().map {
      case (prunedUpToInclusive, divulgencePrunedUpTo) =>
        GetLatestPrunedOffsetsResponse(
          participantPrunedUpToInclusive =
            Some(LedgerOffset(LedgerOffset.Value.Absolute(prunedUpToInclusive.value))),
          allDivulgedContractsPrunedUpToInclusive =
            Some(LedgerOffset(LedgerOffset.Value.Absolute(divulgencePrunedUpTo.value))),
        )
    }
}
