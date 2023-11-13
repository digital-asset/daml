// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.services.TransactionService
import com.digitalasset.canton.ledger.api.validation.TransactionServiceRequestValidator
import com.digitalasset.canton.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

final class ApiTransactionService(
    protected val service: TransactionService,
    val ledgerId: LedgerId,
    telemetry: Telemetry,
    validator: TransactionServiceRequestValidator,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
) extends TransactionServiceGrpc.TransactionService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  def getTransactions(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

      logger.debug(s"Received new transaction request $request.")
      Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
        val validation = validator.validate(request, ledgerEnd)

        validation.fold(
          t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          req =>
            if (req.filter.filtersByParty.isEmpty) Source.empty
            else service.getTransactions(req),
        )
      }
    }
  }

  def getTransactionTrees(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionTreesResponse],
  ): Unit = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

      logger.debug(s"Received new transaction tree request $request.")
      Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
        val validation = validator.validateTree(request, ledgerEnd)

        validation.fold(
          t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          req => {
            if (req.parties.isEmpty) Source.empty
            else service.getTransactionTrees(req)
          },
        )
      }
    }
  }

  private def getSingleTransaction[Request, DomainRequest, DomainTx, Response](
      request: Request,
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response],
  ): Future[Response] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    validate(request).fold(
      t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
      fetch(_),
    )
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getTransactionByEventId,
    )
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getTransactionById,
    )
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getFlatTransactionByEventId,
    )
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getFlatTransactionById,
    )
  }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    val validation = validator.validateLedgerEnd(request)

    validation.fold(
      t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
      _ =>
        service
          .getLedgerEnd(request.ledgerId)
          .map(abs =>
            GetLedgerEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value))))
          ),
    )
  }

  override def bindService(): ServerServiceDefinition =
    TransactionServiceGrpc.bindService(this, executionContext)

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] = {
    service.getLatestPrunedOffsets()(LoggingContextWithTrace(loggerFactory, telemetry))
  }
}
