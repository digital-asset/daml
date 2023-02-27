// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service._
import com.daml.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.daml.ledger.api.validation.{PartyNameChecker, TransactionServiceRequestValidator}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.TransactionService
import com.daml.platform.server.api.services.grpc.Logging.traceId
import com.daml.tracing.Telemetry
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

final class GrpcTransactionService(
    protected val service: TransactionService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    telemetry: Telemetry,
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends TransactionServiceGrpc.TransactionService
    with StreamingServiceLifecycleManagement
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val validator =
    new TransactionServiceRequestValidator(ledgerId, partyNameChecker)

  def getTransactions(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit = registerStream(responseObserver) {
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        logger.debug(s"Received new transaction request $request")
        Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
          val validation = validator.validate(request, ledgerEnd)

          validation.fold(
            t => Source.failed(ValidationLogger.logFailure(request, t)),
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
  ): Unit = registerStream(responseObserver) {
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        logger.debug(s"Received new transaction tree request $request")
        Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
          val validation = validator.validateTree(request, ledgerEnd)

          validation.fold(
            t => Source.failed(ValidationLogger.logFailure(request, t)),
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
  ): Future[Response] =
    validate(request).fold(t => Future.failed(ValidationLogger.logFailure(request, t)), fetch(_))

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        getSingleTransaction(
          request,
          validator.validateTransactionByEventId,
          service.getTransactionByEventId,
        )
    }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        getSingleTransaction(
          request,
          validator.validateTransactionById,
          service.getTransactionById,
        )
    }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        getSingleTransaction(
          request,
          validator.validateTransactionByEventId,
          service.getFlatTransactionByEventId,
        )
    }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        getSingleTransaction(
          request,
          validator.validateTransactionById,
          service.getFlatTransactionById,
        )
    }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    val validation = validator.validateLedgerEnd(request)

    validation.fold(
      t => Future.failed(ValidationLogger.logFailure(request, t)),
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
  ): Future[GetLatestPrunedOffsetsResponse] = service.getLatestPrunedOffsets
}
