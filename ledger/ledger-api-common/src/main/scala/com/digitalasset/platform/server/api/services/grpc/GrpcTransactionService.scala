// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service._
import com.daml.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.daml.ledger.api.validation.{PartyNameChecker, TransactionServiceRequestValidator}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.TransactionService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class GrpcTransactionService(
    protected val service: TransactionService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    protected val optimizeGrpcStreamsThroughput: Boolean,
)(implicit
    protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends TransactionServiceAkkaGrpc
    with GrpcApiService {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val validator =
    new TransactionServiceRequestValidator(ledgerId, partyNameChecker)

  override protected def getTransactionsSource(
      request: GetTransactionsRequest
  ): Source[GetTransactionsResponse, NotUsed] = {
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

  override protected def getTransactionTreesSource(
      request: GetTransactionsRequest
  ): Source[GetTransactionTreesResponse, NotUsed] = {
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

  private def getSingleTransaction[Request, DomainRequest, DomainTx, Response](
      request: Request,
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response],
  ): Future[Response] =
    validate(request).fold(t => Future.failed(ValidationLogger.logFailure(request, t)), fetch(_))

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getTransactionByEventId,
    )
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getTransactionById,
    )
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getFlatTransactionByEventId,
    )
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] = {
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

}
