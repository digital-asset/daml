// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.{
  TransactionService => ApiTransactionService
}
import com.daml.ledger.api.v1.transaction_service._
import com.daml.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.daml.ledger.api.validation.{
  PartyNameChecker,
  TransactionServiceRequestValidator,
  TransactionServiceResponseValidator,
}
import com.daml.lf.data.Ref.LedgerString
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.services.domain.TransactionService
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.{ServerServiceDefinition, Status}
import org.slf4j.{Logger, LoggerFactory}
import com.daml.ledger.api.messages.{transaction => domainRequests}
import com.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

final class GrpcTransactionService(
    protected val service: TransactionService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
)(implicit
    protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer,
    executionContext: ExecutionContext,
) extends TransactionServiceAkkaGrpc
    with GrpcApiService
    with ErrorFactories
    with FieldValidations {

  protected val logger: Logger = LoggerFactory.getLogger(ApiTransactionService.getClass)

  private val requestValidator =
    new TransactionServiceRequestValidator(ledgerId, partyNameChecker)

  private val responseValidator =
    new TransactionServiceResponseValidator(ledgerId, service.getLedgerEnd)

  override protected def getTransactionsSource(
      request: GetTransactionsRequest
  ): Source[GetTransactionsResponse, NotUsed] = {
    logger.debug("Received new transaction request {}", request)
    Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
      val validation = requestValidator.validate(request, ledgerEnd, service.offsetOrdering)

      validation.fold(
        { t =>
          logger.debug("Request validation failed for {}. Message: {}", request: Any, t.getMessage)
          Source.failed(t)
        },
        req =>
          if (req.filter.filtersByParty.isEmpty) Source.empty
          else service.getTransactions(req),
      )
    }
  }

  override protected def getTransactionTreesSource(
      request: GetTransactionsRequest
  ): Source[GetTransactionTreesResponse, NotUsed] = {
    logger.debug("Received new transaction tree request {}", request)
    Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
      val validation = requestValidator.validateTree(request, ledgerEnd, service.offsetOrdering)

      validation.fold(
        { t =>
          logger.debug("Request validation failed for {}. Message: {}", request: Any, t.getMessage)
          Source.failed(t)
        },
        req => {
          if (req.parties.isEmpty) Source.empty
          else service.getTransactionTrees(req)
        },
      )
    }
  }

  private def getSingleTransaction[Request, DomainRequest, Response](
      req: Request,
      validateRequest: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response],
      offsetFromResponse: Response => LedgerString,
  ): Future[Response] =
    for {
      response <- validateRequest(req).fold(Future.failed, fetch(_))
      validatedResponse <- responseValidator.validate(
        response,
        offsetFromResponse,
        service.offsetOrdering,
      )
    } yield validatedResponse

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    getSingleTransaction[
      GetTransactionByEventIdRequest,
      domainRequests.GetTransactionByEventIdRequest,
      GetTransactionResponse,
    ](
      request,
      requestValidator.validateTransactionByEventId,
      service.getTransactionByEventId,
      _.transaction
        .map(tx => Ref.IdString.LedgerString.assertFromString(tx.offset))
        .getOrElse(transactionNotFound()),
    )
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] =
    getSingleTransaction[
      GetTransactionByIdRequest,
      domainRequests.GetTransactionByIdRequest,
      GetTransactionResponse,
    ](
      request,
      requestValidator.validateTransactionById,
      service.getTransactionById,
      _.transaction
        .map(tx => Ref.IdString.LedgerString.assertFromString(tx.offset))
        .getOrElse(transactionNotFound()),
    )

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] =
    getSingleTransaction[
      GetTransactionByEventIdRequest,
      domainRequests.GetTransactionByEventIdRequest,
      GetFlatTransactionResponse,
    ](
      request,
      requestValidator.validateTransactionByEventId,
      service.getFlatTransactionByEventId,
      _.transaction
        .map(tx => Ref.IdString.LedgerString.assertFromString(tx.offset))
        .getOrElse(transactionNotFound()),
    )

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] =
    getSingleTransaction[
      GetTransactionByIdRequest,
      domainRequests.GetTransactionByIdRequest,
      GetFlatTransactionResponse,
    ](
      request,
      requestValidator.validateTransactionById,
      service.getFlatTransactionById,
      _.transaction
        .map(tx => Ref.IdString.LedgerString.assertFromString(tx.offset))
        .getOrElse(transactionNotFound()),
    )

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    requestValidator
      .validateLedgerEnd(request)
      .fold(
        Future.failed,
        _ =>
          service
            .getLedgerEnd(request.ledgerId)
            .map(abs =>
              GetLedgerEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value))))
            ),
      )

  override def bindService(): ServerServiceDefinition =
    TransactionServiceGrpc.bindService(this, executionContext)

  private def transactionNotFound(): Nothing =
    throw Status.NOT_FOUND
      .withDescription("Transaction not found, or not visible.")
      .asRuntimeException()
}
