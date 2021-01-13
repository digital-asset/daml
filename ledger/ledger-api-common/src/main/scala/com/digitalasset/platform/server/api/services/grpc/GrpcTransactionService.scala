// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.{
  TransactionService => ApiTransactionService
}
import com.daml.ledger.api.v1.transaction_service._
import com.daml.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.daml.ledger.api.validation.{
  LedgerOffsetValidator,
  PartyNameChecker,
  TransactionServiceRequestValidator,
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

  private val validator =
    new TransactionServiceRequestValidator(ledgerId, partyNameChecker)

  override protected def getTransactionsSource(
      request: GetTransactionsRequest
  ): Source[GetTransactionsResponse, NotUsed] = {
    logger.debug("Received new transaction request {}", request)
    Source.future(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
      val validation = validator.validate(request, ledgerEnd, service.offsetOrdering)

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
      val validation = validator.validateTree(request, ledgerEnd, service.offsetOrdering)

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
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response],
      fetchLedgerEnd: String => Future[domain.LedgerOffset.Absolute],
      offsetFromResponse: Response => LedgerString,
      ledgerIdFromRequest: Request => String,
  ): Future[Response] =
    for {
      response <- validate(req).fold(Future.failed, fetch(_))
      ledgerEnd <- fetchLedgerEnd(ledgerIdFromRequest(req))
      offset = offsetFromResponse(response)
      ledgerOffser = domain.LedgerOffset.Absolute(offset)
      validatedResponse <- LedgerOffsetValidator
        .offsetIsBeforeEndIfAbsolute(
          "End",
          ledgerOffser,
          ledgerEnd,
          service.offsetOrdering,
        )
        .fold(Future.failed, _ => Future.successful(response))
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
      validator.validateTransactionByEventId,
      service.getTransactionByEventId,
      service.getLedgerEnd,
      r =>
        Ref.IdString.LedgerString
          .assertFromString(r.transaction.getOrElse(transactionNotFound()).offset),
      _.ledgerId,
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
      validator.validateTransactionById,
      service.getTransactionById,
      service.getLedgerEnd,
      r =>
        Ref.IdString.LedgerString
          .assertFromString(r.transaction.getOrElse(transactionNotFound()).offset),
      _.ledgerId,
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
      validator.validateTransactionByEventId,
      service.getFlatTransactionByEventId,
      service.getLedgerEnd,
      r =>
        Ref.IdString.LedgerString
          .assertFromString(r.transaction.getOrElse(transactionNotFound()).offset),
      _.ledgerId,
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
      validator.validateTransactionById,
      service.getFlatTransactionById,
      service.getLedgerEnd,
      r =>
        Ref.IdString.LedgerString
          .assertFromString(r.transaction.getOrElse(transactionNotFound()).offset),
      _.ledgerId,
    )

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    validator
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
