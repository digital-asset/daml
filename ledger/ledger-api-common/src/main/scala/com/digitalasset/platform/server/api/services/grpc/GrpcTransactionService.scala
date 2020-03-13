// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.{
  TransactionService => ApiTransactionService
}
import com.digitalasset.ledger.api.v1.transaction_service._
import com.digitalasset.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.digitalasset.ledger.api.validation.{PartyNameChecker, TransactionServiceRequestValidator}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.services.domain.TransactionService
import com.digitalasset.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GrpcTransactionService(
    protected val service: TransactionService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer)
    extends TransactionServiceAkkaGrpc
    with GrpcApiService
    with ErrorFactories
    with FieldValidations {

  protected val logger: Logger = LoggerFactory.getLogger(ApiTransactionService.getClass)

  private type MapStringSet[T] = Map[String, Set[T]]

  private val validator =
    new TransactionServiceRequestValidator(ledgerId, partyNameChecker)

  override protected def getTransactionsSource(
      request: GetTransactionsRequest): Source[GetTransactionsResponse, NotUsed] = {
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
          else service.getTransactions(req)
      )
    }
  }

  override protected def getTransactionTreesSource(
      request: GetTransactionsRequest): Source[GetTransactionTreesResponse, NotUsed] = {
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
        }
      )
    }
  }

  private def getSingleTransaction[Request, DomainRequest, DomainTx, Response](
      req: Request,
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response]): Future[Response] =
    validate(req).fold(Future.failed, fetch(_))

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getTransactionByEventId
    )
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest): Future[GetTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getTransactionById
    )
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetFlatTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getFlatTransactionByEventId
    )
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest): Future[GetFlatTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getFlatTransactionById
    )
  }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    val validation = validator.validateLedgerEnd(request)

    validation.fold(
      Future.failed,
      v =>
        service
          .getLedgerEnd(request.ledgerId)
          .map(abs =>
            GetLedgerEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value)))))(
            DirectExecutionContext)
    )
  }

  override def bindService(): ServerServiceDefinition =
    TransactionServiceGrpc.bindService(this, DirectExecutionContext)

}
