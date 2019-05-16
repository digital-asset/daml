// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.{
  TransactionService => ApiTransactionService
}
import com.digitalasset.ledger.api.v1.transaction_service._
import com.digitalasset.ledger.api.validation.{PartyNameChecker, TransactionServiceRequestValidator}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.{ApiException, ProxyCloseable}
import com.digitalasset.platform.server.api.services.domain.TransactionService
import com.digitalasset.platform.server.api.validation.{
  ErrorFactories,
  FieldValidations,
  IdentifierResolver
}
import com.digitalasset.platform.server.services.transaction.{
  TransactionConversion,
  VisibleTransaction
}
import io.grpc.{ServerServiceDefinition, Status}
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.tag._

import scala.concurrent.Future

class GrpcTransactionService(
    protected val service: TransactionService with AutoCloseable,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    identifierResolver: IdentifierResolver)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer)
    extends ApiTransactionService
    with TransactionServiceAkkaGrpc
    with ProxyCloseable
    with GrpcApiService
    with ErrorFactories
    with FieldValidations {

  protected val logger: Logger = LoggerFactory.getLogger(ApiTransactionService.getClass)

  private type MapStringSet[T] = Map[String, Set[T]]

  private val validator =
    new TransactionServiceRequestValidator(ledgerId, partyNameChecker, identifierResolver)

  override protected def getTransactionsSource(
      request: GetTransactionsRequest): Source[GetTransactionsResponse, NotUsed] = {
    logger.debug("Received new transaction request {}", request)
    Source.fromFuture(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
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

  private def optionalVisibleToApiTx(visibleTxO: Option[VisibleTransaction]) =
    visibleTxO.fold {
      throw new ApiException(
        Status.INVALID_ARGUMENT.withDescription("Transaction not found, or not visible."))
    }(v => GetTransactionResponse(Some(visibleToApiTxTree(v, "", true))))

  private def visibleToApiTxTree(
      visibleTx: VisibleTransaction,
      offset: String,
      verbose: Boolean): TransactionTree = {
    val mappedDisclosure =
      visibleTx.disclosureByNodeId

    val events = TransactionConversion
      .genToApiTransaction(
        visibleTx.transaction,
        mappedDisclosure,
        verbose
      )
    TransactionTree(
      visibleTx.meta.transactionId.unwrap,
      visibleTx.meta.commandId.fold("")(_.unwrap),
      visibleTx.meta.workflowId.unwrap,
      Some(TimestampConversion.fromInstant(visibleTx.meta.effectiveAt)),
      offset,
      Ref.LedgerString.toStringMap(events.eventsById),
      events.rootEventIds,
      visibleTx.meta.traceContext
    )
  }

  private def optionalTransactionToApiResponse(txO: Option[Transaction]) =
    txO.fold {
      throw new ApiException(
        Status.INVALID_ARGUMENT.withDescription("Transaction not found, or not visible."))
    }(t => GetFlatTransactionResponse(Some(t)))

  override protected def getTransactionTreesSource(
      request: GetTransactionsRequest): Source[GetTransactionTreesResponse, NotUsed] = {
    logger.debug("Received new transaction tree request {}", request)
    Source.fromFuture(service.getLedgerEnd(request.ledgerId)).flatMapConcat { ledgerEnd =>
      val validation = validator.validateTree(request, ledgerEnd, service.offsetOrdering)

      validation.fold(
        { t =>
          logger.debug("Request validation failed for {}. Message: {}", request: Any, t.getMessage)
          Source.failed(t)
        },
        req =>
          if (req.parties.isEmpty) Source.empty
          else {
            service
              .getTransactionTrees(req)
              .map { elem =>
                GetTransactionTreesResponse(
                  List(visibleToApiTxTree(elem.item, elem.offset.toString(), request.verbose)))
              }

        }
      )
    }
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetTransactionResponse] = {
    val validation = validator.validateTransactionByEventId(request)

    validation.fold(
      Future.failed,
      eventId =>
        service
          .getTransactionByEventId(eventId)
          .map(opt => optionalVisibleToApiTx(opt))(DirectExecutionContext))
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest): Future[GetTransactionResponse] = {
    val validation = validator.validateTransactionById(request)

    validation.fold(
      Future.failed,
      txId =>
        service
          .getTransactionById(txId)
          .map(opt => optionalVisibleToApiTx(opt))(DirectExecutionContext))
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetFlatTransactionResponse] = {
    val validation = validator.validateTransactionByEventId(request)

    validation.fold(
      Future.failed,
      txId =>
        service
          .getFlatTransactionByEventId(txId)
          .map(optionalTransactionToApiResponse)(DirectExecutionContext)
    )
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest): Future[GetFlatTransactionResponse] = {
    val validation = validator.validateTransactionById(request)

    validation.fold(
      Future.failed,
      txId =>
        service
          .getFlatTransactionById(txId)
          .map(optionalTransactionToApiResponse)(DirectExecutionContext)
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
