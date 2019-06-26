// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.{
  TransactionService => ApiTransactionService
}
import com.digitalasset.ledger.api.v1.transaction_service._
import com.digitalasset.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.digitalasset.ledger.api.validation.{PartyNameChecker, TransactionServiceRequestValidator}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.server.api.services.domain.TransactionService
import com.digitalasset.platform.server.api.validation.{
  ErrorFactories,
  FieldValidations,
  IdentifierResolver
}
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.Future

class GrpcTransactionService(
    protected val service: TransactionService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    identifierResolver: IdentifierResolver)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer)
    extends ApiTransactionService
    with TransactionServiceAkkaGrpc
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
          else
            service
              .getTransactions(req)
              .map(tx => GetTransactionsResponse(List(domainTxToApiFlat(tx, request.verbose))))
      )
    }
  }

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
              .map { tx =>
                GetTransactionTreesResponse(
                  List(domainTxToApiTree(tx, request.verbose))
                )
              }
        }
      )
    }
  }

  private def getSingleTransaction[Request, DomainRequest, DomainTx, Response](
      req: Request,
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[DomainTx],
      toApi: DomainTx => Response) = {
    val validation = validate(req)
    validation.fold(Future.failed, fetch(_).map(toApi(_))(DirectExecutionContext))
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getTransactionByEventId,
      (tree: domain.TransactionTree) =>
        GetTransactionResponse(Some(domainTxToApiTree(tree, verbose = true)))
    )
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest): Future[GetTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getTransactionById,
      (tree: domain.TransactionTree) =>
        GetTransactionResponse(Some(domainTxToApiTree(tree, verbose = true)))
    )
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetFlatTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionByEventId,
      service.getFlatTransactionByEventId,
      (flat: domain.Transaction) =>
        GetFlatTransactionResponse(Some(domainTxToApiFlat(flat, verbose = true)))
    )
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest): Future[GetFlatTransactionResponse] = {
    getSingleTransaction(
      request,
      validator.validateTransactionById,
      service.getFlatTransactionById,
      (flat: domain.Transaction) =>
        GetFlatTransactionResponse(Some(domainTxToApiFlat(flat, verbose = true)))
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

  def domainTxToApiFlat(tx: domain.Transaction, verbose: Boolean): Transaction =
    Transaction(
      tx.transactionId.unwrap,
      Tag.unsubst(tx.commandId).getOrElse(""),
      Tag.unsubst(tx.workflowId).getOrElse(""),
      Some(TimestampConversion.fromInstant(tx.effectiveAt)),
      tx.events.map {
        case create: domain.Event.CreatedEvent =>
          Event(Created(domainToApiCreate(create, verbose)))
        case archive: domain.Event.ArchivedEvent =>
          Event(Archived(domainToApiArchive(archive)))
      },
      tx.offset.value
    )

  def domainTxToApiTree(tx: domain.TransactionTree, verbose: Boolean): TransactionTree =
    TransactionTree(
      tx.transactionId.unwrap,
      Tag.unsubst(tx.commandId).getOrElse(""),
      Tag.unsubst(tx.workflowId).getOrElse(""),
      Some(TimestampConversion.fromInstant(tx.effectiveAt)),
      tx.offset.value,
      tx.eventsById.map {
        case (eventId, create: domain.Event.CreatedEvent) =>
          eventId.unwrap -> TreeEvent(TreeEvent.Kind.Created(domainToApiCreate(create, verbose)))
        case (eventId, exercise: domain.Event.ExercisedEvent) =>
          eventId.unwrap -> TreeEvent(
            TreeEvent.Kind.Exercised(domainToApiExercise(exercise, verbose)))
      },
      tx.rootEventIds.map(_.unwrap)
    )

  private def domainToApiCreate(
      create: domain.Event.CreatedEvent,
      verbose: Boolean): CreatedEvent = {
    import create._
    CreatedEvent(
      eventId.unwrap,
      contractId.unwrap,
      Some(LfEngineToApi.toApiIdentifier(templateId)),
      contractKey.map(
        ck =>
          LfEngineToApi.assertOrRuntimeEx(
            "translating the contract key",
            LfEngineToApi
              .lfValueToApiValue(verbose, ck))),
      Some(
        LfEngineToApi
          .lfValueToApiRecord(verbose, createArguments)
          .fold(_ => throw new RuntimeException("Expected value to be a record."), identity)),
      witnessParties.toSeq,
      signatories.toSeq,
      observers.toSeq,
      Some(agreementText)
    )
  }

  private def domainToApiExercise(
      exercise: domain.Event.ExercisedEvent,
      verbose: Boolean): ExercisedEvent = {
    import exercise._
    ExercisedEvent(
      eventId.unwrap,
      contractId.unwrap,
      Some(LfEngineToApi.toApiIdentifier(templateId)),
      contractCreatingEventId.unwrap,
      choice,
      Some(
        LfEngineToApi
          .lfValueToApiValue(verbose, choiceArgument)
          .fold(_ => throw new RuntimeException("Error converting choice argument"), identity)),
      actingParties.toSeq,
      consuming,
      witnessParties.toSeq,
      children.map(_.unwrap),
      exerciseResult.map(
        LfEngineToApi
          .lfValueToApiValue(verbose, _)
          .fold(_ => throw new RuntimeException("Error converting exercise result"), identity)),
    )
  }

  private def domainToApiArchive(archive: domain.Event.ArchivedEvent): ArchivedEvent = {
    import archive._
    ArchivedEvent(
      eventId.unwrap,
      contractId.unwrap,
      Some(LfEngineToApi.toApiIdentifier(templateId)),
      witnessParties.toSeq
    )
  }

}
