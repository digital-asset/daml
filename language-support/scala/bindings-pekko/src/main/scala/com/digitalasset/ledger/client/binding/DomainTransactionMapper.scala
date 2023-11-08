// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import com.daml.ledger.api.refinements.ApiTypes._
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.client.binding.DomainTransactionMapper.DecoderType
import com.typesafe.scalalogging.LazyLogging
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.collection.immutable

object DomainTransactionMapper {

  type DecoderType = CreatedEvent => Either[EventDecoderError, Contract.OfAny]

  def apply(decoder: DecoderType): Flow[Transaction, DomainTransaction, NotUsed] =
    new DomainTransactionMapper(decoder).transactionsMapper

  private sealed trait InputValidationError extends Product with Serializable
  private final case class RequiredFieldDoesNotExistError(field: String)
      extends InputValidationError
  private final case object EmptyEvent extends InputValidationError
}

class DomainTransactionMapper(decoder: DecoderType) extends LazyLogging {

  import DomainTransactionMapper._

  def transactionsMapper: Flow[Transaction, DomainTransaction, NotUsed] =
    Flow[Transaction].mapConcat { transaction =>
      domainTransaction(transaction) match {
        case Left(error) =>
          logger.warn(
            s"Input validation error when converting to domain transaction: $error. Transaction is discarded."
          )
          List.empty
        case Right(t) =>
          List(t)
      }
    }

  private def domainTransaction(t: Transaction): Either[InputValidationError, DomainTransaction] =
    for {
      effectiveAt <- checkExists("effectiveAt", t.effectiveAt)
      events <- domainEvents(t.events)
      transactionId = TransactionId(t.transactionId)
      workflowId = WorkflowId(t.workflowId)
      offset = LedgerOffset(Absolute(t.offset))
      commandId = CommandId(t.commandId)
    } yield DomainTransaction(
      transactionId,
      workflowId,
      offset,
      commandId,
      effectiveAt,
      events,
    )

  private def checkExists[T](
      fieldName: String,
      maybeElement: Option[T],
  ): Either[InputValidationError, T] =
    maybeElement match {
      case Some(element) => Right(element)
      case None => Left(RequiredFieldDoesNotExistError(fieldName))
    }

  private def domainEvents(events: Seq[Event]): Either[InputValidationError, Seq[DomainEvent]] =
    events.toList
      .traverse { event =>
        for {
          domainEvent <- mapEvent(event)
        } yield domainEvent.toList
      }
      .map(_.flatten)

  private def mapEvent(event: Event): Either[InputValidationError, Option[DomainEvent]] =
    event.event match {
      case Created(createdEvent) =>
        decoder(createdEvent)
          .fold(logAndDiscard(createdEvent), mapCreatedEvent(createdEvent, _).map(Some.apply))
      case Archived(archivedEvent) =>
        mapArchivedEvent(archivedEvent).map(Some.apply)
      case Empty =>
        Left(EmptyEvent)
    }

  private def logAndDiscard(
      event: CreatedEvent
  )(err: EventDecoderError): Either[InputValidationError, Option[DomainEvent]] = {
    // TODO: improve error handling (make discarding error log message configurable)
    logger.warn(s"Unhandled create event ${event.toString}. Error: ${err.toString}")
    Right(None)
  }

  private def mapCreatedEvent(
      createdEvent: CreatedEvent,
      contract: Contract.OfAny,
  ): Either[InputValidationError, DomainCreatedEvent] =
    for {
      tid <- checkExists("events.witnessedEvents.event.created.templateId", createdEvent.templateId)

      arguments <- checkExists(
        "events.witnessedEvents.event.created.createArguments",
        createdEvent.createArguments,
      )

      eventId = EventId(createdEvent.eventId)
      contractId = ContractId(createdEvent.contractId)
      templateId = TemplateId(tid)
      witnessParties = createdEvent.witnessParties.map(Party.apply).to(immutable.Seq)
      createArguments = CreateArguments(arguments)
    } yield DomainCreatedEvent(
      eventId,
      contractId,
      templateId,
      witnessParties,
      createArguments,
      contract,
    )

  private def mapArchivedEvent(
      archivedEvent: ArchivedEvent
  ): Either[InputValidationError, DomainArchivedEvent] =
    for {
      tid <- checkExists(
        "events.witnessedEvents.event.archived.templateId",
        archivedEvent.templateId,
      )

      eventId = EventId(archivedEvent.eventId)
      contractId = ContractId(archivedEvent.contractId)
      templateId = TemplateId(tid)
      witnessParties = archivedEvent.witnessParties.map(Party.apply).to(immutable.Seq)
    } yield DomainArchivedEvent(eventId, contractId, templateId, witnessParties)

}
