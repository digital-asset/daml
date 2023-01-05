// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v1.event.{CreatedEvent, Event}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.value.{Identifier, Record, Value}
import com.daml.ledger.client.LedgerClient
import com.daml.platform.participant.util.ValueConversions._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

object Application {

  final case class Contract(identifier: String) extends AnyVal
  final case class Template(identifier: Identifier) extends AnyVal

  final case class Choice(template: Template, name: String)

  private def toEventResult(e: Event): EventResult = {
    e.event match {
      case Event.Event.Created(created) =>
        CreatedResult(
          entityName = created.getTemplateId.entityName,
          moduleName = created.getTemplateId.moduleName,
          contractId = created.contractId,
          argument = created.getCreateArguments,
        )
      case Event.Event.Archived(archived) =>
        ArchivedResult(
          entityName = archived.getTemplateId.entityName,
          moduleName = archived.getTemplateId.moduleName,
          archived.contractId,
        )
      case Event.Event.Empty =>
        throw new RuntimeException("Invalid empty event")
    }
  }

  object ContractResult {
    def fromCreateEvent(event: CreatedEvent): ContractResult =
      ContractResult(event.contractId, event.createArguments.get)
  }
  final case class ContractResult(_1: String, _2: Record)

  object TransactionResult {
    def fromTransaction(transaction: Transaction): TransactionResult =
      TransactionResult(
        transaction.transactionId,
        transaction.events.map(toEventResult),
        transaction.effectiveAt.map(t => t.seconds * 1000000L + t.nanos.toLong / 1000L).get,
      )
  }
  final case class TransactionResult(
      transactionId: String,
      events: Seq[EventResult],
      letMicros: Long,
  )

  sealed abstract class EventResult
  final case class CreatedResult(
      entityName: String,
      moduleName: String,
      contractId: String,
      argument: Record,
  ) extends EventResult
  final case class ArchivedResult(
      entityName: String,
      moduleName: String,
      contractId: String,
  ) extends EventResult

  final class Party(
      val name: String,
      client: LedgerClient,
      applicationId: String,
  )(implicit ec: ExecutionContext, mat: Materializer) {

    private def submitAndWait(commands: Commands): Future[TransactionTree] =
      client.commandServiceClient
        .submitAndWaitForTransactionTree(SubmitAndWaitRequest(Some(commands)))
        .map(_.getTransaction)

    def create(template: Template, arguments: (String, Value)*): Future[Contract] = {
      val argument = Record(
        Some(template.identifier),
        arguments.asRecordFields,
      )
      val commands = Commands(
        party = name,
        commands = List(
          Command().withCreate(
            CreateCommand(
              Some(template.identifier),
              Some(argument),
            )
          )
        ),
        ledgerId = client.ledgerId.unwrap,
        applicationId = applicationId,
        commandId = UUID.randomUUID.toString,
      )
      for {
        tree <- submitAndWait(commands)
      } yield {
        assert(tree.eventsById.size == 1)
        Contract(tree.eventsById.head._2.getCreated.contractId)
      }
    }

    def exercise(choice: Choice, contract: Contract, arguments: (String, Value)*): Future[Value] = {
      val commands = Commands(
        party = name,
        commands = List(
          Command().withExercise(
            ExerciseCommand(
              Some(choice.template.identifier),
              contract.identifier,
              choice.name,
              Some(arguments.asRecordValue),
            )
          )
        ),
        ledgerId = client.ledgerId.unwrap,
        applicationId = applicationId,
        commandId = UUID.randomUUID.toString,
      )
      for {
        tree <- submitAndWait(commands)
      } yield {
        assert(tree.rootEventIds.size == 1)
        tree.eventsById(tree.rootEventIds.head).getExercised.getExerciseResult
      }
    }

    def activeContracts(template: Template): Future[Seq[CreatedEvent]] = {
      val filter = TransactionFilter(
        List((name, Filters(Some(InclusiveFilters(Seq(template.identifier)))))).toMap
      )
      client.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
        .map(_.flatMap(_.activeContracts))
    }

    def transactions(templates: Seq[Template]): Future[Seq[Transaction]] = {
      val filter = TransactionFilter(
        List(
          (
            name,
            Filters(
              Some(
                InclusiveFilters(
                  templates
                    .map(_.identifier)
                )
              )
            ),
          )
        ).toMap
      )
      client.transactionClient
        .getTransactions(
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
          Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))),
          filter,
          verbose = true,
        )
        .runWith(Sink.seq[Transaction])
    }

  }

}
