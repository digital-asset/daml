// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.data.DatabaseActions
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.LazyList
import scala.util.{Failure, Success, Try}

/** In-memory projection of ledger events. */
case class Ledger(
    private val forParty: ApiTypes.Party,
    private val lastTransaction: Option[Transaction],
    private val useDatabase: Boolean,
    private val transactionById: Map[ApiTypes.TransactionId, Transaction] = Map.empty,
    private val eventById: Map[ApiTypes.EventId, Event] = Map.empty,
    private val contractById: Map[ApiTypes.ContractId, Contract] = Map.empty,
    private val activeContractById: Map[ApiTypes.ContractId, Contract] = Map.empty,
    private val createEventByContractId: Map[ApiTypes.ContractId, ContractCreated] = Map.empty,
    private val archiveEventByContractId: Map[ApiTypes.ContractId, ChoiceExercised] = Map.empty,
    private val choiceExercisedEventByContractById: Map[ApiTypes.ContractId, List[
      ChoiceExercised
    ]] = Map.empty,
    private val contractsByTemplateId: Map[DamlLfIdentifier, Set[Contract]] = Map.empty,
    private val commandById: Map[ApiTypes.CommandId, Command] = Map.empty,
    private val statusByCommandId: Map[ApiTypes.CommandId, CommandStatus] = Map.empty,
    private val db: DatabaseActions = new DatabaseActions,
) extends LazyLogging {

  private def logErrorAndDefaultTo[A](result: Try[A], default: A): A = {
    result match {
      case Success(a) => a
      case Failure(e) =>
        logger.error(e.getMessage)
        default
    }
  }

  def latestTransaction(types: PackageRegistry): Option[Transaction] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.lastTransaction(types), None)
    } else {
      lastTransaction
    }
  }

  def withCommandStatus(commandId: ApiTypes.CommandId, result: CommandStatus): Ledger = {
    if (useDatabase) {
      db.upsertCommandStatus(commandId, result)
      this
    } else {
      copy(
        statusByCommandId = statusByCommandId + (commandId -> result)
      )
    }
  }

  def withCommand(command: Command): Ledger = {
    if (useDatabase) {
      db.insertCommand(command)
      db.upsertCommandStatus(command.id, CommandStatusWaiting())
      this
    } else {
      copy(
        commandById = commandById + (command.id -> command),
        statusByCommandId = statusByCommandId + (command.id -> CommandStatusWaiting()),
      )
    }
  }

  def withTransaction(tx: Transaction, packageRegistry: PackageRegistry): Ledger = {
    val ledger0 = this
      .withLatestTransaction(tx)
      .withTransactionResult(tx)

    tx.events.foldLeft(ledger0) { (ledger, event) =>
      ledger.withEvent(event, packageRegistry)
    }
  }

  private def withLatestTransaction(tx: Transaction): Ledger = {
    if (useDatabase) {
      db.insertTransaction(tx)
      this
    } else {
      copy(
        lastTransaction = Some(tx),
        transactionById = transactionById + (tx.id -> tx),
      )
    }
  }

  private def withTransactionResult(tx: Transaction): Ledger = {
    // Note: only store the status for commands submitted by this client
    if (useDatabase) {
      tx.commandId.foreach(db.updateCommandStatus(_, CommandStatusSuccess(tx)))
      this
    } else {
      tx.commandId
        .filter(commandById.contains)
        .fold(this) { commandId =>
          copy(statusByCommandId = statusByCommandId + (commandId -> CommandStatusSuccess(tx)))
        }
    }
  }

  private def withEvent(event: Event, packageRegistry: PackageRegistry): Ledger =
    event match {
      case event: ContractCreated =>
        packageRegistry.template(event.templateId).fold(this) { template =>
          val contract = Contract(
            event.contractId,
            template,
            event.argument,
            event.agreementText,
            event.signatories,
            event.observers,
            event.key,
          )
          withContractCreatedInEvent(contract, event)
        }

      case event: ChoiceExercised =>
        withChoiceExercisedInEvent(event.contractId, event)
    }

  private def withContractCreatedInEvent(contract: Contract, event: ContractCreated): Ledger = {
    val isStakeHolder =
      contract.signatories.contains(forParty) || contract.observers.contains(forParty)
    if (useDatabase) {
      if (isStakeHolder) db.insertContract(contract)
      db.insertEvent(event)
      this
    } else {
      val contractUpdated =
        if (isStakeHolder)
          copy(
            contractById = contractById + (contract.id -> contract),
            activeContractById = activeContractById + (contract.id -> contract),
            contractsByTemplateId = contractsByTemplateIdWith(contract),
            createEventByContractId = createEventByContractId + (contract.id -> event),
          )
        else
          this
      contractUpdated.copy(eventById = eventById + (event.id -> event))
    }
  }

  private def contractsByTemplateIdWith(contract: Contract) = {
    val templateId = contract.template.id
    val entryWithContract =
      templateId -> (contractsByTemplateId.getOrElse(templateId, Set.empty) + contract)
    contractsByTemplateId + entryWithContract
  }

  private def withChoiceExercisedInEvent(
      contractId: ApiTypes.ContractId,
      event: ChoiceExercised,
  ): Ledger = {
    if (useDatabase) {
      if (event.consuming) {
        db.archiveContract(contractId, event.transactionId)
      }
      db.insertEvent(event)
      this
    } else {
      val prevExercises: List[ChoiceExercised] =
        choiceExercisedEventByContractById.getOrElse(contractId, List.empty)
      copy(
        archiveEventByContractId =
          if (event.consuming) archiveEventByContractId + (contractId -> event)
          else archiveEventByContractId,
        activeContractById =
          if (event.consuming) activeContractById - contractId else activeContractById,
        choiceExercisedEventByContractById =
          choiceExercisedEventByContractById + (contractId -> (event :: prevExercises)),
        eventById = eventById + (event.id -> event),
      )
    }
  }

  def allContractsCount: Int = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.contractCount(), 0)
    } else {
      contractById.size
    }
  }

  def activeContractsCount: Int = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.activeContractCount(), 0)
    } else {
      activeContractById.size
    }
  }

  def contract(id: ApiTypes.ContractId, types: PackageRegistry): Option[Contract] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.contract(id, types), None)
    } else {
      contractById.get(id)
    }
  }

  def event(id: ApiTypes.EventId, types: PackageRegistry): Option[Event] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.eventById(id, types), None)
    } else {
      eventById.get(id)
    }
  }

  def childEvents(id: ApiTypes.EventId, types: PackageRegistry): List[Event] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.eventsByParentId(id, types), List.empty)
    } else {
      (for {
        ev <- eventById.get(id)
        tx <- transactionById.get(ev.transactionId)
      } yield {
        tx.events.filter(_.parentId.contains(id))
      }).getOrElse(List.empty)
    }
  }

  def transaction(id: ApiTypes.TransactionId, types: PackageRegistry): Option[Transaction] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.transactionById(id, types), None)
    } else {
      transactionById.get(id)
    }
  }

  def command(id: ApiTypes.CommandId, types: PackageRegistry): Option[Command] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.commandById(id, types), None)
    } else {
      commandById.get(id)
    }
  }

  def commandStatus(id: ApiTypes.CommandId, types: PackageRegistry): Option[CommandStatus] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.commandStatusByCommandId(id, types), None)
    } else {
      statusByCommandId.get(id)
    }
  }

  def createEventOf(contract: Contract, types: PackageRegistry): Try[ContractCreated] = {
    if (useDatabase) {
      db.createEventByContractId(contract.id, types)
    } else {
      Success(createEventByContractId(contract.id))
    }
  }

  def archiveEventOf(contract: Contract, types: PackageRegistry): Option[ChoiceExercised] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.archiveEventByContractId(contract.id, types), None)
    } else {
      archiveEventByContractId.get(contract.id)
    }
  }

  def exercisedEventsOf(contract: Contract, types: PackageRegistry): List[ChoiceExercised] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.choiceExercisedEventByContractById(contract.id, types), List.empty)
    } else {
      choiceExercisedEventByContractById.getOrElse(contract.id, List.empty)
    }
  }

  def allContracts(types: PackageRegistry): LazyList[Contract] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.contracts(types), List.empty[Contract]).to(LazyList)
    } else {
      contractById.values.to(LazyList)
    }
  }

  def activeContracts(types: PackageRegistry): LazyList[Contract] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.activeContracts(types), List.empty[Contract]).to(LazyList)
    } else {
      activeContractById.values.to(LazyList)
    }
  }

  def activeTemplateContractsOf(template: Template, types: PackageRegistry): LazyList[Contract] = {
    if (useDatabase) {
      logErrorAndDefaultTo(
        db.activeContractsForTemplate(template.id, types),
        List.empty[Contract],
      ).to(LazyList)
    } else {
      templateContractsOf(template, types).filter(contract =>
        activeContractById.contains(contract.id)
      )
    }
  }

  def templateContractsOf(template: Template, types: PackageRegistry): LazyList[Contract] = {
    if (useDatabase) {
      logErrorAndDefaultTo(
        db.contractsForTemplate(template.id, types),
        List.empty[Contract],
      ).to(LazyList)
    } else {
      contractsByTemplateId.getOrElse(template.id, Set.empty).to(LazyList)
    }
  }

  def allCommands(types: PackageRegistry): LazyList[Command] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.allCommands(types), List.empty[Command]).to(LazyList)
    } else {
      commandById.values.to(LazyList)
    }
  }

  def statusOf(commandId: ApiTypes.CommandId, types: PackageRegistry): Option[CommandStatus] = {
    if (useDatabase) {
      logErrorAndDefaultTo(db.commandStatusByCommandId(commandId, types), None)
    } else {
      statusByCommandId.get(commandId)
    }
  }

  def databaseSchema(): Try[String] =
    if (useDatabase) {
      db.schema()
    } else {
      Failure(NoDatabaseUsed())
    }

  def runQuery(query: String): Try[SqlQueryResult] =
    if (useDatabase) {
      db.runQuery(query)
    } else {
      Failure(NoDatabaseUsed())
    }

  // Deprecated
  def resultOf(commandId: ApiTypes.CommandId, types: PackageRegistry): Option[Result] =
    for {
      status <- statusOf(commandId, types)
      result <- status match {
        case _: CommandStatusWaiting => None
        case cmd: CommandStatusSuccess => Some(Result(commandId, Right(cmd.tx)))
        case cmd: CommandStatusError =>
          Some(Result(commandId, Left(new Error(cmd.code, cmd.details, ""))))
        case _: CommandStatusUnknown =>
          Some(Result(commandId, Left(new Error("INTERNAL", "Command status unknown", ""))))
      }
    } yield result
}
