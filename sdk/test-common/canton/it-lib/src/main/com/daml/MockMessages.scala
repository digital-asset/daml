// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.{LEDGER_BEGIN, LEDGER_END}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.value.Value.Sum.Text
import com.daml.ledger.api.v1.value.{Identifier, Value}
import com.google.protobuf.timestamp.Timestamp

import scala.util.Random

object MockMessages {

  val ledgerBegin = LedgerOffset(Boundary(LEDGER_BEGIN))
  val ledgerEnd = LedgerOffset(Boundary(LEDGER_END))

  val ledgerId = "ledgerId"
  val workflowId = "workflowId"
  val applicationId = "applicationId"
  val commandId = "commandId"
  val party = "party"
  val party2 = "party2"
  val ledgerEffectiveTime = Timestamp(0L, 0)

  val commands = Commands(ledgerId, workflowId, applicationId, commandId, party, Nil)

  val submitRequest = SubmitRequest(Some(commands))

  val submitAndWaitRequest = SubmitAndWaitRequest(Some(commands))

  val moduleName = "moduleName"
  val transactionId = "transactionId"
  val eventIdCreated = "eventIdCreate"
  val eventIdExercised = "eventIdExercise"
  val contractId = "contractId"
  val contractIdOther = contractId + "Other"
  def contractKey = Value(Text("contractKey"))
  val packageId = "packageId"
  val templateName = "templateName"
  val choice = "choice"
  val templateId = Identifier(packageId, moduleName, templateName)
  val offset = "offset"

  val transactionFilter =
    TransactionFilter(Map(party -> Filters()))

  val createdEvent = CreatedEvent(eventIdCreated + "2", contractIdOther, Some(templateId))

  val exercisedEvent = ExercisedEvent(
    eventIdExercised,
    contractId,
    Some(templateId),
    None,
    choice,
    None,
    List(party),
    true,
    Nil, // No witnesses
    List(createdEvent.eventId),
  )
  val transactionTree =
    TransactionTree(
      transactionId,
      commandId,
      workflowId,
      Some(ledgerEffectiveTime),
      offset,
      Map(
        exercisedEvent.eventId -> TreeEvent(TreeEvent.Kind.Exercised(exercisedEvent)),
        createdEvent.eventId -> TreeEvent(TreeEvent.Kind.Created(createdEvent)),
      ),
      List(exercisedEvent.eventId),
    )

  val filteredTransaction = Transaction(
    transactionId,
    commandId,
    workflowId,
    Some(ledgerEffectiveTime),
    List.empty,
    offset,
  )

  private val NO_OF_TRANSACTIONS = 1000

  private def randomId(name: String) = s"$name-${Random.nextInt(10000)}"

  private def generateEvent() = ExercisedEvent(
    randomId("event-id"),
    randomId("contract-id"),
    Some(Identifier(randomId("package-id"), randomId("moduleName"), randomId("template-id"))),
    None,
    randomId("choice-id"),
    None,
    List(randomId("party")),
    Random.nextBoolean(),
    Nil,
  )

  def generateMockTransactions(): List[TransactionTree] =
    (1 to NO_OF_TRANSACTIONS).map { i =>
      val event = generateEvent()
      TransactionTree(
        randomId("transaction"),
        randomId("command"),
        randomId("workflow"),
        Some(ledgerEffectiveTime),
        i.toString,
        Map(event.eventId -> TreeEvent(TreeEvent.Kind.Exercised(event))),
        List(event.eventId),
      )
    }.toList

}
