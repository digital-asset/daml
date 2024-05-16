// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.event.*
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN
import com.daml.ledger.api.v2.transaction.{Transaction, TransactionTree, TreeEvent}
import com.daml.ledger.api.v2.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v2.value.Value.Sum.Text
import com.daml.ledger.api.v2.value.{Identifier, Value}
import com.google.protobuf.timestamp.Timestamp

import scala.util.Random

object MockMessages {

  val participantBegin: ParticipantOffset = ParticipantOffset(
    ParticipantOffset.Value.Boundary(PARTICIPANT_BOUNDARY_BEGIN)
  )
  val workflowId = "workflowId"
  val applicationId = "applicationId"
  val commandId = "commandId"
  val party = "party"
  val party2 = "party2"
  val ledgerEffectiveTime: Timestamp = Timestamp(0L, 0)

  val commands: Commands =
    Commands(workflowId, applicationId, commandId, Nil, actAs = Seq(party))

  val submitRequest: SubmitRequest = SubmitRequest(Some(commands))

  val submitAndWaitRequest: SubmitAndWaitRequest = SubmitAndWaitRequest(Some(commands))

  val moduleName = "moduleName"
  val transactionId = "transactionId"
  val eventIdCreated = "eventIdCreate"
  val eventIdExercised = "eventIdExercise"
  val contractId = "contractId"
  val contractIdOther: String = contractId + "Other"
  def contractKey: Value = Value(Text("contractKey"))
  val packageId = "packageId"
  val templateName = "templateName"
  val choice = "choice"
  val templateId: Identifier = Identifier(packageId, moduleName, templateName)
  val offset = "offset"

  val transactionFilter: TransactionFilter =
    TransactionFilter(Map(party -> Filters()))

  val createdEvent: CreatedEvent =
    CreatedEvent(eventIdCreated + "2", contractIdOther, Some(templateId))

  val exercisedEvent: ExercisedEvent = ExercisedEvent(
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
  val transactionTree: TransactionTree =
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

  val filteredTransaction: Transaction = Transaction(
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
