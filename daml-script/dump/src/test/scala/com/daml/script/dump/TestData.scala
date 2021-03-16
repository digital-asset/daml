// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value, Variant}
import com.daml.script.dump.TreeUtils.{Command, SimpleCommand, SimpleEvent}
import com.google.protobuf

object TestData {
  val defaultTemplateId = Identifier("package", "Module", "Template")
  val defaultParties = Party.subst(Seq("Alice"))
  val defaultChoiceArgument = Value().withVariant(
    Variant(
      Some(Identifier("package", "Module", "Choice")),
      "Choice",
      Some(Value().withUnit(protobuf.empty.Empty())),
    )
  )
  val defaultExerciseResult = Value().withUnit(protobuf.empty.Empty())

  sealed trait Event
  sealed case class Created(
      contractId: ContractId,
      createArguments: Seq[RecordField] = Seq.empty,
      submitters: Seq[Party] = defaultParties,
  ) extends Event {
    def toCreatedEvent(eventId: String): CreatedEvent = {
      CreatedEvent(
        eventId = eventId,
        templateId = Some(defaultTemplateId),
        contractId = ContractId.unwrap(contractId),
        signatories = Party.unsubst(submitters),
        createArguments = Some(Record(recordId = Some(defaultTemplateId), fields = createArguments)),
      )
    }
  }
  sealed case class Exercised(
      contractId: ContractId,
      childEvents: Seq[Event],
      choiceArgument: Value = defaultChoiceArgument,
      actingParties: Seq[Party] = defaultParties,
      exerciseResult: Option[ContractId] = None,
  ) extends Event

  sealed case class ACS(contracts: Seq[Created]) {
    def toCreatedEvents: Seq[CreatedEvent] = {
      contracts.zipWithIndex.map { case (created, i) =>
        created.toCreatedEvent(s"create$i")
      }
    }
  }

  sealed case class Tree(rootEvents: Seq[Event]) {
    def toTransactionTree: TransactionTree = {
      var count = 0
      def go(
          acc: (Seq[String], Map[String, TreeEvent]),
          event: Event,
      ): (Seq[String], Map[String, TreeEvent]) = {
        val (rootEventIds, eventsById) = acc
        val eventId = s"ev$count"
        count += 1
        event match {
          case event: Created =>
            val treeEvent = TreeEvent(TreeEvent.Kind.Created(event.toCreatedEvent(eventId)))
            (rootEventIds :+ eventId, eventsById + (eventId -> treeEvent))
          case Exercised(contractId, childEvents, choiceArgument, actingParties, exerciseResult) =>
            val (childEventIds, childEventsById) =
              childEvents.foldLeft((Seq.empty[String], Map.empty[String, TreeEvent]))(go)
            val treeEvent = TreeEvent(
              TreeEvent.Kind.Exercised(
                ExercisedEvent(
                  eventId = eventId,
                  templateId = Some(defaultTemplateId),
                  contractId = ContractId.unwrap(contractId),
                  actingParties = Party.unsubst(actingParties),
                  choice = "Choice",
                  choiceArgument = Some(choiceArgument),
                  childEventIds = childEventIds,
                  exerciseResult = Some(
                    exerciseResult
                      .map(cid => Value().withContractId(ContractId.unwrap(cid)))
                      .getOrElse(defaultExerciseResult)
                  ),
                )
              )
            )
            (rootEventIds :+ eventId, eventsById + (eventId -> treeEvent) ++ childEventsById)
        }
      }
      val (rootEventIds, eventsById) =
        rootEvents.foldLeft((Seq.empty[String], Map.empty[String, TreeEvent]))(go)
      TransactionTree(
        transactionId = "txid",
        commandId = "cmdid",
        workflowId = "flowid",
        effectiveAt = None,
        offset = "",
        eventsById = eventsById,
        rootEventIds = rootEventIds,
        traceContext = None,
      )
    }
    def toSimpleEvents: Seq[SimpleEvent] = {
      SimpleEvent.fromTree(this.toTransactionTree).get
    }
    def toSimpleCommands: Seq[SimpleCommand] = {
      val tree = this.toTransactionTree
      SimpleCommand.fromCommands(Command.fromTree(tree), tree).get
    }
  }
}
