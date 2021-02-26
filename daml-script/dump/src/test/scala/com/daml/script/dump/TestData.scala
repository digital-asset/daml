// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value, Variant}
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

  sealed trait Event
  sealed case class Created(
      contractId: ContractId,
      createArguments: Seq[RecordField] = Seq.empty,
  ) extends Event {
    def toCreatedEvent(eventId: String): CreatedEvent = {
      CreatedEvent(
        eventId = eventId,
        templateId = Some(defaultTemplateId),
        contractId = ContractId.unwrap(contractId),
        signatories = Party.unsubst(defaultParties),
        createArguments = Some(Record(recordId = Some(defaultTemplateId), fields = createArguments)),
      )
    }
  }
  sealed case class Exercised(
      contractId: ContractId,
      childEvents: Seq[Event],
      choiceArgument: Value = defaultChoiceArgument,
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
          case Exercised(contractId, childEvents, choiceArgument) =>
            val (childEventIds, childEventsById) =
              childEvents.foldLeft((Seq.empty[String], Map.empty[String, TreeEvent]))(go)
            val treeEvent = TreeEvent(
              TreeEvent.Kind.Exercised(
                ExercisedEvent(
                  eventId = eventId,
                  templateId = Some(defaultTemplateId),
                  contractId = ContractId.unwrap(contractId),
                  actingParties = Party.unsubst(defaultParties),
                  choice = "Choice",
                  choiceArgument = Some(choiceArgument),
                  childEventIds = childEventIds,
                  exerciseResult = Some(Value().withUnit(protobuf.empty.Empty())),
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
  }
}
