// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import java.time.Instant

import com.daml.ledger.api.refinements.ApiTypes
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
class LedgerSpec extends AnyWordSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  private val party = ApiTypes.Party("party")

  private val templateRegistry = (new PackageRegistry).withPackages(List(C.iface))
  private val templateId = C.simpleRecordTemplateId
  private val contractArgument = C.simpleRecordV
  private val template = templateRegistry.template(templateId).get
  private val alice = ApiTypes.Party("Alice")
  private val bob = ApiTypes.Party("Bob")
  private val charlie = ApiTypes.Party("Charlie")

  private def transaction(id: String): Transaction =
    Transaction(
      ApiTypes.TransactionId(id),
      Some(ApiTypes.CommandId("commandId")),
      Instant.now(),
      "0",
      List.empty,
    )
  private def contract(id: String): Contract =
    Contract(
      ApiTypes.ContractId(id),
      template,
      contractArgument,
      Option(""),
      List(alice),
      List(bob, charlie),
      None,
    )

  "A ledger with existing contracts" when {
    val subject = Ledger(alice, None, false).withTransaction(
      transaction("Tx0").copy(
        events = List(
          ContractCreated(
            ApiTypes.EventId("E0"),
            None,
            ApiTypes.TransactionId("Tx0"),
            List.empty,
            ApiTypes.WorkflowId("workflowId"),
            ApiTypes.ContractId("C0"),
            templateId,
            contractArgument,
            Some(""),
            List(alice),
            List(bob, charlie),
            None,
          )
        )
      ),
      templateRegistry,
    )

    "adding a transaction" should {
      val latest = transaction("Tx1").copy(commandId = Some(ApiTypes.CommandId("Cmd1")))

      val result = subject.withTransaction(latest, templateRegistry)

      "have the transaction as the latest transaction" in {
        result.latestTransaction(templateRegistry) shouldBe Some(latest)
      }
    }

    "adding an event that creates contracts" should {
      def contractCreated(id: String, contractId: String): ContractCreated = ContractCreated(
        id = ApiTypes.EventId(id),
        parentId = None,
        transactionId = ApiTypes.TransactionId("Tx1"),
        witnessParties = List(party),
        workflowId = ApiTypes.WorkflowId("workflow"),
        contractId = ApiTypes.ContractId(contractId),
        templateId = templateId,
        argument = contractArgument,
        agreementText = Some(""),
        signatories = List(alice),
        observers = List(bob, charlie),
        key = None,
      )
      val created1 = contractCreated("E1", "C1")
      val created2 = contractCreated("E2", "C2")
      val latest = transaction("Tx1").copy(events = List(created1, created2))

      val result = subject.withTransaction(latest, templateRegistry)

      "return the created contracts by id" in {
        result.contract(ApiTypes.ContractId("C1"), templateRegistry) shouldBe Some(contract("C1"))
        result.contract(ApiTypes.ContractId("C2"), templateRegistry) shouldBe Some(contract("C2"))
      }

      "return the created contracts when traversing all contracts" in {
        result.allContracts(templateRegistry) should contain.allOf(contract("C1"), contract("C2"))
      }

      "consider the created contracts to be active" in {
        result.activeContracts(templateRegistry) should contain.allOf(
          contract("C1"),
          contract(
            "C2"
          ),
        )
      }

      "return the events by id" in {
        result.event(created1.id, templateRegistry) shouldBe Some(created1)
        result.event(created2.id, templateRegistry) shouldBe Some(created2)
      }

      "return the event creating the contracts" in {
        result.createEventOf(contract("C1"), templateRegistry) shouldBe Success(created1)
        result.createEventOf(contract("C2"), templateRegistry) shouldBe Success(created2)
      }

      "indicate that there is no event that archived the contracts" in {
        result.archiveEventOf(contract("C1"), templateRegistry) shouldBe None
        result.archiveEventOf(contract("C2"), templateRegistry) shouldBe None
      }

      "indicate that there is no event that exercised a choice on the contracts" in {
        result.exercisedEventsOf(contract("C1"), templateRegistry) shouldBe List.empty
        result.exercisedEventsOf(contract("C2"), templateRegistry) shouldBe List.empty
      }

      "increase the number of all contracts" in {
        result.allContractsCount shouldBe subject.allContractsCount + 2
      }

      "increase the number of active contracts" in {
        result.activeContractsCount shouldBe subject.activeContractsCount + 2
      }
    }

    "adding an event that creates a contract for which the party is not a stakeholder" should {
      def contractCreated(id: String, contractId: String): ContractCreated = ContractCreated(
        id = ApiTypes.EventId(id),
        parentId = None,
        transactionId = ApiTypes.TransactionId("Tx2"),
        witnessParties = List(party),
        workflowId = ApiTypes.WorkflowId("workflow"),
        contractId = ApiTypes.ContractId(contractId),
        templateId = templateId,
        argument = contractArgument,
        agreementText = Some(""),
        signatories = List(bob),
        observers = List(charlie),
        key = None,
      )

      val created1 = contractCreated("NonVisible", "NotVisible")
      val latest = transaction("Tx2").copy(events = List(created1))

      val result = subject.withTransaction(latest, templateRegistry)

      "not store the contract for the party" in {
        result.contract(created1.contractId, templateRegistry) shouldBe None
      }
    }

    "adding an event that exercises a consuming choice" should {
      val exercisedEvent = ChoiceExercised(
        id = ApiTypes.EventId("E1"),
        parentId = None,
        transactionId = ApiTypes.TransactionId("Tx1"),
        witnessParties = List(party),
        workflowId = ApiTypes.WorkflowId("workflow"),
        contractId = ApiTypes.ContractId("C0"),
        templateId = templateId,
        choice = ApiTypes.Choice("choice"),
        argument = C.simpleUnitV,
        actingParties = List(party),
        consuming = true,
      )
      val latest = transaction("Tx1").copy(events = List(exercisedEvent))

      val result = subject.withTransaction(latest, templateRegistry)

      "still return the target contract by id" in {
        result.contract(ApiTypes.ContractId("C0"), templateRegistry) shouldBe Some(contract("C0"))
      }

      "still return the target contract when traversing all contracts" in {
        result.allContracts(templateRegistry) should contain(contract("C0"))
      }

      "still not consider the target contract to be active" in {
        result.activeContracts(templateRegistry) should not contain (contract("C0"))
      }

      "return the event by id" in {
        result.event(exercisedEvent.id, templateRegistry) shouldBe Some(exercisedEvent)
      }

      "list the event that exercised the choice" in {
        result.exercisedEventsOf(contract("C0"), templateRegistry) should contain(exercisedEvent)
      }

      "list the event that archived the contract" in {
        result.archiveEventOf(contract("C0"), templateRegistry) should contain(exercisedEvent)
      }

      "leave the number of all contracts unchanged" in {
        result.allContractsCount shouldBe subject.allContractsCount
      }

      "decrease the number of active contracts" in {
        result.activeContractsCount shouldBe subject.activeContractsCount - 1
      }
    }

    "adding a transaction with an exercise event and a child create event" should {
      val createdEvent = ContractCreated(
        id = ApiTypes.EventId("E3"),
        parentId = Some(ApiTypes.EventId("E2")),
        transactionId = ApiTypes.TransactionId("Tx1"),
        witnessParties = List(party),
        workflowId = ApiTypes.WorkflowId("workflow"),
        contractId = ApiTypes.ContractId("C3"),
        templateId = templateId,
        argument = contractArgument,
        agreementText = Some(""),
        signatories = List(alice),
        observers = List(bob, charlie),
        key = None,
      )
      val exercisedEvent = ChoiceExercised(
        id = ApiTypes.EventId("E2"),
        parentId = None,
        transactionId = ApiTypes.TransactionId("Tx1"),
        witnessParties = List(party),
        workflowId = ApiTypes.WorkflowId("workflow"),
        contractId = ApiTypes.ContractId("C0"),
        templateId = templateId,
        choice = ApiTypes.Choice("choice"),
        argument = C.simpleUnitV,
        actingParties = List(party),
        consuming = true,
      )
      val latest = transaction("Tx1").copy(events = List[Event](exercisedEvent, createdEvent))

      val result = subject.withTransaction(latest, templateRegistry)

      "still return the target contract by id" in {
        result.contract(ApiTypes.ContractId("C0"), templateRegistry) shouldBe Some(contract("C0"))
      }

      "return the new contract by id" in {
        result.contract(ApiTypes.ContractId("C3"), templateRegistry) shouldBe Some(contract("C3"))
      }

      "still return the target contract when traversing all contracts" in {
        result.allContracts(templateRegistry) should contain(contract("C0"))
      }

      "return the new contract when traversing all contracts" in {
        result.allContracts(templateRegistry) should contain(contract("C3"))
      }

      "consider the target contract to no longer be active" in {
        result.activeContracts(templateRegistry) should not contain contract("C0")
      }

      "return the events by id" in {
        result.event(exercisedEvent.id, templateRegistry) shouldBe Some(exercisedEvent)
        result.event(createdEvent.id, templateRegistry) shouldBe Some(createdEvent)
      }

      "list the event that exercised the choice" in {
        result.exercisedEventsOf(contract("C0"), templateRegistry) should contain(exercisedEvent)
      }

      "return the event that archived the contract" in {
        result.archiveEventOf(contract("C0"), templateRegistry) shouldBe Some(exercisedEvent)
      }

      "return the event that created the new contract" in {
        result.createEventOf(contract("C3"), templateRegistry) shouldBe Success(createdEvent)
      }

      "increase the number of all contracts" in {
        result.allContractsCount shouldBe subject.allContractsCount + 1
      }

      "leave the number of active contracts unchanged" in {
        result.activeContractsCount shouldBe subject.activeContractsCount
      }
    }

    "adding a command" should {
      val createCmd = CreateCommand(
        ApiTypes.CommandId("Cmd1"),
        1,
        ApiTypes.WorkflowId("W1"),
        Instant.EPOCH,
        template.id,
        contractArgument,
      )

      val result = subject.withCommand(createCmd)

      "report the status of the command as waiting" in {
        result.statusOf(ApiTypes.CommandId("Cmd1"), templateRegistry) shouldBe Some(
          CommandStatusWaiting()
        )
      }

      "don't report any result of the command" in {
        result.resultOf(ApiTypes.CommandId("Cmd1"), templateRegistry) shouldBe None
      }
    }

    "adding a command and then a transaction" should {
      val createCmd = CreateCommand(
        ApiTypes.CommandId("Cmd1"),
        1,
        ApiTypes.WorkflowId("W1"),
        Instant.EPOCH,
        template.id,
        contractArgument,
      )

      val latest = transaction("Tx1").copy(commandId = Some(ApiTypes.CommandId("Cmd1")))

      val result = subject.withCommand(createCmd).withTransaction(latest, templateRegistry)

      "report the status of the command as success with the transaction" in {
        result.statusOf(ApiTypes.CommandId("Cmd1"), templateRegistry) shouldBe Some(
          CommandStatusSuccess(latest)
        )
      }

      "report the result of the command as success with the transaction" in {
        result.resultOf(ApiTypes.CommandId("Cmd1"), templateRegistry) shouldBe Some(
          Result(ApiTypes.CommandId("Cmd1"), Right(latest))
        )
      }
    }
  }
}
