// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import java.time.Instant

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.model.{
  ChoiceExercised,
  CommandStatusError,
  CommandStatusSuccess,
  CommandStatusUnknown,
  CommandStatusWaiting,
  ContractCreated,
  CreateCommand,
  ExerciseCommand,
  PackageRegistry,
  Transaction,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class RowSpec extends AnyWordSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  private val registry: PackageRegistry = PackageRegistry().withPackages(List(C.iface))

  "CommandRow" when {
    "converting CreateCommand" should {
      val value = CreateCommand(
        ApiTypes.CommandId("c01"),
        1L,
        ApiTypes.WorkflowId("w01"),
        Instant.EPOCH,
        C.complexRecordId,
        C.complexRecordV,
      )

      "not change the value" in {
        CommandRow.fromCommand(value).toCommand(registry) shouldBe Success(value)
      }
    }

    "converting ExerciseCommand" should {
      val value = ExerciseCommand(
        ApiTypes.CommandId("c01"),
        1L,
        ApiTypes.WorkflowId("w01"),
        Instant.EPOCH,
        ApiTypes.ContractId("#0:0"),
        C.complexRecordId,
        Some(C.complexRecordId),
        ApiTypes.Choice("text"),
        C.simpleTextV,
      )

      "not change the value" in {
        CommandRow.fromCommand(value).toCommand(registry) shouldBe Success(value)
      }
    }
  }

  "CommandStatusRow" when {
    "converting CommandStatusWaiting" should {
      val id = ApiTypes.CommandId("c01")
      val value = CommandStatusWaiting()

      "not change the value" in {
        CommandStatusRow
          .fromCommandStatus(id, value)
          .toCommandStatus(_ => Success(None)) shouldBe Success(value)
      }
    }

    "converting CommandStatusError" should {
      val id = ApiTypes.CommandId("c01")
      val value = CommandStatusError("code", "message")

      "not change the value" in {
        CommandStatusRow
          .fromCommandStatus(id, value)
          .toCommandStatus(_ => Success(None)) shouldBe Success(value)
      }
    }

    "converting CommandStatusSuccess" should {
      val id = ApiTypes.CommandId("c01")
      val tx = Transaction(
        ApiTypes.TransactionId("t01"),
        Some(ApiTypes.CommandId("c01")),
        Instant.EPOCH,
        "1",
        List.empty,
      )
      val value = CommandStatusSuccess(tx)

      "not change the value" in {
        CommandStatusRow
          .fromCommandStatus(id, value)
          .toCommandStatus(_ => Success(Some(tx))) shouldBe Success(value)
      }
    }

    "converting CommandStatusUnknown" should {
      val id = ApiTypes.CommandId("c01")
      val value = CommandStatusUnknown()

      "not change the value" in {
        CommandStatusRow
          .fromCommandStatus(id, value)
          .toCommandStatus(_ => Success(None)) shouldBe Success(value)
      }
    }
  }

  "EventRow" when {

    val alice = ApiTypes.Party("Alice")
    val bob = ApiTypes.Party("Bob")
    val charlie = ApiTypes.Party("Charlie")

    "converting ContractCreated" should {
      val value = ContractCreated(
        ApiTypes.EventId("e01"),
        Some(ApiTypes.EventId("e00")),
        ApiTypes.TransactionId("t01"),
        List(ApiTypes.Party("p01")),
        ApiTypes.WorkflowId("w01"),
        ApiTypes.ContractId("c01"),
        C.complexRecordId,
        C.complexRecordV,
        Some("agreement"),
        List(alice),
        List(bob, charlie),
        None,
      )

      "not change the value" in {
        EventRow.fromEvent(value).toEvent(registry) shouldBe Success(value)
      }
    }

    "converting ChoiceExercised" should {
      val value = ChoiceExercised(
        ApiTypes.EventId("e01"),
        Some(ApiTypes.EventId("e00")),
        ApiTypes.TransactionId("t01"),
        List(ApiTypes.Party("p01")),
        ApiTypes.WorkflowId("w01"),
        ApiTypes.ContractId("c01"),
        C.complexRecordId,
        ApiTypes.Choice("text"),
        C.simpleTextV,
        List(ApiTypes.Party("p01")),
        true,
      )

      "not change the value" in {
        EventRow.fromEvent(value).toEvent(registry) shouldBe Success(value)
      }
    }
  }
}
