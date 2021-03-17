// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.script.dump.TreeUtils.{
  Command,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseCommand,
}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

class IdentifyCommandsSpec extends AnyFreeSpec with Matchers with OptionValues {
  "fromTree" - {
    "CreateCommand" in {
      val events = TestData
        .Tree(
          Seq(
            TestData.Created(ContractId("cid1"))
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 1
      commands.head shouldBe a[CreateCommand]
    }
    "ExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq(
            TestData.Exercised(ContractId("cid1"), Seq())
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 1
      commands.head shouldBe a[ExerciseCommand]
    }
    "CreateAndExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Exercised(ContractId("cid1"), Seq()),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 1
      commands.head shouldBe a[CreateAndExerciseCommand]
    }
    "non-adjacent Create and Exercise commands" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Created(ContractId("cid2")),
            TestData.Exercised(ContractId("cid1"), Seq()),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 3
      commands(0) shouldBe a[CreateCommand]
      commands(1) shouldBe a[CreateCommand]
      commands(2) shouldBe an[ExerciseCommand]
    }
  }
}
