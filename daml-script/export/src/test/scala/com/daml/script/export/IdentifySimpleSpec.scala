// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.digitalasset.canton.ledger.api.refinements.ApiTypes.ContractId
import com.daml.ledger.api.v1.value.Value
import com.daml.script.export.TreeUtils.{Command, ExerciseByKeyCommand, SimpleCommand}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

class IdentifySimpleSpec extends AnyFreeSpec with Matchers with OptionValues {
  "fromCommands" - {
    "createCommand" in {
      val events = TestData
        .Tree(
          Seq(
            TestData.Created(ContractId("cid1"))
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(Symbol("defined"))
    }
    "simple exerciseCommand" in {
      val events = TestData
        .Tree(
          Seq(
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid2"))
              ),
              exerciseResult = Some(ContractId("cid2")),
            )
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(Symbol("defined"))
    }
    "complex exerciseCommand" in {
      val events = TestData
        .Tree(
          Seq(
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid2")),
                TestData.Created(ContractId("cid3")),
              ),
              exerciseResult = Some(ContractId("cid2")),
            )
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(None)
    }
    "simple exerciseByKeyCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1"), contractKey = Some(Value().withParty("Alice"))),
            TestData.Created(ContractId("cid2")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid3"))
              ),
              exerciseResult = Some(ContractId("cid3")),
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 3
      commands(2) shouldBe an[ExerciseByKeyCommand]
      SimpleCommand.fromCommands(commands, events) should be(Symbol("defined"))
    }
    "complex exerciseByKeyCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1"), contractKey = Some(Value().withParty("Alice"))),
            TestData.Created(ContractId("cid2")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid3")),
                TestData.Created(ContractId("cid4")),
              ),
              exerciseResult = Some(ContractId("cid3")),
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 3
      commands(2) shouldBe an[ExerciseByKeyCommand]
      SimpleCommand.fromCommands(commands, events) should be(None)
    }
    "simple createAndExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid2"))
              ),
              exerciseResult = Some(ContractId("cid2")),
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(Symbol("defined"))
    }
    "nested simple createAndExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq[TestData.Event](
                TestData.Created(ContractId("cid2")),
                TestData.Exercised(
                  ContractId("cid2"),
                  Seq(
                    TestData.Created(ContractId("cid3"))
                  ),
                  exerciseResult = Some(ContractId("cid3")),
                ),
              ),
              exerciseResult = Some(ContractId("cid3")),
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(Symbol("defined"))
    }
    "non-consuming createAndExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid2"))
              ),
              exerciseResult = Some(ContractId("cid2")),
              consuming = false,
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(None)
    }
    "nested consuming createAndExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq[TestData.Event](
                TestData.Created(ContractId("cid2")),
                TestData.Exercised(
                  ContractId("cid1"),
                  Seq(),
                ),
              ),
              exerciseResult = Some(ContractId("cid2")),
              consuming = false,
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(Symbol("defined"))
    }
    "complex createAndExerciseCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1")),
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(ContractId("cid2")),
                TestData.Created(ContractId("cid3")),
              ),
              exerciseResult = Some(ContractId("cid2")),
            ),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      SimpleCommand.fromCommands(commands, events) should be(None)
    }
  }
}
