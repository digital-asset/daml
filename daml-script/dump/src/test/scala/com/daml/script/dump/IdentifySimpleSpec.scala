// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.script.dump.TreeUtils.{Command, SimpleCommand, SimpleEvent}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

class IdentifySimpleSpec extends AnyFreeSpec with Matchers with OptionValues {
  "fromTree" - {
    "createdEvent" in {
      val events = TestData
        .Tree(
          Seq(
            TestData.Created(ContractId("cid1"))
          )
        )
        .toTransactionTree
      SimpleEvent.fromTree(events) should be(Symbol("defined"))
    }
    "simple exercisedEvent" in {
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
      SimpleEvent.fromTree(events) should be(Symbol("defined"))
    }
    "complex exercisedEvent" in {
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
      SimpleEvent.fromTree(events) should be(None)
    }
  }
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
      // TODO[AH] Identify simple createAndExercise commands.
      SimpleCommand.fromCommands(commands, events) should be(None)
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
