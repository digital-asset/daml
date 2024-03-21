// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.script.export.TreeUtils.{
  Command,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

class IdentifyCommandsSpec extends AnyFreeSpec with Matchers with OptionValues {
  private def tuple2(a: Value, b: Value): Value = {
    Value().withRecord(
      Record()
        .withRecordId(
          Identifier()
            .withPackageId("pkg-id")
            .withModuleName("DA.Types")
            .withEntityName("Tuple2")
        )
        .withFields(
          Seq(
            RecordField()
              .withLabel("_1")
              .withValue(a),
            RecordField()
              .withLabel("_2")
              .withValue(b),
          )
        )
    )
  }
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
    "ExerciseByKeyCommand" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(
              ContractId("cid1"),
              contractKey = Some(Value().withParty("Alice")),
            ),
            TestData.Created(ContractId("cid2")),
            TestData.Exercised(ContractId("cid1"), Seq()),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 3
      commands(0) shouldBe a[CreateCommand]
      commands(1) shouldBe a[CreateCommand]
      commands(2) shouldBe an[ExerciseByKeyCommand]
    }
    "ExerciseByKey after Exercise" in {
      def contractKey(party: String, count: Long): Value =
        tuple2(Value().withParty(party), Value().withInt64(count))
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(
                  ContractId("cid2"),
                  contractKey = Some(contractKey("Alice", 2)),
                )
              ),
            ),
            TestData.Exercised(ContractId("cid2"), Seq()),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 2
      commands(0) shouldBe a[ExerciseCommand]
      commands(1) shouldBe an[ExerciseByKeyCommand]
    }
    "ExerciseByKey after CreateAndExercise" in {
      def contractKey(party: String, count: Long): Value =
        tuple2(Value().withParty(party), Value().withInt64(count))
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(
              ContractId("cid1"),
              contractKey = Some(contractKey("Alice", 1)),
            ),
            TestData.Exercised(
              ContractId("cid1"),
              Seq(
                TestData.Created(
                  ContractId("cid2"),
                  contractKey = Some(contractKey("Alice", 2)),
                )
              ),
            ),
            TestData.Exercised(ContractId("cid2"), Seq()),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 2
      commands(0) shouldBe a[CreateAndExerciseCommand]
      commands(1) shouldBe an[ExerciseByKeyCommand]
    }
    "adjacent create and exercise with key" in {
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(
              ContractId("cid1"),
              contractKey = Some(Value().withParty("Alice")),
            ),
            TestData.Exercised(ContractId("cid1"), Seq()),
          )
        )
        .toTransactionTree
      val commands = Command.fromTree(events)
      commands should have length 1
      commands(0) shouldBe a[CreateAndExerciseCommand]
    }
  }
}
