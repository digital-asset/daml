// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.ledger.api.v1.{value => v}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scalaz.std.iterable._
import scalaz.std.set._
import scalaz.syntax.foldable._

class CmdReferencedCidsSpec extends AnyFreeSpec with Matchers {
  import TreeUtils._
  "cmdReferencedCids" - {
    "empty" in {
      val (cmds, _) = TestData.Tree(Seq()).toCommands
      cmds.foldMap(cmdReferencedCids) shouldBe Set.empty
    }
    "created only" in {
      val (cmds, _) = TestData.Tree(Seq(TestData.Created(ContractId("cid")))).toCommands
      cmds.foldMap(cmdReferencedCids) shouldBe Set.empty
    }
    "exercised" in {
      val (cmds, _) = TestData
        .Tree(
          Seq(
            TestData.Exercised(
              ContractId("cid"),
              Seq(),
            )
          )
        )
        .toCommands
      cmds.foldMap(cmdReferencedCids) shouldBe Set("cid")
    }
    "createAndExercise" in {
      val (cmds, _) = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid")),
            TestData.Exercised(
              ContractId("cid"),
              Seq(),
            ),
          )
        )
        .toCommands
      cmds.foldMap(cmdReferencedCids) shouldBe Set.empty
    }
    "referenced" in {
      val variant = v.Value(
        v.Value.Sum.Variant(
          v.Variant(
            variantId = Some(v.Identifier("package", "Module", "variant")),
            constructor = "Variant",
            value = Some(v.Value(v.Value.Sum.ContractId("cid_variant"))),
          )
        )
      )
      val list = v.Value(v.Value.Sum.List(v.List(Seq(v.Value(v.Value.Sum.ContractId("cid_list"))))))
      val optional = v.Value(
        v.Value.Sum.Optional(v.Optional(Some(v.Value(v.Value.Sum.ContractId("cid_optional")))))
      )
      val map = v.Value(
        v.Value.Sum.Map(
          v.Map(Seq(v.Map.Entry("entry", Some(v.Value(v.Value.Sum.ContractId("cid_map"))))))
        )
      )
      val genmap = v.Value(
        v.Value.Sum.GenMap(
          v.GenMap(
            Seq(
              v.GenMap.Entry(
                key = Some(v.Value(v.Value.Sum.ContractId("cid_genmap_key"))),
                value = Some(v.Value(v.Value.Sum.ContractId("cid_genmap_value"))),
              )
            )
          )
        )
      )
      val record = v.Value(
        v.Value.Sum.Record(
          v.Record(
            recordId = Some(v.Identifier("package", "Module", "record")),
            fields = Seq(
              v.RecordField("variant", Some(variant)),
              v.RecordField("list", Some(list)),
              v.RecordField("optional", Some(optional)),
              v.RecordField("map", Some(map)),
              v.RecordField("genmap", Some(genmap)),
            ),
          )
        )
      )
      val (cmds, _) = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(
              ContractId("cid_create"),
              Seq(v.RecordField("contract_id", Some(v.Value().withContractId("cid_create_arg")))),
            ),
            TestData.Exercised(
              ContractId("cid_exercise"),
              Seq.empty[TestData.Event],
              TestData.Choice(choiceArgument = Some(record)),
            ),
          )
        )
        .toCommands
      cmds.foldMap(cmdReferencedCids) shouldBe Set(
        "cid_create_arg",
        "cid_exercise",
        "cid_variant",
        "cid_list",
        "cid_optional",
        "cid_map",
        "cid_genmap_key",
        "cid_genmap_value",
      )
    }
    "only referenced internally" in {
      val (cmds, _) = TestData
        .Tree(
          Seq(
            TestData.Exercised(
              ContractId("cid_exercise_outer"),
              Seq(
                TestData.Exercised(
                  ContractId("cid_exercise_inner"),
                  Seq.empty[TestData.Event],
                )
              ),
            )
          )
        )
        .toCommands
      cmds.foldMap(cmdReferencedCids) shouldBe Set(
        "cid_exercise_outer"
      )
    }
  }
}
