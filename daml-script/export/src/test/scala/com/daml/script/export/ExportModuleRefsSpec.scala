// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.script.`export`.TestData
import com.daml.ledger.api.v1.value.{Identifier, Value, Variant}
import com.daml.script.`export`.TestData.Created
import com.daml.script.export.TreeUtils.moduleRefs
import com.google.protobuf
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ExportModuleRefsSpec extends AnyFreeSpec with Matchers {
  "module references" - {
    "handles empty case" in {
      val acs = TestData
        .ACS(Seq.empty[Created])
        .toACS
      val trees = Seq.empty[TransactionTree]
      val refs = moduleRefs(acs.values, trees)
      refs shouldBe empty
    }
    "merges ACS and trees references" in {
      val acs = TestData
        .ACS(
          Seq(
            TestData.Created(
              ContractId("acs1"),
              templateId = Identifier("package-a", "ModuleA", "TemplateA"),
            ),
            TestData.Created(
              ContractId("acs2"),
              templateId = Identifier("package-b", "ModuleB", "TemplateB"),
            ),
          )
        )
        .toACS
      val trees = Seq(
        TestData
          .Tree(
            Seq(
              TestData.Exercised(
                ContractId("acs2"),
                Seq(
                  TestData.Created(
                    ContractId("tree1"),
                    templateId = Identifier("package-c", "ModuleC", "TemplateC"),
                  )
                ),
                choiceArgument = Value().withVariant(
                  Variant(
                    Some(Identifier("package-d", "ModuleD", "ChoiceD")),
                    "ChoiceD",
                    Some(Value().withUnit(protobuf.empty.Empty())),
                  )
                ),
              )
            )
          ),
        TestData
          .Tree(
            Seq(
              TestData.Exercised(
                ContractId("tree1"),
                Seq.empty[TestData.Event],
                choiceArgument = Value().withVariant(
                  Variant(
                    Some(Identifier("package-e", "ModuleE", "ChoiceE")),
                    "ChoiceE",
                    Some(Value().withUnit(protobuf.empty.Empty())),
                  )
                ),
              )
            )
          ),
      ).map(_.toTransactionTree)
      val refs = moduleRefs(acs.values, trees)
      refs should contain only ("ModuleA", "ModuleB", "ModuleC", "ModuleD", "ModuleE")
    }
  }
}
