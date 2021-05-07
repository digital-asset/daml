// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.ledger.api.v1.{value => v}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ExportCidRefsSpec extends AnyFreeSpec with Matchers {
  "contract id references" - {
    "unreferenced" in {
      val acs = TestData
        .ACS(
          Seq(
            TestData.Created(ContractId("acs1")),
            TestData.Created(ContractId("acs2")),
          )
        )
        .toACS
      val trees = Seq(
        TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("tree1"))
            )
          ),
        TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("tree2"))
            )
          ),
      ).map(_.toTransactionTree)
      val export = Export.fromTransactionTrees(acs, trees, acsBatchSize = 10, setTime = false)
      export.cidRefs shouldBe empty
      export.cidMap shouldBe empty
      export.unknownCids shouldBe empty
    }
    "referenced" in {
      val acs = TestData
        .ACS(
          Seq(
            TestData.Created(ContractId("acs1")),
            TestData.Created(
              ContractId("acs2"),
              createArguments = Seq(
                v.RecordField()
                  .withLabel("_1")
                  .withValue(v.Value().withContractId("acs1"))
              ),
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
                Seq(TestData.Created(ContractId("tree1"))),
              )
            )
          ),
        TestData
          .Tree(
            Seq(
              TestData.Exercised(ContractId("tree1"), Seq.empty)
            )
          ),
      ).map(_.toTransactionTree)
      val export = Export.fromTransactionTrees(acs, trees, acsBatchSize = 10, setTime = false)
      export.cidRefs should contain only (
        ContractId("acs1"),
        ContractId("acs2"),
        ContractId("tree1"),
      )
      export.cidMap should contain only (
        ContractId("acs1") -> "template_0_0",
        ContractId("acs2") -> "template_0_1",
        ContractId("tree1") -> "template_1_0",
      )
      export.unknownCids shouldBe empty
    }
  }
  "unknown" in {
    val acs = TestData
      .ACS(
        Seq(
          TestData.Created(
            ContractId("acs1"),
            createArguments = Seq(
              v.RecordField()
                .withLabel("_1")
                .withValue(v.Value().withContractId("un1"))
            ),
          )
        )
      )
      .toACS
    val trees = Seq(
      TestData
        .Tree(
          Seq(
            TestData.Exercised(
              ContractId("un2"),
              Seq(TestData.Created(ContractId("tree1"))),
            )
          )
        )
    ).map(_.toTransactionTree)
    val export = Export.fromTransactionTrees(acs, trees, acsBatchSize = 10, setTime = false)
    export.cidRefs should contain only (ContractId("un1"), ContractId("un2"))
    export.cidMap shouldBe empty
    export.unknownCids should contain only (ContractId("un1"), ContractId("un2"))
  }
}
