// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      val scriptExport =
        Export.fromTransactionTrees(acs, trees, Map.empty, acsBatchSize = 10, setTime = false)
      scriptExport.cidRefs shouldBe empty
      scriptExport.cidMap shouldBe empty
      scriptExport.unknownCids shouldBe empty
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
      val scriptExport =
        Export.fromTransactionTrees(acs, trees, Map.empty, acsBatchSize = 10, setTime = false)
      scriptExport.cidRefs should contain only (
        ContractId("acs1"),
        ContractId("acs2"),
        ContractId("tree1"),
      )
      scriptExport.cidMap should contain only (
        ContractId("acs1") -> "template_0_0",
        ContractId("acs2") -> "template_0_1",
        ContractId("tree1") -> "template_1_0",
      )
      scriptExport.unknownCids shouldBe empty
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
    val scriptExport =
      Export.fromTransactionTrees(acs, trees, Map.empty, acsBatchSize = 10, setTime = false)
    scriptExport.cidRefs should contain only (ContractId("un1"), ContractId("un2"))
    scriptExport.cidMap shouldBe empty
    scriptExport.unknownCids should contain only (ContractId("un1"), ContractId("un2"))
  }
}
