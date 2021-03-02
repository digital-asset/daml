// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeTreeSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeTree" - {
    "contract id bindings" - {
      "unreferenced create" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val tree = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1"))
            )
          )
          .toTransactionTree
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """_ <- submit alice_0 do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "unreferenced creates" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set.empty[ContractId]
        val tree = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
            )
          )
          .toTransactionTree
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """submit alice_0 do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()""".stripMargin.replace("\r\n", "\n")
      }
      "unreferenced exercise" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid0") -> "contract_0_0",
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid1") -> "contract_1_1",
        )
        val cidRefs = Set.empty[ContractId]
        val tree = TestData
          .Tree(
            Seq(
              TestData.Exercised(
                ContractId("cid0"),
                Seq(
                  TestData.Created(ContractId("cid1"))
                ),
              )
            )
          )
          .toTransactionTree
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  exerciseCmd contract_0_0 (Module.Choice ())""".stripMargin.replace("\r\n", "\n")
      }
      "referenced create" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid1"))
        val tree = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1"))
            )
          )
          .toTransactionTree
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """contract_0_0 <- submit alice_0 do
            |  createCmd Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "referenced creates" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid2"))
        val tree = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
            )
          )
          .toTransactionTree
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """(contract_1_0, contract_1_1) <- submit alice_0 do
            |  contract_1_0 <- createCmd Module.Template
            |  contract_1_1 <- createCmd Module.Template
            |  pure (contract_1_0, contract_1_1)""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "referenced exercise" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid0") -> "contract_0_0",
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid2"))
        val tree = TestData
          .Tree(
            Seq(
              TestData.Exercised(
                ContractId("cid0"),
                Seq(
                  TestData.Created(ContractId("cid1")),
                  TestData.Created(ContractId("cid2")),
                ),
              )
            )
          )
          .toTransactionTree
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  exerciseCmd contract_0_0 (Module.Choice ())
            |let contract_1_0 = createdCid @Module.Template [0, 0] tree
            |let contract_1_1 = createdCid @Module.Template [0, 1] tree""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
  }
}
