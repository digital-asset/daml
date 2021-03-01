// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeAcsSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeACS" - {
    "command batching" - {
      val parties = Map(Party("Alice") -> "alice_0")
      val cidMap = Map(
        ContractId("cid1") -> "contract_0_0",
        ContractId("cid2") -> "contract_0_1",
        ContractId("cid3") -> "contract_0_2",
        ContractId("cid4") -> "contract_0_3",
      )
      val cidRefs = Set.empty[ContractId]
      val events = TestData
        .ACS(
          Seq(
            TestData.Created(ContractId("cid1")),
            TestData.Created(ContractId("cid2")),
            TestData.Created(ContractId("cid3")),
            TestData.Created(ContractId("cid4")),
          )
        )
        .toCreatedEvents
      "batch size 1" in {
        encodeACS(parties, cidMap, cidRefs, events, 1).render(80) shouldBe
          """_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template
            |_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template
            |_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template
            |_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "batch size 2 - divides evenly" in {
        encodeACS(parties, cidMap, cidRefs, events, 2).render(80) shouldBe
          """submitMulti [alice_0] [] do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()
            |submitMulti [alice_0] [] do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "batch size 3 - does not divide evenly" in {
        encodeACS(parties, cidMap, cidRefs, events, 3).render(80) shouldBe
          """submitMulti [alice_0] [] do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()
            |_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "batch size 4 - equals total size" in {
        encodeACS(parties, cidMap, cidRefs, events, 4).render(80) shouldBe
          """submitMulti [alice_0] [] do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "batch size 5 - greater than total size" in {
        encodeACS(parties, cidMap, cidRefs, events, 5).render(80) shouldBe
          """submitMulti [alice_0] [] do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
  }
}
