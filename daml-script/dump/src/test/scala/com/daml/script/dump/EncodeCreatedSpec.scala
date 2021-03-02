// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeCreatedSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeCreatedEvent" - {
    "multi party submissions" in {
      val parties = Map(
        Party("Alice") -> "alice_0",
        Party("Bob") -> "bob_0",
      )
      val cidMap = Map(
        ContractId("cid1") -> "contract_0_0",
        ContractId("cid2") -> "contract_0_1",
      )
      val cidRefs = Set.empty[ContractId]
      val events = TestData
        .ACS(
          Seq(
            TestData.Created(ContractId("cid1"), submitters = Seq(Party("Alice"))),
            TestData.Created(ContractId("cid2"), submitters = Seq(Party("Bob"))),
          )
        )
        .toCreatedEvents
      encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
        """submitMulti [alice_0, bob_0] [] do
          |  _ <- createCmd Module.Template
          |  _ <- createCmd Module.Template
          |  pure ()""".stripMargin.replace(
          "\r\n",
          "\n",
        )
    }
    "contract id bindings" - {
      "unreferenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val events = TestData
          .ACS(
            Seq(
              TestData.Created(ContractId("cid1"))
            )
          )
          .toCreatedEvents
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """_ <- submit alice_0 do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "referenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid1"))
        val events = TestData
          .ACS(
            Seq(
              TestData.Created(ContractId("cid1"))
            )
          )
          .toCreatedEvents
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """contract_0_0 <- submit alice_0 do
            |  createCmd Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "multiple unreferenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0",
          ContractId("cid2") -> "contract_0_1",
          ContractId("cid3") -> "contract_0_2",
        )
        val cidRefs = Set.empty[ContractId]
        val events = TestData
          .ACS(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
              TestData.Created(ContractId("cid3")),
            )
          )
          .toCreatedEvents
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """submit alice_0 do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "multiple referenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0",
          ContractId("cid2") -> "contract_0_1",
          ContractId("cid3") -> "contract_0_2",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid3"))
        val events = TestData
          .ACS(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
              TestData.Created(ContractId("cid3")),
            )
          )
          .toCreatedEvents
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """(contract_0_0, contract_0_2) <- submit alice_0 do
            |  contract_0_0 <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  contract_0_2 <- createCmd Module.Template
            |  pure (contract_0_0, contract_0_2)""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "last referenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0",
          ContractId("cid2") -> "contract_0_1",
        )
        val cidRefs = Set(ContractId("cid2"))
        val events = TestData
          .ACS(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
            )
          )
          .toCreatedEvents
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """contract_0_1 <- submit alice_0 do
            |  _ <- createCmd Module.Template
            |  contract_0_1 <- createCmd Module.Template
            |  pure contract_0_1""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
  }
}
