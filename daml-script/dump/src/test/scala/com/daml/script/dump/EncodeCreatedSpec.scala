// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.{Identifier, Record}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeCreatedSpec extends AnyFreeSpec with Matchers {
  import Encode._
  private def mkCreated(i: Int) =
    CreatedEvent(
      eventId = s"create$i",
      templateId = Some(Identifier("package", "Module", "Template")),
      contractId = s"cid$i",
      signatories = Seq("Alice"),
      createArguments = Some(
        Record(
          recordId = Some(Identifier("package", "Module", "Template")),
          fields = Seq.empty,
        )
      ),
    )
  "encodeCreatedEvent" - {
    "contract id bindings" - {
      "unreferenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val created = mkCreated(1)
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, Seq(created)).render(80) shouldBe
          """_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "referenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid1"))
        val created = mkCreated(1)
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, Seq(created)).render(80) shouldBe
          """contract_0_0 <- submitMulti [alice_0] [] do
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
        val events = Seq(mkCreated(1), mkCreated(2), mkCreated(3))
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """submitMulti [alice_0] [] do
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
        val events = Seq(mkCreated(1), mkCreated(2), mkCreated(3))
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """(contract_0_0, contract_0_2) <- submitMulti [alice_0] [] do
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
        val events = Seq(mkCreated(1), mkCreated(2))
        encodeSubmitCreatedEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
          """contract_0_1 <- submitMulti [alice_0] [] do
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
