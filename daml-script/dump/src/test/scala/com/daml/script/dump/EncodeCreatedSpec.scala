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
  "encodeCreatedEvent" - {
    "contract id bindings" - {
      "unreferenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val created = CreatedEvent(
          eventId = "create",
          templateId = Some(Identifier("package", "Module", "Template")),
          contractId = "cid",
          signatories = Seq("Alice"),
          createArguments = Some(
            Record(
              recordId = Some(Identifier("package", "Module", "Template")),
              fields = Seq.empty,
            )
          ),
        )
        encodeCreatedEvent(parties, cidMap, cidRefs, created).render(80) shouldBe
          """submitMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "referenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid"))
        val created = CreatedEvent(
          eventId = "create",
          templateId = Some(Identifier("package", "Module", "Template")),
          contractId = "cid",
          signatories = Seq("Alice"),
          createArguments = Some(
            Record(
              recordId = Some(Identifier("package", "Module", "Template")),
              fields = Seq.empty,
            )
          ),
        )
        encodeCreatedEvent(parties, cidMap, cidRefs, created).render(80) shouldBe
          """contract_0_0 <- submitMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
  }
}
