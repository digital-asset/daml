// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.{Identifier, Record}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeTreeSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeTree" - {
    "contract id bindings" - {
      "unreferenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            "create" -> TreeEvent(
              TreeEvent.Kind.Created(
                CreatedEvent(
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
              )
            )
          ),
          rootEventIds = Seq("create"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """tree <- submitTreeMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "referenced" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid"))
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            "create" -> TreeEvent(
              TreeEvent.Kind.Created(
                CreatedEvent(
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
              )
            )
          ),
          rootEventIds = Seq("create"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """tree <- submitTreeMulti [alice_0] [] do
            |  createCmd Module.Template
            |let contract_0_0 = createdCid @Module.Template [0] tree""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
  }
}
