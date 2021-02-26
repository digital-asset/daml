// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.{Identifier, Record, Value, Variant}
import com.google.protobuf
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeTreeSpec extends AnyFreeSpec with Matchers {
  import Encode._
  private def mkCreated(i: Int) = {
    s"create$i" -> TreeEvent(
      TreeEvent.Kind.Created(
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
      )
    )
  }
  private def mkExercised(i: Int, cid: ContractId, result: Value, childEventIds: Seq[String]) = {
    s"exercise$i" -> TreeEvent(
      TreeEvent.Kind.Exercised(
        ExercisedEvent(
          eventId = s"exercise$i",
          templateId = Some(Identifier("package", "Module", "Template")),
          contractId = ContractId.unwrap(cid),
          actingParties = Seq("Alice"),
          choice = "Choice",
          choiceArgument = Some(
            Value()
              .withVariant(
                Variant(
                  Some(Identifier("package", "Module", "Choice")),
                  "Choice",
                  Some(Value().withUnit(protobuf.empty.Empty())),
                )
              )
          ),
          childEventIds = childEventIds,
          exerciseResult = Some(result),
        )
      )
    )
  }

  "encodeTree" - {
    "contract id bindings" - {
      "unreferenced create" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            mkCreated(1)
          ),
          rootEventIds = Seq("create1"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """_ <- submitMulti [alice_0] [] do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "unreferenced creates" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set.empty[ContractId]
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            mkCreated(1),
            mkCreated(2),
          ),
          rootEventIds = Seq("create1", "create2"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """submitMulti [alice_0] [] do
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
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            mkCreated(1),
            mkExercised(2, ContractId("cid0"), Value().withContractId("cid2"), Seq("create1")),
          ),
          rootEventIds = Seq("exercise2"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """tree <- submitTreeMulti [alice_0] [] do
            |  exerciseCmd contract_0_0 (Module.Choice ())""".stripMargin.replace("\r\n", "\n")
      }
      "referenced create" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid1"))
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            mkCreated(1)
          ),
          rootEventIds = Seq("create1"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """contract_0_0 <- submitMulti [alice_0] [] do
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
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            mkCreated(1),
            mkCreated(2),
          ),
          rootEventIds = Seq("create1", "create2"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """(contract_1_0, contract_1_1) <- submitMulti [alice_0] [] do
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
        val tree = TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            mkCreated(1),
            mkCreated(2),
            mkExercised(
              3,
              ContractId("cid0"),
              Value().withContractId("cid2"),
              Seq("create1", "create2"),
            ),
          ),
          rootEventIds = Seq("exercise3"),
          traceContext = None,
        )
        encodeTree(parties, cidMap, cidRefs, tree).render(80) shouldBe
          """tree <- submitTreeMulti [alice_0] [] do
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
