// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.{value => v}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class TreeReferencedCidsSpec extends AnyFreeSpec with Matchers {
  import TreeUtils._
  "treeReferencedCids" - {
    "empty" in {
      treeReferencedCids(
        TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map.empty,
          rootEventIds = Seq.empty,
          traceContext = None,
        )
      ) shouldBe Set.empty
    }
    "created only" in {
      treeReferencedCids(
        TransactionTree(
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
                  contractId = "cid",
                )
              )
            )
          ),
          rootEventIds = Seq("create"),
          traceContext = None,
        )
      ) shouldBe Set.empty
    }
    "exercised" in {
      treeReferencedCids(
        TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            "exercise" -> TreeEvent(
              TreeEvent.Kind.Exercised(
                ExercisedEvent(
                  eventId = "exercise",
                  contractId = "cid",
                )
              )
            )
          ),
          rootEventIds = Seq("exercise"),
          traceContext = None,
        )
      ) shouldBe Set("cid")
    }
    "referenced" in {
      val variant = v.Value(
        v.Value.Sum.Variant(
          v.Variant(
            variantId = Some(v.Identifier("package", "Module", "variant")),
            constructor = "Variant",
            value = Some(v.Value(v.Value.Sum.ContractId("cid_variant"))),
          )
        )
      )
      val list = v.Value(v.Value.Sum.List(v.List(Seq(v.Value(v.Value.Sum.ContractId("cid_list"))))))
      val optional = v.Value(
        v.Value.Sum.Optional(v.Optional(Some(v.Value(v.Value.Sum.ContractId("cid_optional")))))
      )
      val map = v.Value(
        v.Value.Sum.Map(
          v.Map(Seq(v.Map.Entry("entry", Some(v.Value(v.Value.Sum.ContractId("cid_map"))))))
        )
      )
      val genmap = v.Value(
        v.Value.Sum.GenMap(
          v.GenMap(
            Seq(
              v.GenMap.Entry(
                key = Some(v.Value(v.Value.Sum.ContractId("cid_genmap_key"))),
                value = Some(v.Value(v.Value.Sum.ContractId("cid_genmap_value"))),
              )
            )
          )
        )
      )
      val record = v.Value(
        v.Value.Sum.Record(
          v.Record(
            recordId = Some(v.Identifier("package", "Module", "record")),
            fields = Seq(
              v.RecordField("variant", Some(variant)),
              v.RecordField("list", Some(list)),
              v.RecordField("optional", Some(optional)),
              v.RecordField("map", Some(map)),
              v.RecordField("genmap", Some(genmap)),
            ),
          )
        )
      )
      treeReferencedCids(
        TransactionTree(
          transactionId = "txid",
          commandId = "cmdid",
          workflowId = "flowid",
          effectiveAt = None,
          offset = "",
          eventsById = Map(
            "exercise" -> TreeEvent(
              TreeEvent.Kind.Exercised(
                ExercisedEvent(
                  eventId = "exercise",
                  contractId = "cid_exercise",
                  choiceArgument = Some(record),
                )
              )
            )
          ),
          rootEventIds = Seq("exercise"),
          traceContext = None,
        )
      ) shouldBe Set(
        "cid_exercise",
        "cid_variant",
        "cid_list",
        "cid_optional",
        "cid_map",
        "cid_genmap_key",
        "cid_genmap_value",
      )
    }
  }
}
