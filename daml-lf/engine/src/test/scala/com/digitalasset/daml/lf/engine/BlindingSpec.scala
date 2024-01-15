// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.ImmArray
import com.daml.lf.engine.BlindingSpec.TxBuilder
import com.daml.lf.transaction.{BlindingInfo, Node}
import com.daml.lf.transaction.test.{NodeIdTransactionBuilder, TransactionBuilder, TestNodeBuilder}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ValueRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec

object BlindingSpec {
  class TxBuilder extends NodeIdTransactionBuilder with TestNodeBuilder
}

class BlindingSpec extends AnyFreeSpec with Matchers {

  import TransactionBuilder.Implicits._
  import TestNodeBuilder.CreateKey

  def create(builder: TxBuilder): (Value.ContractId, Node.Create) = {
    val cid = builder.newCid
    val create = builder.create(
      id = cid,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.Empty),
      signatories = Seq("Alice", "Bob"),
      observers = Seq("Carl"),
    )
    (cid, create)
  }

  "blind" - {
    // TEST_EVIDENCE: Confidentiality: ensure correct privacy for create node
    "create" in {
      val builder = new TxBuilder()
      val (_, createNode) = create(builder)
      val nodeId = builder.add(createNode)
      val tx = builder.build()
      val blindingInfo = Blinding.blind(tx)
      blindingInfo shouldBe BlindingInfo(
        disclosure = Map(nodeId -> Set("Alice", "Bob", "Carl")),
        divulgence = Map.empty,
      )
    }
    // TEST_EVIDENCE: Confidentiality: ensure correct privacy for exercise node (consuming)
    "consuming exercise" in {
      val builder = new TxBuilder()
      val (cid, createNode) = create(builder)
      val exercise = builder.exercise(
        createNode,
        "C",
        consuming = true,
        Set("Actor"),
        ValueRecord(None, ImmArray.Empty),
        choiceObservers = Set("ChoiceObserver"),
        byKey = false,
      )
      val nodeId = builder.add(exercise)
      val tx = builder.build()
      val blindingInfo = Blinding.blind(tx)
      blindingInfo shouldBe BlindingInfo(
        disclosure = Map(nodeId -> Set("ChoiceObserver", "Carl", "Bob", "Actor", "Alice")),
        divulgence = Map(cid -> Set("ChoiceObserver")),
      )
    }
    // TEST_EVIDENCE: Confidentiality: ensure correct privacy for exercise node (non-consuming)
    "non-consuming exercise" in {
      val builder = new TxBuilder()
      val (cid, createNode) = create(builder)
      val exercise = builder.exercise(
        createNode,
        "C",
        consuming = false,
        Set("Actor"),
        ValueRecord(None, ImmArray.empty),
        choiceObservers = Set("ChoiceObserver"),
        byKey = false,
      )
      val nodeId = builder.add(exercise)
      val tx = builder.build()
      val blindingInfo = Blinding.blind(tx)
      blindingInfo shouldBe BlindingInfo(
        disclosure = Map(nodeId -> Set("ChoiceObserver", "Bob", "Actor", "Alice")),
        divulgence = Map(cid -> Set("ChoiceObserver")),
      )
    }
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for fetch node
  "fetch" in {
    val builder = new TxBuilder()
    val (_, createNode) = create(builder)
    val fetch = builder.fetch(createNode, byKey = false)
    val nodeId = builder.add(fetch)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(nodeId -> Set("Bob", "Alice")),
      divulgence = Map.empty,
    )
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for lookup-by-key node (found)
  "lookupByKey found" in {
    val builder = new TxBuilder()
    val cid = builder.newCid
    val create = builder.create(
      id = cid,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("Alice", "Bob"),
      observers = Seq("Carl"),
      key = CreateKey.KeyWithMaintainers(ValueRecord(None, ImmArray.empty), Seq("Alice")),
    )
    val lookup = builder.lookupByKey(create)
    val nodeId = builder.add(lookup)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(nodeId -> Set("Alice")),
      divulgence = Map.empty,
    )
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for lookup-by-key node (not-found)
  "lookupByKey not found" in {
    val builder = new TxBuilder()
    val cid = builder.newCid
    val create = builder.create(
      id = cid,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("Alice", "Bob"),
      observers = Seq("Carl"),
      key = CreateKey.KeyWithMaintainers(ValueRecord(None, ImmArray.empty), Seq("Alice")),
    )
    val lookup = builder.lookupByKey(create, found = false)
    val nodeId = builder.add(lookup)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(nodeId -> Set("Alice")),
      divulgence = Map.empty,
    )
  }

  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for exercise subtree
  "exercise with children" in {
    val builder = new TxBuilder()
    val cid1 = builder.newCid
    val cid2 = builder.newCid
    val cid3 = builder.newCid
    val cid4 = builder.newCid
    val create1 = builder.create(
      id = cid1,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("A"),
      observers = Seq(),
    )
    val create2 = builder.create(
      id = cid2,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("B"),
      observers = Seq(),
    )
    val create3 = builder.create(
      id = cid3,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("C"),
      observers = Seq(),
    )
    val create4 = builder.create(
      id = cid4,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("D"),
      observers = Seq(),
    )
    val ex1 =
      builder.add(
        builder.exercise(
          create1,
          "C",
          consuming = true,
          Set("A"),
          ValueRecord(None, ImmArray.empty),
          byKey = false,
        )
      )
    val c2Id = builder.add(create2, ex1)
    val ex2 = builder.add(
      builder.exercise(
        create2,
        "C",
        consuming = true,
        Set("B"),
        ValueRecord(None, ImmArray.empty),
        byKey = false,
      ),
      ex1,
    )
    val c3Id = builder.add(create3, ex2)
    val c4Id = builder.add(create4, ex2)
    val tx = builder.build()
    val blindingInfo = Blinding.blind(tx)
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(
        ex1 -> Set("A"),
        c2Id -> Set("A", "B"),
        ex2 -> Set("A", "B"),
        c3Id -> Set("A", "B", "C"),
        c4Id -> Set("A", "B", "D"),
      ),
      divulgence = Map(
        create2.coid -> Set("A")
      ),
    )
  }
  // TEST_EVIDENCE: Confidentiality: ensure correct privacy for rollback subtree
  "rollback" in {
    val builder = new TxBuilder()
    val cid1 = builder.newCid
    val cid2 = builder.newCid
    val create1 = builder.create(
      id = cid1,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("A", "B"),
      observers = Seq(),
    )
    val ex1 = builder.add(
      builder.exercise(
        create1,
        "Choice",
        consuming = true,
        Set("C"),
        ValueRecord(None, ImmArray.empty),
        byKey = false,
      )
    )
    val rollback = builder.add(builder.rollback(), ex1)
    val create2 = builder.create(
      id = cid2,
      templateId = "M:T",
      argument = ValueRecord(None, ImmArray.empty),
      signatories = Seq("D"),
      observers = Seq(),
    )
    val create2Node = builder.add(create2, rollback)
    val ex2 = builder.add(
      builder
        .exercise(
          contract = create2,
          "Choice",
          consuming = true,
          Set("F"),
          ValueRecord(None, ImmArray.empty),
          byKey = false,
        ),
      rollback,
    )
    val blindingInfo = Blinding.blind(builder.build())
    blindingInfo shouldBe BlindingInfo(
      disclosure = Map(
        ex1 -> Set("A", "B", "C"),
        rollback -> Set("A", "B", "C"),
        create2Node -> Set("A", "B", "C", "D"),
        ex2 -> Set("A", "B", "C", "D", "F"),
      ),
      divulgence = Map(
        cid2 -> Set("A", "B", "C")
      ),
    )
  }
}
