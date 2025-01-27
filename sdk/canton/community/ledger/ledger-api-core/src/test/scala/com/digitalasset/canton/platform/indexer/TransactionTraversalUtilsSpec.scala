// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.indexer.TransactionTraversalUtils.{
  NodeInfo,
  arrangeNodeIdsInExecutionOrder,
  executionOrderTraversalForIngestion,
}
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.digitalasset.daml.lf.transaction.test.{
  NodeIdTransactionBuilder,
  TestNodeBuilder,
  TransactionBuilder,
}
import com.digitalasset.daml.lf.transaction.{Node, NodeId, Transaction}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random

class TransactionTraversalUtilsSpec extends AnyFlatSpec with BaseTest {
  import TransactionBuilder.Implicits.*

  val shuffles = 100

  object TxBuilder {
    def apply(): NodeIdTransactionBuilder & TestNodeBuilder = new NodeIdTransactionBuilder
      with TestNodeBuilder
  }

  behavior of "executionOrderTraversalForIngestion"

  it should "handle a single create node" in {
    val builder = TxBuilder()
    val createNode = create("", builder)
    builder.add(createNode)
    val transaction = builder.build().transaction

    verifyNodeIdsAreInExecutionOrder(transaction)

    executionOrderTraversalForIngestion(transaction).toSeq shouldBe
      Seq(
        NodeInfo(NodeId(0), createNode, NodeId(0))
      )
  }

  it should "handle a single exercise node" in {
    val builder = TxBuilder()
    val exerciseNode = {
      val createNode = create("", builder)
      exercise("someChoice", createNode, builder)
    }
    builder.add(exerciseNode)
    val transaction = builder.build().transaction

    verifyNodeIdsAreInExecutionOrder(transaction)

    executionOrderTraversalForIngestion(transaction).toSeq shouldBe
      Seq(
        NodeInfo(NodeId(0), exerciseNode, NodeId(0))
      )
  }

  it should "handle nested exercise nodes" in {
    // Previous transaction
    // └─ #0 Create
    // Transaction
    // └─ #0 Exercise (choice A)    (last descendant: #2)
    //    ├─ #1 Exercise (choice B) (last descendant: #1)
    //    └─ #2 Exercise (choice C) (last descendant: #2)
    val builder = TxBuilder()
    val createNode0 = create("0", builder)
    val exerciseNodeA = exercise("A", createNode0, builder)
    val exerciseNodeB = exercise("B", createNode0, builder)
    val exerciseNodeC = exercise("C", createNode0, builder)
    val exerciseNodeAId = builder.add(exerciseNodeA)
    builder.add(exerciseNodeB, exerciseNodeAId)
    builder.add(exerciseNodeC, exerciseNodeAId)
    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 2,
      1 -> 1,
      2 -> 2,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  it should "handle nested exercise and create nodes (right weighted execution order)" in {
    // Previous transactions
    // └─ #0 Create
    // Transaction
    // └─ #0 Exercise (choice A)     (last descendant: #3)
    //    ├─ #1 Create A             (last descendant: #1)
    //    └─ #2 Exercise (choice B)  (last descendant: #3)
    //       └─ #3 Create B          (last descendant: #3)
    val builder = TxBuilder()
    val createNode0 = create("0", builder)
    val exerciseNodeA = exercise("A", createNode0, builder)
    val createNodeA = create("A", builder)
    val exerciseNodeB = exercise("B", createNodeA, builder)
    val createNodeB = create("B", builder)

    val exerciseNodeAId = builder.add(exerciseNodeA)
    builder.add(createNodeA, exerciseNodeAId)
    val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
    builder.add(createNodeB, exerciseNodeBId)
    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 3,
      1 -> 1,
      2 -> 3,
      3 -> 3,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  it should "handle nested exercise and create nodes (left weighted execution order)" in {
    // Previous transactions
    // └─ #0 Create
    // Transaction
    // └─ #0 Exercise (choice A)     (last descendant: #3)
    //    ├─ #1 Exercise (choice B)  (last descendant: #2)
    //    │  └─ #2 Create B          (last descendant: #2)
    //    └─ #3 Create A             (last descendant: #3)
    val builder = TxBuilder()
    val createNode0 = create("0", builder)
    val exerciseNodeA = exercise("A", createNode0, builder)
    val exerciseNodeB = exercise("B", createNode0, builder)
    val createNodeB = create("B", builder)
    val createNodeA = create("A", builder)
    val exerciseNodeAId = builder.add(exerciseNodeA)
    val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
    builder.add(createNodeB, exerciseNodeBId)
    builder.add(createNodeA, exerciseNodeAId)
    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 3,
      1 -> 2,
      2 -> 2,
      3 -> 3,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  it should "handle fetch and lookup nodes (they should not appear in output)" in {
    // Previous transaction
    // └─ #0 Create
    // Transaction
    // └─ #0 Exercise (choice A)    (last descendant: #5)
    //    ├─ #1 Exercise (choice B) (last descendant: #2)
    //    │  └─ #2 Fetch B          (should not appear)
    //    ├─ #3 Lookup A            (should not appear)
    //    ├─ #4 Exercise (choice C) (last descendant: #4)
    //    └─ #5 Fetch A             (should not appear)
    val builder = TxBuilder()
    val createNode0 = create("0", builder, withKey = true)
    val exerciseNodeA = exercise("A", createNode0, builder)
    val exerciseNodeB = exercise("B", createNode0, builder)
    val fetchNodeB = builder.fetch(createNode0, byKey = true)
    val lookupNodeA = builder.lookupByKey(createNode0)
    val exerciseNodeC = exercise("C", createNode0, builder)
    val fetchNodeA = builder.fetch(createNode0, byKey = false)

    val exerciseNodeAId = builder.add(exerciseNodeA)
    val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
    builder.add(fetchNodeB, exerciseNodeBId)
    builder.add(lookupNodeA, exerciseNodeAId)
    builder.add(exerciseNodeC, exerciseNodeAId)
    builder.add(fetchNodeA, exerciseNodeAId)
    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 5,
      1 -> 2,
      4 -> 4,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  it should "handle rollback nodes (they and their descendants should not appear in output)" in {
    // Previous transaction
    // └─ #0 Create
    // Transaction
    // └─ #0 Exercise (choice A)             (last descendant: #1)
    //    ├─ #1 Rollback A                   (should not appear)
    //    │  └─ #2 Exercise (choice B)       (should not appear)
    //    │     ├─ #3 Rollback B             (should not appear)
    //    │     │  └─ #4 Exercise (choice C) (should not appear)
    //    │     └─ #5 Create B               (should not appear)
    //    └─ #6 Exercise (choice D)          (last descendant: #6)
    val builder = TxBuilder()
    val createNode0 = create("0", builder, withKey = true)
    val exerciseNodeA = exercise("A", createNode0, builder)
    val rollbackNodeA = builder.rollback()
    val exerciseNodeB = exercise("B", createNode0, builder)
    val rollbackNodeB = builder.rollback()
    val exerciseNodeC = exercise("C", createNode0, builder)
    val createNodeB = create("B", builder)
    val exerciseNodeD = exercise("D", createNode0, builder)

    val exerciseNodeAId = builder.add(exerciseNodeA)
    val rollbackNodeAId = builder.add(rollbackNodeA, exerciseNodeAId)
    val exerciseNodeBId = builder.add(exerciseNodeB, rollbackNodeAId)
    val rollbackNodeBId = builder.add(rollbackNodeB, exerciseNodeBId)
    builder.add(exerciseNodeC, rollbackNodeBId)
    builder.add(createNodeB, exerciseNodeBId)
    builder.add(exerciseNodeD, exerciseNodeAId)

    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 6,
      6 -> 6,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  it should "handle complex combinations of nested exercise and create nodes" in {
    // Previous transactions
    // └─ #0 Create
    // Transaction
    // └─ #0 Exercise (choice A)          (last descendant: #9)
    //    ├─ #1 Create A1                 (last descendant: #1)
    //    ├─ #2 Create A2                 (last descendant: #2)
    //    └─ #3 Exercise (choice B)       (last descendant: #9)
    //       ├─ #4 Create B1              (last descendant: #4)
    //       ├─ #5 Exercise (choice C)    (last descendant: #8)
    //       │  ├─ #6 Create C1           (last descendant: #6)
    //       │  └─ #7 Exercise (choice D) (last descendant: #8)
    //       │     └─ #8 Create D1        (last descendant: #8)
    //       └─ #9 Create B2              (last descendant: #9)

    val builder = TxBuilder()
    val createNode0 = create("0", builder)
    val exerciseNodeA = exercise("A", createNode0, builder)
    val createNodeA1 = create("A1", builder)
    val createNodeA2 = create("A2", builder)
    val exerciseNodeB = exercise("B", createNode0, builder)
    val createNodeB1 = create("B1", builder)
    val exerciseNodeC = exercise("C", createNode0, builder)
    val createNodeC1 = create("C1", builder)
    val exerciseNodeD = exercise("D", createNode0, builder)
    val createNodeD1 = create("D1", builder)
    val createNodeB2 = create("B2", builder)

    val exerciseNodeAId = builder.add(exerciseNodeA)

    builder.add(createNodeA1, exerciseNodeAId)
    builder.add(createNodeA2, exerciseNodeAId)
    val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
    builder.add(createNodeB1, exerciseNodeBId)
    val exerciseNodeCId = builder.add(exerciseNodeC, exerciseNodeBId)
    builder.add(createNodeC1, exerciseNodeCId)
    val exerciseNodeDId = builder.add(exerciseNodeD, exerciseNodeCId)
    builder.add(createNodeD1, exerciseNodeDId)
    builder.add(createNodeB2, exerciseNodeBId)

    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 9,
      1 -> 1,
      2 -> 2,
      3 -> 9,
      4 -> 4,
      5 -> 8,
      6 -> 6,
      7 -> 8,
      8 -> 8,
      9 -> 9,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  it should "handle complex combinations with multiple root nodes" in {
    // Previous transactions
    // └─ #0 Create
    // Transaction
    // ├─ #0 Create A1                 (last descendant: #0)
    // ├─ #1 Create A2                 (last descendant: #1)
    // ├─ #2 Exercise (choice B)       (last descendant: #7)
    // │  ├─ #3 Exercise (choice C)    (last descendant: #6)
    // │  │  ├─ #4 Exercise (choice D) (last descendant: #5)
    // │  │  │  └─ #5 Create D1        (last descendant: #5)
    // │  │  └─ #6 Create C1           (last descendant: #6)
    // │  └─ #7 Create B1              (last descendant: #7)
    // └─ #8 Create A3                 (last descendant: #8)
    val builder = TxBuilder()
    val createNode0 = create("0", builder)
    val createNodeA1 = create("A1", builder)
    val createNodeA2 = create("A2", builder)
    val exerciseNodeB = exercise("B", createNode0, builder)
    val exerciseNodeC = exercise("C", createNode0, builder)
    val exerciseNodeD = exercise("D", createNode0, builder)
    val createNodeD1 = create("D1", builder)
    val createNodeC1 = create("C1", builder)
    val createNodeB1 = create("B1", builder)
    val createNodeA3 = builder.create(
      id = builder.newCid,
      templateId = "M:TA3",
      argument = Value.ValueUnit,
      signatories = List("signatory"),
      observers = List("observer"),
    )

    builder.add(createNodeA1)
    builder.add(createNodeA2)
    val exerciseNodeBId = builder.add(exerciseNodeB)
    val exerciseNodeCId = builder.add(exerciseNodeC, exerciseNodeBId)
    val exerciseNodeDId = builder.add(exerciseNodeD, exerciseNodeCId)
    builder.add(createNodeD1, exerciseNodeDId)
    builder.add(createNodeC1, exerciseNodeCId)
    builder.add(createNodeB1, exerciseNodeBId)
    builder.add(createNodeA3)

    val transaction = builder.build().transaction

    val expectedNodeInfos = Seq(
      // node id, last descendant
      0 -> 0,
      1 -> 1,
      2 -> 7,
      3 -> 6,
      4 -> 5,
      5 -> 5,
      6 -> 6,
      7 -> 7,
      8 -> 8,
    ).map { case (id, lastDescendant) =>
      NodeInfo(NodeId(id), transaction.nodes.get(NodeId(id)).value, NodeId(lastDescendant))
    }

    verifyRearrangements(transaction, expectedNodeInfos)
  }

  def verifyNodeIdsAreInExecutionOrder(transaction: Transaction): Assertion =
    transaction shouldBe arrangeNodeIdsInExecutionOrder(transaction)

  def verifyNodeIdsAreNotInExecutionOrder(transaction: Transaction): Assertion =
    transaction should not be arrangeNodeIdsInExecutionOrder(transaction)

  def create(
      id: String,
      builder: NodeIdTransactionBuilder & TestNodeBuilder,
      withKey: Boolean = false,
  ): Node.Create =
    builder.create(
      id = builder.newCid,
      templateId = "M:T" + id,
      argument = Value.ValueUnit,
      signatories = List("signatory"),
      observers = List("observer"),
      key = if (withKey) CreateKey.SignatoryMaintainerKey(Value.ValueUnit) else CreateKey.NoKey,
    )
  def exercise(
      choice: String,
      contract: Node.Create,
      builder: NodeIdTransactionBuilder & TestNodeBuilder,
  ): Node.Exercise =
    builder.exercise(
      contract = contract,
      choice = choice,
      consuming = true,
      actingParties = Set("signatory"),
      argument = Value.ValueUnit,
      result = Some(Value.ValueUnit),
      choiceObservers = Set.empty,
      byKey = false,
    )

  def verifyRearrangements(transaction: Transaction, expectedNodeInfos: Seq[NodeInfo]): Unit = {
    verifyNodeIdsAreInExecutionOrder(transaction)

    // change the order of node ids to verify that the executionOrderTraversalForIngestion remaps the node ids to follow
    // the execution order
    val originalOrder: Seq[Int] = 0 until transaction.nodes.size

    val permutations = (1 to shuffles).map(_ => Random.shuffle(originalOrder)).toSet

    permutations.foreach { permutation =>
      val tx =
        if (permutation == originalOrder) transaction
        else {
          val mapping = permutation.zipWithIndex.toMap.map { case (x, y) => NodeId(x) -> NodeId(y) }
          val changedOrderTx = transaction.mapNodeId(n => mapping.get(n).value)
          verifyNodeIdsAreNotInExecutionOrder(changedOrderTx)
          changedOrderTx
        }

      executionOrderTraversalForIngestion(tx).toSeq shouldBe expectedNodeInfos
    }
  }

}
