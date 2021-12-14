// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.TransactionUtils.TransactionNodeIdWithNode
import com.daml.lf.transaction.{TransactionOuterClass, TransactionVersion}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** [[TransactionUtils.traverseTransactionWithWitnesses]] is covered by [[TransactionTraversalSpec]]. */
class TransactionUtilsSpec extends AnyWordSpec with Matchers {

  import TransactionUtilsSpec._

  "TransactionUtilsSpec" should {
    "extractTransactionVersion" in {
      val actualVersion = TransactionUtils.extractTransactionVersion(aRawTransaction)
      actualVersion shouldBe TransactionVersion.VDev.protoValue
    }

    "extractNodeId" in {
      val actualNodeId = TransactionUtils.extractNodeId(aRawRootNode)
      actualNodeId shouldBe Raw.NodeId("rootId")
    }

    "reconstructTransaction" in {
      val reconstructedTransaction = TransactionUtils.reconstructTransaction(
        TransactionVersion.VDev.protoValue,
        Seq(
          TransactionNodeIdWithNode(aRawRootNodeId, aRawRootNode),
          TransactionNodeIdWithNode(aRawChildNodeId, aRawChildNode),
        ),
      )
      reconstructedTransaction shouldBe aRawTransaction
    }
  }
}

object TransactionUtilsSpec {
  private val aRootNodeId = "rootId"
  private val aChildNodeId = "childId"
  private val aRawRootNodeId = Raw.NodeId(aRootNodeId)
  private val aRawChildNodeId = Raw.NodeId(aChildNodeId)

  private val aChildNode = TransactionOuterClass.Node
    .newBuilder()
    .setNodeId(aChildNodeId)
    .setExercise(
      TransactionOuterClass.NodeExercise
        .newBuilder()
        .addObservers("childObserver")
    )
  private val aRawChildNode = Raw.TransactionNode(aChildNode.build().toByteString)
  private val aRootNode = TransactionOuterClass.Node
    .newBuilder()
    .setNodeId(aRootNodeId)
    .setExercise(
      TransactionOuterClass.NodeExercise
        .newBuilder()
        .addChildren(aChildNodeId)
        .addObservers("rootObserver")
    )
  private val aRawRootNode = Raw.TransactionNode(aRootNode.build().toByteString)
  private val aRawTransaction = Raw.Transaction(
    TransactionOuterClass.Transaction
      .newBuilder()
      .addRoots(aRootNodeId)
      .addNodes(aRootNode)
      .addNodes(aChildNode)
      .setVersion(TransactionVersion.VDev.protoValue)
      .build()
      .toByteString
  )
}
