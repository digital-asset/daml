// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray, Ref}
import com.daml.lf.transaction.TransactionOuterClass.Node
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass, TransactionVersion}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object TransactionUtils {

  def extractTransactionVersion(rawTransaction: Raw.Transaction): String =
    TransactionOuterClass.Transaction.parseFrom(rawTransaction.bytes).getVersion

  def extractNodeId(rawTransactionNode: Raw.TransactionNode): Raw.NodeId =
    Raw.NodeId(TransactionOuterClass.Node.parseFrom(rawTransactionNode.bytes).getNodeId)

  // Helper to traverse the transaction, top-down, while keeping track of the
  // witnessing parties of each node.
  def traverseTransactionWithWitnesses(rawTx: Raw.Transaction)(
      f: (Raw.NodeId, Raw.TransactionNode, Set[Ref.Party]) => Unit
  ): Unit = {
    val tx = TransactionOuterClass.Transaction.parseFrom(rawTx.bytes)
    val nodes = tx.getNodesList.iterator.asScala.map(n => n.getNodeId -> n).toMap
    val txVersion = TransactionVersion.assertFromString(tx.getVersion)

    @tailrec
    @throws[Err.InvalidSubmission]
    def go(toVisit: FrontStack[(Raw.NodeId, Set[Ref.Party])]): Unit = {
      toVisit match {
        case FrontStack() => ()
        case FrontStackCons((nodeId, parentWitnesses), toVisit) =>
          val node = nodes(nodeId.value)
          val witnesses = parentWitnesses union informeesOfNode(txVersion, node)
          f(nodeId, Raw.TransactionNode(node.toByteString), witnesses)
          // Recurse into children (if any).
          node.getNodeTypeCase match {
            case Node.NodeTypeCase.EXERCISE =>
              val next = node.getExercise.getChildrenList.asScala.view
                .map(Raw.NodeId(_) -> witnesses)
                .to(ImmArray)
              go(next ++: toVisit)

            case _ =>
              go(toVisit)
          }
      }
    }

    def informeesOfNode(
        txVersion: TransactionVersion,
        node: TransactionOuterClass.Node,
    ): Set[Ref.Party] =
      TransactionCoder
        .protoActionNodeInfo(txVersion, node)
        .fold(err => throw Err.InvalidSubmission(err.errorMessage), _.informeesOfNode)

    go(tx.getRootsList.asScala.view.map(Raw.NodeId(_) -> Set.empty[Ref.Party]).to(FrontStack))
  }

  def reconstructTransaction(
      transactionVersion: String,
      nodesWithIds: Seq[TransactionNodeIdWithNode],
  ): Raw.Transaction = {
    // Reconstruct roots by considering the transaction nodes in order and
    // marking all child nodes as non-roots and skipping over them.
    val nonRoots = mutable.HashSet.empty[Raw.NodeId]
    val txBuilder = TransactionOuterClass.Transaction.newBuilder.setVersion(transactionVersion)
    for (TransactionNodeIdWithNode(nodeId, rawNode) <- nodesWithIds) {
      val node = TransactionOuterClass.Node.parseFrom(rawNode.bytes)
      txBuilder.addNodes(node)
      if (!nonRoots.contains(nodeId)) {
        txBuilder.addRoots(nodeId.value)
      }
      if (node.hasExercise) {
        val children = node.getExercise.getChildrenList.asScala.map(Raw.NodeId).toSet
        nonRoots ++= children
      }
    }
    Raw.Transaction(txBuilder.build.toByteString)
  }

  final case class TransactionNodeIdWithNode(
      nodeId: Raw.NodeId,
      node: Raw.TransactionNode,
  )
}
