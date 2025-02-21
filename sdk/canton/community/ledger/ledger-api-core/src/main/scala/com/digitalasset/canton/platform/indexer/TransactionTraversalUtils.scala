// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.digitalasset.daml.lf.transaction.Node.Create
import com.digitalasset.daml.lf.transaction.Transaction.ChildrenRecursion
import com.digitalasset.daml.lf.transaction.{Node, NodeId, Transaction}

object TransactionTraversalUtils {

  final case class NodeInfo(nodeId: NodeId, node: Node, lastDescendantNodeId: NodeId)

  /** It reorders the node ids of a transaction to follow the execution order (pre-order traversal)
    * and finds the node ids of their last descendant, omitting the fetch, the lookup, the rollback
    * nodes and the descendants of the rollback nodes.
    *
    * @param transaction
    *   the given transaction
    * @return
    *   the node id, node and the node id of the last descendant in the execution order
    */
  def executionOrderTraversalForIngestion(transaction: Transaction): Iterator[NodeInfo] = {
    // Rearrange node ids to follow the execution order.
    // We need to do this to guarantee that the representation of the descendants with the use of the last descendant
    // holds.
    // It requires that all descendant nodes have a node id which is greater than their ancestor's and
    // lower than their ancestor's last descendant node id.
    val orderedTx = arrangeNodeIdsInExecutionOrder(transaction)

    val lastDescendantMapping: Map[NodeId, NodeId] = getLastDescendantMapping(orderedTx)

    def lastDescendant(nid: NodeId): NodeId = lastDescendantMapping.getOrElse(
      nid,
      throw new RuntimeException(s"It should never have been here (nodeId: $nid)!"),
    )

    orderedTx
      .foldInExecutionOrder(List.empty[NodeInfo])(
        exerciseBegin = (acc, nid, node) => {
          (
            NodeInfo(nid, node, lastDescendant(nid)) :: acc,
            ChildrenRecursion.DoRecurse,
          )
        },
        // Rollback nodes are not indexed
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        // Fetch and Lookup nodes are not indexed
        leaf = {
          case (acc, nid, node: Create) => NodeInfo(nid, node, lastDescendant(nid)) :: acc
          case (acc, _, _) => acc
        },
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
  }.reverseIterator

  private[indexer] def arrangeNodeIdsInExecutionOrder(
      transaction: Transaction
  ): Transaction = {
    val mapping = transaction
      .foldInExecutionOrder((List.empty[(NodeId, NodeId)], 0))(
        exerciseBegin = { case ((acc, next), nid, _) =>
          (((nid -> NodeId(next)) :: acc, next + 1), ChildrenRecursion.DoRecurse)
        },
        rollbackBegin = { case ((acc, next), nid, _) =>
          (((nid -> NodeId(next)) :: acc, next + 1), ChildrenRecursion.DoRecurse)
        },
        leaf = { case ((acc, next), nid, _) =>
          ((nid -> NodeId(next)) :: acc, next + 1)
        },
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      ._1
      .toMap
    transaction.mapNodeId(nid =>
      mapping.getOrElse(
        nid,
        throw new RuntimeException(s"It should never have been here (nodeId: $nid)!"),
      )
    )
  }

  private def getLastDescendantMapping(transaction: Transaction): Map[NodeId, NodeId] =
    transaction
      .foldInExecutionOrder(List.empty[(NodeId, NodeId)])(
        exerciseBegin = (acc, _, _) => (acc, ChildrenRecursion.DoRecurse),
        // Rollback nodes are not indexed
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        leaf = (acc, nid, _) => (nid -> nid) :: acc,
        exerciseEnd = (acc, nid, _) => {
          val lastDescendantNodeId =
            // if the exercise node had no children then the last element added to the list will have a node id that is
            // less than the exercise node (since we traverse the transaction in execution order) and the node id of
            // itself will be stored as its last descendant
            NodeId(acc.headOption.map(_._2.index).getOrElse(Int.MinValue).max(nid.index))
          (nid -> lastDescendantNodeId) :: acc
        },
        rollbackEnd = (acc, _, _) => acc,
      )
      .toMap
}
