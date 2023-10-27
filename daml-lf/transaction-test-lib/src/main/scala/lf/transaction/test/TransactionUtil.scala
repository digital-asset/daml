// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data.{ImmArray}
import com.daml.lf.transaction.{NodeId, Transaction}

object TransactionUtil {

  /* Performs the naive projection of a subtransaction given the transaction
   * that contains it and the subtransaction's root node id.
   *
   * This is equivalent to the following process:
   * - replace the roots of the transaction by the given ID.
   * - prune the dangling nodes in the resulting transaction.
   * - rename sequentially the node Ids in traversal order starting from 0
   *
   * The "naive" qualifier is because this does not attempt to handle
   * rolled-back nodes correctly - the result won't agree with a proper replay
   * of the subtransaction.
   */
  def projectNaively(tx: Transaction, rootId: NodeId): Transaction = {
    val nidMap = Transaction(
      nodes = tx.nodes,
      roots = ImmArray(rootId),
    ).reachableNodeIdsInExecutionOrder.zipWithIndex.toMap

    val projectedNodes = tx.nodes.collect {
      case (nodeId, node) if nidMap.contains(nodeId) =>
        NodeId(nidMap(nodeId)) -> node.mapNodeId(x => NodeId(nidMap(x)))
    }

    Transaction(nodes = projectedNodes, roots = ImmArray(NodeId(0)))
  }
}
