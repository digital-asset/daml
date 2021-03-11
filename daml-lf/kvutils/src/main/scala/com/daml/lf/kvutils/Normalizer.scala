// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kvutils

import com.daml.lf.transaction._
import com.daml.lf.value.Value.ContractId

object Normalizer {

  // KV specific normalization.
  // drops Fetch and Lookup nodes from a transaction.
  def normalizeTransaction(
      tx: CommittedTransaction
  ): CommittedTransaction = {
    val nodes = tx.nodes.filter {
      case (_, _: Node.NodeFetch[_] | _: Node.NodeLookupByKey[_]) => false
      case (_, _: Node.NodeExercises[_, _] | _: Node.NodeCreate[_]) => true
    }
    val filteredNodes = nodes.transform {
      case (_, node: Node.NodeExercises[NodeId, ContractId]) =>
        val filteredNode = node.copy(children = node.children.filter(nodes.contains))
        filteredNode
      case (_, keep) =>
        keep
    }
    val filteredRoots = tx.roots.filter(filteredNodes.contains)
    CommittedTransaction(
      VersionedTransaction(tx.version, filteredNodes, filteredRoots)
    )
  }

}
