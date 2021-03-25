// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.lf.transaction._
import com.daml.lf.value.Value.ContractId

object Normalizer {

  // KV specific normalization.
  // Drop Fetch, Lookup and Rollback nodes from a transaction, keeping Create and Exercise.
  def normalizeTransaction(
      tx: CommittedTransaction
  ): CommittedTransaction = {
    // TODO https://github.com/digital-asset/daml/issues/8020

    // We need drop Rollback nodes and all their children. Before we do this we need to
    // update NormalizerSpec. Which in turn requires we extend our scalagen generators to
    // generate transactions containing rollback nodes.
    val nodes = tx.nodes.filter {
      case (_, _: Node.NodeRollback[_] | _: Node.NodeFetch[_] | _: Node.NodeLookupByKey[_]) => false
      case (_, _: Node.NodeExercises[_, _] | _: Node.NodeCreate[_]) =>
        true
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
