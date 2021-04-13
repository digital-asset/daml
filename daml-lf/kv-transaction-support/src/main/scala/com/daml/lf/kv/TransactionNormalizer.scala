// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.lf.transaction._
import com.daml.lf.value.Value.ContractId

object TransactionNormalizer {

  // KV specific normalization.
  // Drop Fetch, Lookup and Rollback nodes from a transaction, keeping Create and Exercise.
  // Also drop everything contained with a rollback node
  def normalize(
      tx: CommittedTransaction
  ): CommittedTransaction = {

    val keepNids: Set[NodeId] =
      tx.foldInExecutionOrder[Set[NodeId]](Set.empty)(
        (acc, nid, _) => (acc + nid, true),
        (acc, _, _) => (acc, false),
        (acc, nid, node) =>
          node match {
            case _: Node.NodeCreate[_] => acc + nid
            case _: Node.NodeFetch[_] => acc
            case _: Node.NodeLookupByKey[_] => acc
          },
        (acc, _, _) => acc,
        (acc, _, _) => acc,
      )
    val filteredNodes =
      tx.nodes
        .filter { case (nid, _) => keepNids.contains(nid) }
        .transform {
          case (_, node: Node.NodeExercises[NodeId, ContractId]) =>
            node.copy(children = node.children.filter(keepNids.contains))
          case (_, node: Node.NodeRollback[NodeId]) =>
            node.copy(children = node.children.filter(keepNids.contains))
          case (_, keep) =>
            keep
        }

    val filteredRoots = tx.roots.filter(keepNids.contains)
    CommittedTransaction(
      VersionedTransaction(tx.version, filteredNodes, filteredRoots)
    )
  }

}
