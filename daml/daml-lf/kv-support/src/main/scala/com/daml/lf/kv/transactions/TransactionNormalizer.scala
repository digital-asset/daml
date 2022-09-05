// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{CommittedTransaction, Node, NodeId, VersionedTransaction}

object TransactionNormalizer {

  // KV specific normalization.
  // Drop Fetch, Lookup and Rollback nodes from a transaction, keeping Create and Exercise.
  // Also drop everything contained with a rollback node
  def normalize(
      tx: CommittedTransaction
  ): CommittedTransaction = {

    val keepNids: Set[NodeId] =
      tx.foldInExecutionOrder[Set[NodeId]](Set.empty)(
        (acc, nid, _) => (acc + nid, ChildrenRecursion.DoRecurse),
        (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        (acc, nid, node) =>
          node match {
            case _: Node.Create => acc + nid
            case _: Node.Fetch => acc
            case _: Node.LookupByKey => acc
          },
        (acc, _, _) => acc,
        (acc, _, _) => acc,
      )
    val filteredNodes =
      tx.nodes
        .filter { case (nid, _) => keepNids.contains(nid) }
        .transform {
          case (_, node: Node.Exercise) =>
            node.copy(children = node.children.filter(keepNids.contains))
          case (_, node: Node.Rollback) =>
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
