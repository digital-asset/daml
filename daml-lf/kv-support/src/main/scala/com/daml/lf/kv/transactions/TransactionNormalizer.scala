// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{CommittedTransaction, Node, NodeId, VersionedTransaction}

object TransactionNormalizer {

  // KV specific normalization.
  // Drop Fetch, Lookup and Rollback nodes from a transaction, keeping Create and Exercise.
  // Also drop everything contained with a rollback node
  // Ignore Authority node, but traverse them
  def normalize(
      tx: CommittedTransaction
  ): CommittedTransaction = {

    val keepNids: Set[NodeId] =
      tx.foldInExecutionOrder[Set[NodeId]](Set.empty)(
        exerciseBegin = (acc, nid, _) => (acc + nid, ChildrenRecursion.DoRecurse),
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        authorityBegin = (acc, _, _) => (acc, ChildrenRecursion.DoRecurse),
        leaf = (acc, nid, node) =>
          node match {
            case _: Node.Create => acc + nid
            case _: Node.Fetch => acc
            case _: Node.LookupByKey => acc
          },
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
        authorityEnd = (acc, _, _) => acc,
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
