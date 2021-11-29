// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.lf.transaction._

object TransactionNormalizer {

  // KV specific normalization.
  // Drop Fetch, Lookup and Rollback nodes from a transaction, keeping Create and Exercise.
  // Also drop everything contained with a rollback node
  def normalize(
      tx: CommittedTransaction
  ): CommittedTransaction = {
    val filteredTx = tx.map { utx =>
      val keepNids: Set[NodeId] =
        utx.foldInExecutionOrder[Set[NodeId]](Set.empty)(
          (acc, nid, _) => (acc + nid, true),
          (acc, _, _) => (acc, false),
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
        utx.nodes
          .filter { case (nid, _) => keepNids.contains(nid) }
          .transform {
            case (_, node: Node.Exercise) =>
              node.copy(children = node.children.filter(keepNids.contains))
            case (_, node: Node.Rollback) =>
              node.copy(children = node.children.filter(keepNids.contains))
            case (_, keep) =>
              keep
          }
      val filteredRoots = utx.roots.filter(keepNids.contains)
      Transaction(filteredNodes, filteredRoots)
    }
    CommittedTransaction(filteredTx)
  }

}
