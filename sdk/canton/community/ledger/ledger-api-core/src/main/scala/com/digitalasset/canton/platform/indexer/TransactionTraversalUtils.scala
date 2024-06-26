// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Node, NodeId, Transaction}

object TransactionTraversalUtils {
  def preorderTraversalForIngestion(transaction: Transaction): Iterator[(NodeId, Node)] =
    transaction
      .foldInExecutionOrder(List.empty[(NodeId, Node)])(
        exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
        // Rollback nodes are not indexed
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        leaf = (acc, nid, node) => (nid -> node) :: acc,
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      .reverseIterator
}
