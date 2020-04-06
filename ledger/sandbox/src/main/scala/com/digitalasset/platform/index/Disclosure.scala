// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.Transaction.{NodeId, Transaction}
import com.daml.lf.transaction.{GenTransaction, NodeInfo}

object Disclosure {

  def forFlatTransaction[Nid, Cid, Val](tx: GenTransaction[Nid, Cid, Val]): Relation[Nid, Party] =
    tx.nodes.collect {
      case (nodeId, c: NodeInfo.Create) =>
        nodeId -> c.stakeholders
      case (nodeId, e: NodeInfo.Exercise) if e.consuming =>
        nodeId -> e.stakeholders
    }

  def forTransactionTree(tx: Transaction): Relation[NodeId, Party] = {
    val createAndExercise: Set[NodeId] =
      tx.nodes.collect {
        case p @ (_, _: NodeInfo.Create) => p
        case p @ (_, _: NodeInfo.Exercise) => p
      }.keySet

    Blinding.blind(tx).disclosure.filterKeys(createAndExercise)
  }

}
