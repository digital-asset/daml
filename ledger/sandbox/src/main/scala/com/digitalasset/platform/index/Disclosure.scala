// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.Transaction.{NodeId, TContractId, Transaction, Value}
import com.digitalasset.daml.lf.transaction.{GenTransaction, NodeInfo}

object Disclosure {

  def forFlatTransaction[Nid, Cid, Val](tx: GenTransaction[Nid, Cid, Val]): Relation[Nid, Party] =
    tx.nodes.collect {
      case (nodeId, c: NodeInfo.Create) =>
        nodeId -> c.stakeholders
      case (nodeId, e: NodeInfo.Exercise) if e.consuming =>
        nodeId -> e.stakeholders
    }

  def forTransactionTree[Nid, Cid, Val](
      disclosure: GenTransaction[Nid, Cid, Val] => Relation[Nid, Party])(
      tx: GenTransaction[Nid, Cid, Val]): Relation[Nid, Party] = {

    val createAndExercise: Set[Nid] =
      tx.nodes.collect {
        case p @ (_, _: NodeInfo.Create) => p
        case p @ (_, _: NodeInfo.Exercise) => p
      }.keySet

    disclosure(tx).filterKeys(createAndExercise)
  }

  val forTransactionTree: Transaction => Relation[NodeId, Party] =
    forTransactionTree[NodeId, TContractId, Value[TContractId]](Blinding.blind(_).disclosure)

}
