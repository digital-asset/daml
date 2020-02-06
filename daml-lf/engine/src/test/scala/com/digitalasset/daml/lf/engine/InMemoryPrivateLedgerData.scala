// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.digitalasset.daml.lf.data.{FrontStack, FrontStackCons, Ref}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction => Tx}
import com.digitalasset.daml.lf.value.Value._

import scala.annotation.tailrec

trait PrivateLedgerData {
  def update(tx: GenTransaction.WithTxValue[NodeId, ContractId]): Unit
  def get(id: AbsoluteContractId): Option[ContractInst[VersionedValue[AbsoluteContractId]]]
  def toContractIdString(txCounter: Int)(cid: RelativeContractId): Ref.ContractIdString
  def transactionCounter: Int
  def clear(): Unit
}

private[engine] class InMemoryPrivateLedgerData extends PrivateLedgerData {
  private val pcs
    : ConcurrentHashMap[AbsoluteContractId, ContractInst[Tx.Value[AbsoluteContractId]]] =
    new ConcurrentHashMap()
  private val txCounter: AtomicInteger = new AtomicInteger(0)

  def update(tx: GenTransaction.WithTxValue[NodeId, ContractId]): Unit =
    updateWithAbsoluteContractId(tx.resolveRelCid(toContractIdString(txCounter.get)))

  def toContractIdString(txCounter: Int)(r: RelativeContractId): Ref.ContractIdString =
    // It is safe to concatenate numbers and "-" to form a valid ContractId
    Ref.ContractIdString.assertFromString(s"$txCounter-${r.txnid.index}")

  def updateWithAbsoluteContractId(
      tx: GenTransaction.WithTxValue[NodeId, AbsoluteContractId]): Unit =
    this.synchronized {
      // traverse in topo order and add / remove
      @tailrec
      def go(remaining: FrontStack[Tx.NodeId]): Unit = remaining match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, nodeIds) =>
          val node = tx.nodes(nodeId)
          node match {
            case nc: NodeCreate.WithTxValue[AbsoluteContractId] =>
              pcs.put(nc.coid, nc.coinst)
              go(nodeIds)
            case ne: NodeExercises.WithTxValue[Tx.NodeId, AbsoluteContractId] =>
              go(ne.children ++: nodeIds)
            case _: NodeLookupByKey[_, _] | _: NodeFetch[_] =>
              go(nodeIds)
          }
      }
      go(FrontStack(tx.roots))
      txCounter.incrementAndGet()
      ()
    }

  def get(id: AbsoluteContractId): Option[ContractInst[VersionedValue[AbsoluteContractId]]] =
    this.synchronized {
      Option(pcs.get(id))
    }

  def clear(): Unit = this.synchronized {
    pcs.clear()
  }

  def transactionCounter: Int = txCounter.intValue()

  override def toString: String = s"InMemoryPrivateContractStore@{txCounter: $txCounter, pcs: $pcs}"
}

private[engine] object InMemoryPrivateLedgerData {
  def apply(): PrivateLedgerData = new InMemoryPrivateLedgerData()
}
