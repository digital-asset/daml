// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.digitalasset.daml.lf.data.{FrontStack, FrontStackCons}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction => Tx}
import com.digitalasset.daml.lf.value.Value._

import scala.annotation.tailrec

trait PrivateLedgerData {
  def update(tx: GenTransaction[NodeId, ContractId, VersionedValue[ContractId]]): Unit
  def get(id: AbsoluteContractId): Option[ContractInst[VersionedValue[AbsoluteContractId]]]
  def toAbsoluteContractId(txCounter: Int)(cid: ContractId): AbsoluteContractId
  def transactionCounter: Int
  def clear(): Unit
}

private[engine] class InMemoryPrivateLedgerData extends PrivateLedgerData {
  private val pcs
    : ConcurrentHashMap[AbsoluteContractId, ContractInst[Tx.Value[AbsoluteContractId]]] =
    new ConcurrentHashMap()
  private val txCounter: AtomicInteger = new AtomicInteger(0)

  def update(tx: GenTransaction[NodeId, ContractId, VersionedValue[ContractId]]): Unit =
    updateWithAbsoluteContractId(tx.mapContractId(toAbsoluteContractId(txCounter.get)))

  def toAbsoluteContractId(txCounter: Int)(cid: ContractId): AbsoluteContractId =
    cid match {
      case r: RelativeContractId => AbsoluteContractId(s"$txCounter-${r.txnid.index}")
      case a: AbsoluteContractId => a
    }

  def updateWithAbsoluteContractId(
      tx: GenTransaction[NodeId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]): Unit =
    this.synchronized {
      // traverse in topo order and add / remove
      @tailrec
      def go(remaining: FrontStack[Tx.NodeId]): Unit = remaining match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, nodeIds) =>
          val node = tx.nodes(nodeId)
          node match {
            case _: NodeFetch[AbsoluteContractId] =>
              go(nodeIds)
            case nc: NodeCreate[AbsoluteContractId, Tx.Value[AbsoluteContractId]] =>
              pcs.put(nc.coid, nc.coinst)
              go(nodeIds)
            case ne: NodeExercises[Tx.NodeId, AbsoluteContractId, Tx.Value[AbsoluteContractId]] =>
              go(ne.children ++: nodeIds)
            case _: NodeLookupByKey[_, _] =>
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
