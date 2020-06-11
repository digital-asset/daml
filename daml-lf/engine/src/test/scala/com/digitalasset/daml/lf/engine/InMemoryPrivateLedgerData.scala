// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.daml.lf.data.{FrontStack, FrontStackCons}
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value._

import scala.annotation.tailrec

trait PrivateLedgerData {
  def update(tx: Tx.Transaction): Unit
  def get(id: ContractId): Option[ContractInst[VersionedValue[ContractId]]]
  def transactionCounter: Int
  def clear(): Unit
}

private[engine] class InMemoryPrivateLedgerData extends PrivateLedgerData {
  private val pcs: ConcurrentHashMap[ContractId, ContractInst[Tx.Value[ContractId]]] =
    new ConcurrentHashMap()
  private val txCounter: AtomicInteger = new AtomicInteger(0)

  def update(tx: Tx.Transaction): Unit =
    updateWithContractId(tx)

  def updateWithContractId(tx: Tx.Transaction): Unit =
    this.synchronized {
      // traverse in topo order and add / remove
      @tailrec
      def go(remaining: FrontStack[Tx.NodeId]): Unit = remaining match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, nodeIds) =>
          val node = tx.nodes(nodeId)
          node match {
            case nc: NodeCreate.WithTxValue[ContractId] =>
              pcs.put(nc.coid, nc.coinst)
              go(nodeIds)
            case ne: NodeExercises.WithTxValue[Tx.NodeId, ContractId] =>
              go(ne.children ++: nodeIds)
            case _: NodeLookupByKey[_, _] | _: NodeFetch[_, _] =>
              go(nodeIds)
          }
      }
      go(FrontStack(tx.roots))
      txCounter.incrementAndGet()
      ()
    }

  def get(id: ContractId): Option[ContractInst[VersionedValue[ContractId]]] =
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
