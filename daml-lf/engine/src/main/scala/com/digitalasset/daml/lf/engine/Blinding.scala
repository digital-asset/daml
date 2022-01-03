// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.data._
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.{BlindingInfo, Transaction, NodeId, VersionedTransaction}
import com.daml.lf.ledger._
import com.daml.lf.data.Relation.Relation

import scala.annotation.tailrec

object Blinding {

  /** Given a transaction provide concise information on visibility
    * for all stakeholders
    *
    * We keep this in Engine since it needs the packages and your
    * typical engine already has a way to look those up and we do not
    * want to reinvent the wheel.
    *
    *  @param tx transaction to be blinded
    */
  def blind(tx: VersionedTransaction): BlindingInfo =
    BlindingTransaction.calculateBlindingInfo(tx)

  /** Returns the part of the transaction which has to be divulged to the given party.
    *
    * Note that if the child of a root node is divulged but the parent isn't, the child
    * will become a root note itself. Such nodes are "uprooted" in order, in the sense
    * that nodes that come before when traversing depth first, left to right will appear
    * first in the roots list.
    *
    * This also mean that there might be more roots in the divulged transaction than in
    * the original transaction.
    *
    * This function will crash if the transaction provided is malformed -- that is, if the
    * transaction has Nid references that are not present in its nodes. Use `isWellFormed`
    * if you are getting the transaction from a third party.
    */
  def divulgedTransaction(
      divulgences: Relation[NodeId, Party],
      party: Party,
      tx: Transaction,
  ): Transaction = {
    val partyDivulgences = Relation.invert(divulgences)(party)
    // Note that this relies on the local divulgence to be well-formed:
    // if an exercise node is divulged to A but some of its descendants
    // aren't the resulting transaction will not be well formed.
    val filteredNodes = tx.nodes.filter { case (k, _) => partyDivulgences.contains(k) }

    @tailrec
    def go(
        filteredRoots: BackStack[NodeId],
        remainingRoots: FrontStack[NodeId],
    ): ImmArray[NodeId] = {
      remainingRoots match {
        case FrontStack() => filteredRoots.toImmArray
        case FrontStackCons(root, remainingRoots) =>
          if (partyDivulgences.contains(root)) {
            go(filteredRoots :+ root, remainingRoots)
          } else {
            tx.nodes(root) match {
              case nr: Node.Rollback =>
                go(filteredRoots, nr.children ++: remainingRoots)
              case _: Node.Fetch | _: Node.Create | _: Node.LookupByKey =>
                go(filteredRoots, remainingRoots)
              case ne: Node.Exercise =>
                go(filteredRoots, ne.children ++: remainingRoots)
            }
          }
      }
    }

    Transaction(
      roots = go(BackStack.empty, tx.roots.toFrontStack),
      nodes = filteredNodes,
    )
  }
}
