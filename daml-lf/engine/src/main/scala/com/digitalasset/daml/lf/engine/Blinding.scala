// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.data._
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises, NodeFetch, NodeLookupByKey}
import com.daml.lf.transaction.{BlindingInfo, GenTransaction, Transaction}
import com.daml.lf.ledger._
import com.daml.lf.data.Relation.Relation

import scala.annotation.tailrec

object Blinding {

  /**
    * Given a transaction provide concise information on visibility
    * for all stakeholders returns error if the transaction is not
    * well-authorized.
    *
    * We keep this in Engine since it needs the packages and your
    * typical engine already has a way to look those up and we do not
    * want to reinvent the wheel.
    *
    *  @param tx transaction to be blinded
    *  @param initialAuthorizers set of parties claimed to be authorizers of the transaction
    */
  def checkAuthorizationAndBlind( //TODO: remove this method. Authorization now performed when transaction is constructed
      tx: Transaction.Transaction,
      initialAuthorizers: Set[Party],
  ): Either[AuthorizationError, BlindingInfo] = {
    val _ = initialAuthorizers
    Right(blind(tx))
  }

  /**
    * Like checkAuthorizationAndBlind, but does not authorize the transaction, just blinds it.
    */
  def blind(tx: Transaction.Transaction): BlindingInfo =
    BlindingTransaction
      .calculateBlindingInfo(tx)

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
  def divulgedTransaction[Nid, Cid, Val](
      divulgences: Relation[Nid, Party],
      party: Party,
      tx: GenTransaction[Nid, Cid, Val]): GenTransaction[Nid, Cid, Val] = {
    val partyDivulgences = Relation.invert(divulgences)(party)
    // Note that this relies on the local divulgence to be well-formed:
    // if an exercise node is divulged to A but some of its descendants
    // aren't the resulting transaction will not be well formed.
    val filteredNodes = tx.nodes.filter { case (k, _) => partyDivulgences.contains(k) }

    @tailrec
    def go(filteredRoots: BackStack[Nid], remainingRoots: FrontStack[Nid]): ImmArray[Nid] = {
      remainingRoots match {
        case FrontStack() => filteredRoots.toImmArray
        case FrontStackCons(root, remainingRoots) =>
          if (partyDivulgences.contains(root)) {
            go(filteredRoots :+ root, remainingRoots)
          } else {
            tx.nodes(root) match {
              case _: NodeFetch[Cid, Val] | _: NodeCreate[Cid, Val] |
                  _: NodeLookupByKey[Cid, Val] =>
                go(filteredRoots, remainingRoots)
              case ne: NodeExercises[Nid, Cid, Val] =>
                go(filteredRoots, ne.children ++: remainingRoots)
            }
          }
      }
    }

    GenTransaction(
      roots = go(BackStack.empty, FrontStack(tx.roots)),
      nodes = filteredNodes
    )
  }
}
