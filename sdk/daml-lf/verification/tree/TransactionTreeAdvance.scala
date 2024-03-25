// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package tree

import stainless.lang.{
  unfold,
  decreases,
  BooleanDecorations,
  Either,
  Some,
  None,
  Option,
  Right,
  Left,
}
import stainless.annotation._
import stainless.collection._
import utils.Value.ContractId
import utils.Transaction.{DuplicateContractKey, InconsistentContractKey, KeyInputError}
import utils._
import utils.TreeProperties._

import transaction.{State}
import transaction.CSMHelpers._
import transaction.CSMAdvanceDef._
import transaction.CSMAdvance._
import transaction.CSMEitherDef._
import transaction.ContractStateMachine._

import TransactionTreeDef._
import TransactionTree._

/** File stating how the activeState evolve while processing a transaction. We want to prove that the activeState of the
  * resulting state of a traversal can be expressed in terms of the inital state active state and the advance method.
  */
object TransactionTreeAdvanceDef {

  /** Function called when a node is entered for the first time ([[utils.TraversalDirection.Down]]).
    * Compute the active state and the rollbackStack of a state after processing a node given only the rollbackStack
    * and the active state of it behorehand.
    *
    * @param s rollbackStack and activeState of the state before the node
    * @param p Node and its id
    */
  @pure
  @opaque
  def activeStateInFun(
      s: (ActiveLedgerState, List[ActiveLedgerState]),
      p: (NodeId, Node),
  ): (ActiveLedgerState, List[ActiveLedgerState]) = {
    p._2 match {
      case a: Node.Action => (s._1.advance(actionActiveStateAddition(p._1, a)), s._2)
      case r: Node.Rollback => (s._1, s._1 :: s._2)
    }
  }

  /** Function called when a node is entered for the second time ([[utils.TraversalDirection.Up]]).
    * Compute the active state and the rollbackStack of a state after processing a node given only the rollbackStack
    * and the active state of it behorehand.
    *
    * @param s rollbackStack and activeState of the state before the node
    * @param p Node and its id
    */
  @pure
  @opaque
  def activeStateOutFun(
      s: (ActiveLedgerState, List[ActiveLedgerState]),
      p: (NodeId, Node),
  ): (ActiveLedgerState, List[ActiveLedgerState]) = {
    p._2 match {
      case a: Node.Action => s
      case r: Node.Rollback =>
        s._2 match {
          case Nil() => s
          case Cons(h, t) => (h, t)
        }
    }
  }

  /** List of triples whose respective entries are:
    *  - The activeState and the rollbackStack before the i-th step of the traversal
    *  - The pair node id - node that is handle during the i-th step
    *  - The direction i.e. if that's the first or the second time we enter the node
    *
    * @param tr   The transaction that is being processed
    * @param init The activeState and teh rollbackStack of the initial state of the traversal
    */
  @pure
  def scanActiveState(
      tr: Tree[(NodeId, Node)],
      init: (ActiveLedgerState, List[ActiveLedgerState]),
  ): List[((ActiveLedgerState, List[ActiveLedgerState]), (NodeId, Node), TraversalDirection)] = {
    tr.scan(init, activeStateInFun, activeStateOutFun)
  }

  /** [[scanActiveState]] where the inital state is empty
    */
  @pure
  def scanActiveState(
      tr: Tree[(NodeId, Node)]
  ): List[((ActiveLedgerState, List[ActiveLedgerState]), (NodeId, Node), TraversalDirection)] = {
    scanActiveState(tr, (ActiveLedgerState.empty, Nil[ActiveLedgerState]()))
  }

  /** Computes the activeState and the rollbackStack of the state obtained after processing a transaction.
    *
    * @param tr   The transaction that is being processed
    * @param init The activeState and the rollbackStack of the initial state of the traversal
    */
  @pure
  def traverseActiveState(
      tr: Tree[(NodeId, Node)],
      init: (ActiveLedgerState, List[ActiveLedgerState]),
  ): (ActiveLedgerState, List[ActiveLedgerState]) = {
    tr.traverse(init, activeStateInFun, activeStateOutFun)
  }

  /** [[traverseActiveState]] where the inital state is empty
    */
  @pure
  def traverseActiveState(
      tr: Tree[(NodeId, Node)]
  ): (ActiveLedgerState, List[ActiveLedgerState]) = {
    traverseActiveState(tr, (ActiveLedgerState.empty, Nil[ActiveLedgerState]()))
  }
}

object TransactionTreeAdvance {

  import TransactionTreeAdvanceDef._

  /** The rollbackStack of the initial and final state of a transaction traversal are equal
    *
    * @param tr The transaction that is being processed
    * @param init The initial state of the traversal
    *
    * @see [[TransactionTree.traverseTransactionProp]] for an alternative proof of the same concept.
    */
  @pure
  @opaque
  def traverseActiveStateSameStack(
      tr: Tree[(NodeId, Node)],
      init: (ActiveLedgerState, List[ActiveLedgerState]),
  ): Unit = {
    decreases(tr)
    unfold(tr.traverse(init, activeStateInFun, activeStateOutFun))
    tr match {
      case Endpoint() => Trivial()
      case ContentNode(n, sub) =>
        val a1 = activeStateInFun(init, n)
        traverseActiveStateSameStack(sub, a1)
        unfold(activeStateInFun(init, n))
        unfold(activeStateOutFun(traverseActiveState(sub, a1), n))
      case ArticulationNode(l, r) =>
        val al = traverseActiveState(l, init)
        traverseActiveStateSameStack(l, init)
        traverseActiveStateSameStack(r, al)
    }
  }.ensuring(
    init._2 == traverseActiveState(tr, init)._2
  )

  /** The rollbackStacks of the state before entering a node for the first time and after having entered it for the second
    * time are the same.
    *
    * @param tr    The transaction that is being processed.
    * @param init The activeState and the rollbackStack of the initial state of the transaction
    * @param i     The step number during which the node is entered for the first time.
    * @param j     The step number during which the node is entered for the second time.
    *
    * @note This is one of the only structural induction proof in the codebase. Because the global nature of the property,
    *       it is not possible to prove a local claim that is preserved during every step. This is due to the symmetry
    *       of the traversal (for rollbacks) and it would therefore not make sense finding a i such that the
    *       property is true at the i-th step but not the i+1-th one.
    * @see [[TransactionTree.findBeginRollback]] for an alternative proof of the same concept.
    */
  @pure
  @opaque
  def findBeginRollbackActiveState(
      tr: Tree[(NodeId, Node)],
      init: (ActiveLedgerState, List[ActiveLedgerState]),
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(tr)

    require(tr.isUnique)
    require(i >= 0)
    require(i <= j)
    require(j < 2 * tr.size)
    require(scanActiveState(tr, init)(i)._2 == scanActiveState(tr, init)(j)._2)
    require(scanActiveState(tr, init)(i)._2._2.isInstanceOf[Node.Rollback])
    require(scanActiveState(tr, init)(i)._3 == TraversalDirection.Down)
    require(scanActiveState(tr, init)(j)._3 == TraversalDirection.Up)

    unfold(tr.size)
    unfold(tr.isUnique)

    tr match {
      case Endpoint() => Unreachable()
      case ContentNode(c, str) =>
        scanIndexing(c, str, init, activeStateInFun, activeStateOutFun, i)
        scanIndexing(c, str, init, activeStateInFun, activeStateOutFun, j)

        if ((i == 0) || (j == 2 * tr.size - 1)) {
          scanIndexing(c, str, init, activeStateInFun, activeStateOutFun, 0)
          scanIndexing(c, str, init, activeStateInFun, activeStateOutFun, 2 * tr.size - 1)
          isUniqueIndexing(tr, init, activeStateInFun, activeStateOutFun, 0, i)
          isUniqueIndexing(tr, init, activeStateInFun, activeStateOutFun, j, 2 * tr.size - 1)
          traverseActiveStateSameStack(str, activeStateInFun(init, c))
          unfold(activeStateInFun(init, c))
        } else {
          findBeginRollbackActiveState(str, activeStateInFun(init, c), i - 1, j - 1)
        }
      case ArticulationNode(l, r) =>
        scanIndexing(l, r, init, activeStateInFun, activeStateOutFun, i)
        scanIndexing(l, r, init, activeStateInFun, activeStateOutFun, j)

        if (j < 2 * l.size) {
          findBeginRollbackActiveState(l, init, i, j)
        } else if (i >= 2 * l.size) {
          findBeginRollbackActiveState(
            r,
            l.traverse(init, activeStateInFun, activeStateOutFun),
            i - 2 * l.size,
            j - 2 * l.size,
          )
        } else {
          scanContains(l, init, activeStateInFun, activeStateOutFun, i)
          scanContains(
            r,
            l.traverse(init, activeStateInFun, activeStateOutFun),
            activeStateInFun,
            activeStateOutFun,
            j - 2 * l.size,
          )
          SetProperties.disjointContains(
            l.content,
            r.content,
            tr.scan(init, activeStateInFun, activeStateOutFun)(i)._2,
          )
          SetProperties.disjointContains(
            l.content,
            r.content,
            tr.scan(init, activeStateInFun, activeStateOutFun)(j)._2,
          )
          Unreachable()
        }
    }
  }.ensuring(
    scanActiveState(tr, init)(i)._1._1 :: scanActiveState(tr, init)(i)._1._2 == scanActiveState(
      tr,
      init,
    )(j)._1._2
  )

  /** For any step in a transaction traversal, [[scanActiveState]] computes the activeState of the intermediate state
    * of that step.
    *
    * @param tr   The transaction that is being processed.
    * @param init The initial state of the traversal.
    * @param i The step number.
    */
  @pure
  @opaque
  def scanActiveStateAdvance(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    decreases(i)

    require(i >= 0)
    require(i < 2 * tr.size)
    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(tr.isUnique)

    scanIndexingState(tr, init, traverseInFun, traverseOutFun, i)
    scanIndexingState(
      tr,
      (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
      activeStateInFun,
      activeStateOutFun,
      i,
    )

    if (i == 0) {
      emptyAdvance(init.get.activeState)
    } else {
      scanTransactionProp(tr, init, i - 1, i)
      unfold(propagatesError(scanTransaction(tr, init)(i - 1)._1, scanTransaction(tr, init)(i)._1))
      scanActiveStateAdvance(tr, init, i - 1)
      scanIndexingNode(
        tr,
        (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
        init,
        activeStateInFun,
        activeStateOutFun,
        traverseInFun,
        traverseOutFun,
        i - 1,
      )

      val (si, n, dir) = scanTransaction(tr, init)(i - 1)
      val ai = scanActiveState(tr)(i - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFun(si, n))
        unfold(activeStateInFun(ai, n))

        n._2 match {
          case a: Node.Action =>
            unfold(handleNode(si, n._1, a))
            handleNodeActiveState(si.get, n._1, a)
            advanceAssociativity(init.get.activeState, ai._1, actionActiveStateAddition(n._1, a))
          case r: Node.Rollback =>
            unfold(beginRollback(si))
            unfold(si.get.beginRollback())
        }
      } else {
        unfold(traverseOutFun(si, n))
        unfold(activeStateOutFun(ai, n))
        n._2 match {
          case a: Node.Action => Trivial()
          case r: Node.Rollback =>
            val (j, sub) = findBeginRollback(tr, init, i - 1)
            val sj = scanTransaction(tr, init)(j)._1

            traverseTransactionProp(sub, beginRollback(sj))
            scanTransactionProp(tr, init, j, i)
            unfold(propagatesError(sj, scanTransaction(tr, init)(i)._1))

            unfold(sameStack(beginRollback(sj), si))
            unfold(sameStack(sj.get.beginRollback(), si))
            unfold(endRollback(si))
            unfold(si.get.endRollback())
            unfold(beginRollback(sj))
            unfold(sj.get.beginRollback())

            scanIndexingNode(
              tr,
              (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
              init,
              activeStateInFun,
              activeStateOutFun,
              traverseInFun,
              traverseOutFun,
              j,
            )

            findBeginRollbackActiveState(
              tr,
              (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
              j,
              i - 1,
            )
            scanActiveStateAdvance(tr, init, j)
        }
      }
    }

  }.ensuring(
    (scanTransaction(tr, init)(i)._1.get.activeState ==
      init.get.activeState.advance(scanActiveState(tr)(i)._1._1))
  )

  /** [[traverseActiveState]] computes the activeState of the resulting state of transaction traversal.
    *
    * @param tr   The transaction that is being processed.
    * @param init The initial state of the traversal.
    */
  @pure
  @opaque
  def traverseActiveStateAdvance(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {

    require(init.isRight)
    require(traverseTransaction(tr, init).isRight)
    require(tr.isUnique)

    if (tr.size == 0) {
      emptyAdvance(init.get.activeState)
    } else {
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
      scanIndexingState(
        tr,
        (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
        activeStateInFun,
        activeStateOutFun,
        0,
      )

      traverseTransactionDefined(tr, init, 2 * tr.size - 1)
      unfold(
        propagatesError(
          scanTransaction(tr, init)(2 * tr.size - 1)._1,
          traverseTransaction(tr, init),
        )
      )
      scanActiveStateAdvance(tr, init, 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
        init,
        activeStateInFun,
        activeStateOutFun,
        traverseInFun,
        traverseOutFun,
        2 * tr.size - 1,
      )

      val (si, n, dir) = scanTransaction(tr, init)(2 * tr.size - 1)
      val ai = scanActiveState(tr)(2 * tr.size - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFun(si, n))
        unfold(activeStateInFun(ai, n))

        n._2 match {
          case a: Node.Action =>
            unfold(handleNode(si, n._1, a))
            handleNodeActiveState(si.get, n._1, a)
            advanceAssociativity(init.get.activeState, ai._1, actionActiveStateAddition(n._1, a))
          case r: Node.Rollback =>
            unfold(beginRollback(si))
            unfold(si.get.beginRollback())
        }
      } else {
        unfold(traverseOutFun(si, n))
        unfold(activeStateOutFun(ai, n))
        n._2 match {
          case a: Node.Action => Trivial()
          case r: Node.Rollback =>
            val (j, sub) = findBeginRollback(tr, init, 2 * tr.size - 1)
            val sj = scanTransaction(tr, init)(j)._1

            traverseTransactionProp(sub, beginRollback(sj))
            traverseTransactionDefined(tr, init, j)
            unfold(propagatesError(sj, traverseTransaction(tr, init)))

            unfold(sameStack(beginRollback(sj), si))
            unfold(sameStack(sj.get.beginRollback(), si))
            unfold(endRollback(si))
            unfold(si.get.endRollback())
            unfold(beginRollback(sj))
            unfold(sj.get.beginRollback())

            scanIndexingNode(
              tr,
              (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
              init,
              activeStateInFun,
              activeStateOutFun,
              traverseInFun,
              traverseOutFun,
              j,
            )

            findBeginRollbackActiveState(
              tr,
              (ActiveLedgerState.empty, Nil[ActiveLedgerState]()),
              j,
              2 * tr.size - 1,
            )
            scanActiveStateAdvance(tr, init, j)
        }
      }
    }

  }.ensuring(
    (traverseTransaction(tr, init).get.activeState ==
      init.get.activeState.advance(traverseActiveState(tr)._1))
  )

}
