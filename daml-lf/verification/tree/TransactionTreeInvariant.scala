// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import transaction.CSMHelpers._

import transaction.CSMEitherDef._
import transaction.CSMEither._

import transaction.CSMLocallyCreatedProperties._
import transaction.{State}

import transaction.CSMKeysPropertiesDef._

import transaction.CSMInvariantDef._
import transaction.CSMInvariant._

import transaction.ContractStateMachine.{KeyMapping, ActiveLedgerState}

import TransactionTreeDef._

import TransactionTreeChecksDef._
import TransactionTreeChecks._

import TransactionTreeKeys._
import TransactionTreeKeysDef._

/** Properties on how invariants are preserved throughout transaction traversals.
  */
object TransactionTreeInvariant {

  /** If the traversals in [[TransactionTreeChecks]] did not raise any error, then every pair intermediate state - node
    * in the traversal respects the [[transaction.TransactioInvariantDef.stateNodeCompatibility]] condition.
    *
    * @param tr The tree that is being traversed
    * @param init The initial state of the traversal
    * @param i The step number during which the node is processed
    */
  @pure @opaque
  def scanStateNodeCompatibility(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(i < 2 * tr.size)
    require(0 <= i)
    require(init.isRight)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(scanTransaction(tr, init)(i)._1.isRight)

    val (si, n, dir) = scanTransaction(tr, init)(i)
    val ilc = init.get.locallyCreated
    val icons = init.get.consumed

    unfold(
      stateNodeCompatibility(
        si.get,
        n._2,
        traverseUnbound(tr)._1,
        traverseLC(tr, ilc, icons, true)._1,
        dir,
      )
    )

    dir match {
      case TraversalDirection.Up => Trivial()
      case TraversalDirection.Down =>
        scanIndexingNode(
          tr,
          init,
          (ilc, icons, true),
          traverseInFun,
          traverseOutFun,
          buildLC,
          (z, t) => z,
          i,
        )
        scanIndexingNode(
          tr,
          init,
          (Set.empty[ContractId], Set.empty[ContractId], true),
          traverseInFun,
          traverseOutFun,
          unboundFun,
          (z, t) => z,
          i,
        )
        scanTraverseLCPropDown(tr, ilc, icons, true, i)
        scanTraverseUnboundPropDown(tr, i)
        scanTransactionLC(tr, init, true, i)
    }

  }.ensuring(
    stateNodeCompatibility(
      scanTransaction(tr, init)(i)._1.get,
      scanTransaction(tr, init)(i)._2._2,
      traverseUnbound(tr)._1,
      traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      scanTransaction(tr, init)(i)._3,
    )
  )

  /** If the traversals in [[TransactionTreeChecks]] did not raise any error and if the initial state contains all the
    * keys of the tree, then every intermediate state of the transaction traversal preserves the invariants.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param i The step number of the intermediate state
    */
  @pure @opaque
  def scanInvariant(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    require(init.isRight)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    val p: Either[KeyInputError, State] => Boolean = x =>
      stateInvariant(x)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )

    if (!p(scanTransaction(tr, init)(i)._1)) {
      val j = scanNotProp(tr, init, traverseInFun, traverseOutFun, p, i)
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, j + 1)

      val s = scanTransaction(tr, init)(j)._1
      val n = scanTransaction(tr, init)(j)._2
      val dir = scanTransaction(tr, init)(j)._3

      s match {
        case Left(_) =>
          if (dir == TraversalDirection.Down) {
            unfold(propagatesError(s, traverseInFun(s, n)))
          } else {
            unfold(propagatesError(s, traverseOutFun(s, n)))
          }
        case Right(state) =>
          unfold(traverseInFun(s, n))
          unfold(traverseOutFun(s, n))

          n._2 match {
            case a: Node.Action =>
              if (dir == TraversalDirection.Down) {
                containsAllKeysImpliesDown(tr, init, j)
                unfold(containsKey(s)(n._2))
                unfold(containsNodeKey(state)(n._2))
                scanStateNodeCompatibility(tr, init, j)
                handleNodeInvariant(
                  state,
                  n._1,
                  a,
                  traverseUnbound(tr)._1,
                  traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
                )
              }
            case r: Node.Rollback =>
              if (dir == TraversalDirection.Down) {
                unfold(beginRollback(s))
                stateInvariantBeginRollback(
                  state,
                  traverseUnbound(tr)._1,
                  traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
                )
              } else {
                unfold(endRollback(s))
                stateInvariantEndRollback(
                  state,
                  traverseUnbound(tr)._1,
                  traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
                )
              }
          }
      }
    }

  }.ensuring(
    stateInvariant(scanTransaction(tr, init)(i)._1)(
      traverseUnbound(tr)._1,
      traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
    )
  )

  /** If the traversals in [[TransactionTreeChecks]] did not raise any error and if the initial state contains all the
    * keys of the tree, then the state obtained after processing the transaction preserves the invariants.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    */
  @pure
  @opaque
  def traverseInvariant(tr: Tree[(NodeId, Node)], init: Either[KeyInputError, State]): Unit = {
    require(init.isRight)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    if (tr.size == 0) {
      Trivial()
    } else {

      scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
      scanInvariant(tr, init, 2 * tr.size - 1)

      val s = scanTransaction(tr, init)(2 * tr.size - 1)._1
      val n = scanTransaction(tr, init)(2 * tr.size - 1)._2

      s match {
        case Left(_) => unfold(propagatesError(s, traverseOutFun(s, n)))
        case Right(state) =>
          unfold(traverseOutFun(s, n))
          n._2 match {
            case a: Node.Action => Trivial()
            case r: Node.Rollback =>
              unfold(endRollback(s))
              stateInvariantEndRollback(
                state,
                traverseUnbound(tr)._1,
                traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
              )
          }
      }
    }

  }.ensuring(
    stateInvariant(traverseTransaction(tr, init))(
      traverseUnbound(tr)._1,
      traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
    )
  )

}
