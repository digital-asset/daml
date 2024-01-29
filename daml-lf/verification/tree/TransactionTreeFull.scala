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

import transaction.CSMHelpers._

import transaction.CSMEitherDef._
import transaction.CSMEither._

import transaction.CSMKeysPropertiesDef._
import transaction.CSMKeysProperties._

import transaction.CSMAdvance._
import transaction.CSMInvariantDef._

import transaction.{State}
import transaction.ContractStateMachine.{KeyMapping}

import TransactionTree._
import TransactionTreeDef._
import TransactionTreeKeys._
import TransactionTreeKeysDef._
import TransactionTreeChecksDef._
import TransactionTreeChecks._
import TransactionTreeInconsistency._
import TransactionTreeAdvance._
import TransactionTreeAdvanceDef._

/** In the contract state maching, handling a node comes in two major step:
  * - Adding the node's key to the global keys with its corresponding mapping
  * - Processing the node
  * In the simplified version of the contract state machine, this behavior is respectively split in two different
  * functions [[transaction.CSMKeysPropertiesDef.addKeyBeforeNode]] and [[transaction.State.handleNode]]
  *
  * A key property of transaction traversal is that one can first add the key-mapping pairs of every node in the
  * globalKeys and then process the transaction. The proof of this claims lies in [[TransactionTreeFull.scanTransactionFullCommute]].
  */

object TransactionTreeFullDef {

  /** Function called when a node is entered for the first time ([[utils.TraversalDirection.Down]]).
    *  - If the node is an instance of [[transaction.Node.Action]] we add its key and its corresponding
    *    mapping. Then, we handle it.
    *  - If it is a [[transaction.Node.Rollback]] node we call [[transaction.State.beginRollback]].
    *
    * Among the direct properties one can deduce we have that
    *  - If the state already contains the node's key, then the function behaves in the same way that
    *    [[TransactionTreeDef.traverseInFun]]
    *  - if the inital state is an error then the result is also an error
    *
    * @param s State before entering the node for the first time
    * @param p Node and its id
    */
  @pure
  @opaque
  def traverseInFunFull(
      s: Either[KeyInputError, State],
      p: (NodeId, Node),
  ): Either[KeyInputError, State] = {
    val res = traverseInFun(addKeyBeforeNode(s, p), p)
    propagatesErrorTransitivity(s, addKeyBeforeNode(s, p), res)
    res
  }.ensuring(res =>
    propagatesError(s, res) &&
      (containsKey(s)(p._2) ==> (res == traverseInFun(s, p)))
  )

  /** Tree traversal that adds all the necessary keys in the tree to the initial state.
    * Returns a list of triples whose entries are:
    *  - The intermediate states of the traversal
    *  - The nodes that are being processed
    *  - The traversal directions (i.e. if the nodes are visited for the first or the second time)
    */
  @pure
  def scanAddKey(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): List[(Either[KeyInputError, State], (NodeId, Node), TraversalDirection)] = {
    tr.scan(init, addKeyBeforeNode, (z, t) => z)
  }

  /** Tree traversal that adds all the necessary keys in the tree to the initial state.
    */
  @pure
  def traverseAddKey(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Either[KeyInputError, State] = {
    tr.traverse(init, addKeyBeforeNode, (z, t) => z)
  }

  /** List of triples whose respective entries are:
    *  - The state before the i-th step of the traversal
    *  - The pair node id - node that is handle during the i-th step
    *  - The direction i.e. if that's the first or the second time we enter the node
    *
    * @param tr   The transaction that is being processed
    * @param init The initial state of the transaction
    */
  @pure
  def scanTransactionFull(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): List[(Either[KeyInputError, State], (NodeId, Node), TraversalDirection)] = {
    tr.scan(init, traverseInFunFull, traverseOutFun)
  }

  /** Resulting state after a transaction traversal.
    *
    * @param tr   The transaction that is being processed
    * @param init The initial state of the transaction
    */
  @pure
  def traverseTransactionFull(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Either[KeyInputError, State] = {
    tr.traverse(init, traverseInFunFull, traverseOutFun)
  }
}

object TransactionTreeFull {

  import TransactionTreeFullDef._

  /** Adding all the keys to a state  up to a given point in a tree traversal is equivalent to collecting all of them
    * and concatenating them to the global keys of the state.
    *
    * @param tr The tree that is being traversed
    * @param init The state to which we are adding the keys
    * @param i The point up to which we gather the keys
    */
  @pure @opaque
  def scanAddKeyIsConcatLeftGlobalKey(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)

    scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, i)
    scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, i)
    if (i == 0) {
      unfold(concatLeftGlobalKeys(init, collectTrace(tr)(i)._1))
      if (init.isRight) {
        unfold(concatLeftGlobalKeys(init.get, collectTrace(tr)(i)._1))
        MapProperties.concatEmpty(init.get.globalKeys)
        MapAxioms.extensionality(
          init.get.globalKeys,
          Map.empty[GlobalKey, KeyMapping] ++ init.get.globalKeys,
        )
      }
    } else {
      scanAddKeyIsConcatLeftGlobalKey(tr, init, i - 1)
      scanIndexingNode(
        tr,
        Map.empty[GlobalKey, KeyMapping],
        init,
        collectFun,
        (z, t) => z,
        addKeyBeforeNode,
        (z, t) => z,
        i - 1,
      )
      val (si, n, dir) = scanAddKey(tr, init)(i - 1)
      val ci = collectTrace(tr)(i - 1)._1

      if (dir == TraversalDirection.Down) {
        collectFunConcat(ci, n)
        concatLeftGlobalKeysAssociativity(init, ci, nodeKeyMap(n._2))
      }
    }
  }.ensuring(scanAddKey(tr, init)(i)._1 == concatLeftGlobalKeys(init, collectTrace(tr)(i)._1))

  /** Adding all the keys of a tree to a state is equivalent to collecting all of them
    * and concatenating them to the global keys of the state.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    */
  @pure
  @opaque
  def traverseAddKeyIsConcatLeftGlobalKey(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {

    if (tr.size > 0) {
      scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, 0)
      scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, 0)
      scanAddKeyIsConcatLeftGlobalKey(tr, init, 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        Map.empty[GlobalKey, KeyMapping],
        init,
        collectFun,
        (z, t) => z,
        addKeyBeforeNode,
        (z, t) => z,
        2 * tr.size - 1,
      )
      val (si, n, dir) = scanAddKey(tr, init)(2 * tr.size - 1)
      val ci = collectTrace(tr)(2 * tr.size - 1)._1

      if (dir == TraversalDirection.Down) {
        collectFunConcat(ci, n)
        concatLeftGlobalKeysAssociativity(init, ci, nodeKeyMap(n._2))
      }
    } else {
      unfold(concatLeftGlobalKeys(init, collect(tr)))
      if (init.isRight) {
        unfold(concatLeftGlobalKeys(init.get, collect(tr)))
        MapProperties.concatEmpty(init.get.globalKeys)
        MapAxioms.extensionality(
          init.get.globalKeys,
          Map.empty[GlobalKey, KeyMapping] ++ init.get.globalKeys,
        )
      }
    }
  }.ensuring(traverseAddKey(tr, init) == concatLeftGlobalKeys(init, collect(tr)))

  /** If a state contains all the keys of a transaction up to a given point, then first concatenating to the left new
    * keys to its glboal keys and processing the transaction afterward up to that point, is the same as first processing
    * the transaction and then adding the keys. The operations commute.
    * @param tr The tree that is being traversed
    * @param init The initial state of the traversal
    * @param i The point up to which the transaction is being processed
    * @param glK The keys that are being concatenated to the globalKeys of the state
    */
  @pure
  @opaque
  def scanTransactionConcatLeftGlobalKeys(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(containsAllKeysBefore(tr, init, i))

    scanIndexingState(tr, concatLeftGlobalKeys(init, glK), traverseInFun, traverseOutFun, i)
    scanIndexingState(tr, init, traverseInFun, traverseOutFun, i)

    if (i == 0) {
      Trivial()
    } else {
      scanIndexingNode(
        tr,
        init,
        concatLeftGlobalKeys(init, glK),
        traverseInFun,
        traverseOutFun,
        traverseInFun,
        traverseOutFun,
        i - 1,
      )
      containsAllKeysBeforeImplies(tr, init, i, i - 1)
      scanTransactionConcatLeftGlobalKeys(tr, init, i - 1, glK)

      val (si, n, dir) = scanTransaction(tr, init)(i - 1)
      val sci = scanTransaction(tr, concatLeftGlobalKeys(init, glK))(i - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFun(si, n))
        unfold(traverseInFun(sci, n))
        n._2 match {
          case a: Node.Action =>
            scanIndexingNode(
              tr,
              init,
              true,
              traverseInFun,
              traverseOutFun,
              containsAllKeysFun(init),
              containsAllKeysFun(init),
              i - 1,
            )
            scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), i)
            unfold(containsAllKeysFun(init)(containsAllKeysBefore(tr, init, i - 1), n))
            scanTransactionProp(tr, init, i - 1)
            containsKeySameGlobalKeys(init, si, n._2)
            handleNodeConcatLeftGlobalKeys(si, n._1, a, glK)
          case r: Node.Rollback => beginRollbackConcatLeftGlobalKeys(si, glK)
        }
      } else {
        unfold(traverseOutFun(si, n))
        unfold(traverseOutFun(sci, n))
        endRollbackConcatLeftGlobalKeys(si, glK)
      }
    }

  }.ensuring(
    scanTransaction(tr, concatLeftGlobalKeys(init, glK))(i)._1 ==
      concatLeftGlobalKeys(scanTransaction(tr, init)(i)._1, glK)
  )

  /** If a state contains all the keys of a transaction up to a given point, then first adding a key mapping and processing
    * the transaction afterward up to that point, is the same as first processing the transaction and then adding the
    * key. The operations commute.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param i    The point up to which the transaction is being processed
    * @param n    The node whose key is added.
    */
  @pure
  @opaque
  def scanTransactionAddKeyBeforeNode(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
      n: Node,
  ): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    require(containsAllKeysBefore(tr, init, i))
    scanTransactionConcatLeftGlobalKeys(tr, init, i, nodeKeyMap(n))
  }.ensuring(
    scanTransaction(tr, addKeyBeforeNode(init, n))(i)._1 ==
      addKeyBeforeNode(scanTransaction(tr, init)(i)._1, n)
  )

  /** For any point in time adding all the necessary keys at the beginning and then processing the transaction is
    * equivalent to add the key and then processing the node for every step.
    *
    * @param tr The transaction
    * @param init The initial state of the traversal
    * @param i The point up to which the transaction is being processed
    */
  @pure
  @opaque
  def scanTransactionFullCommute(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(init.isRight)

    scanIndexingNode(
      tr,
      scanAddKey(tr, init)(i)._1,
      init,
      traverseInFun,
      traverseOutFun,
      traverseInFunFull,
      traverseOutFun,
      i,
    )
    scanIndexingState(tr, init, traverseInFunFull, traverseOutFun, i)
    scanIndexingState(tr, scanAddKey(tr, init)(i)._1, traverseInFun, traverseOutFun, i)
    scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, i)

    if (i == 0) {
      Trivial()
    } else {
      scanTransactionFullCommute(tr, init, i - 1)
      scanIndexingNode(
        tr,
        scanAddKey(tr, init)(i)._1,
        init,
        traverseInFun,
        traverseOutFun,
        traverseInFunFull,
        traverseOutFun,
        i - 1,
      )
      scanIndexingNode(
        tr,
        init,
        init,
        traverseInFunFull,
        traverseOutFun,
        addKeyBeforeNode,
        (z, t) => z,
        i - 1,
      )

      val (tfi, n, dir) = scanTransactionFull(tr, init)(i - 1)
      val ti = scanTransaction(tr, scanAddKey(tr, init)(i)._1)(i - 1)._1
      val si = scanAddKey(tr, init)(i - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFunFull(tfi, n))
        scanAddKeyContainsAllKeysBefore(tr, init, i - 1)
        scanTransactionAddKeyBeforeNode(tr, scanAddKey(tr, init)(i - 1)._1, i - 1, n._2)
      }
    }

  }.ensuring(scanTransactionFull(tr, init)(i) == scanTransaction(tr, scanAddKey(tr, init)(i)._1)(i))

  /** Adding all the necessary keys at the beginning and then processing the transaction is
    * equivalent to add the key and then processing the node for every step.
    *
    * @param tr   The transaction
    * @param init The initial state of the traversal
    */
  @pure
  @opaque
  def traverseTransactionFullCommute(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {
    require(init.isRight)

    if (tr.size > 0) {
      scanIndexingState(tr, init, traverseInFunFull, traverseOutFun, 0)
      scanIndexingState(tr, traverseAddKey(tr, init), traverseInFun, traverseOutFun, 0)
      scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, 0)

      scanTransactionFullCommute(tr, init, 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        traverseAddKey(tr, init),
        init,
        traverseInFun,
        traverseOutFun,
        traverseInFunFull,
        traverseOutFun,
        2 * tr.size - 1,
      )
      scanIndexingNode(
        tr,
        init,
        init,
        traverseInFunFull,
        traverseOutFun,
        addKeyBeforeNode,
        (z, t) => z,
        2 * tr.size - 1,
      )

      val (tfi, n, dir) = scanTransactionFull(tr, init)(2 * tr.size - 1)
      val ti = scanTransaction(tr, traverseAddKey(tr, init))(2 * tr.size - 1)._1
      val si = scanAddKey(tr, init)(2 * tr.size - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFunFull(tfi, n))
        scanAddKeyContainsAllKeysBefore(tr, init, 2 * tr.size - 1)
        scanTransactionAddKeyBeforeNode(
          tr,
          scanAddKey(tr, init)(2 * tr.size - 1)._1,
          2 * tr.size - 1,
          n._2,
        )
      }
    }

  }.ensuring(traverseTransactionFull(tr, init) == traverseTransaction(tr, traverseAddKey(tr, init)))

  /** Given a node in a tree traversal that adds all the necessary key of the tree to an initial state, any intermediate
    * state in the traversal that comes after the node will contain its key
    *
    * @param tr The tree that is being traversed
    * @param init The initial state of the transaction
    * @param i The step number of the node
    * @param j The step number of the interemediate state
    */
  @pure
  @opaque
  def scanAddKeyContains(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(j)
    require(i >= 0)
    require(i < j)
    require(j < 2 * tr.size)
    require(scanAddKey(tr, init)(i)._3 == TraversalDirection.Down)

    scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, j)
    if (j == i + 1) {
      Trivial()
    } else {
      scanAddKeyContains(tr, init, i, j - 1)
      containsKeyAddKeyBeforeNode(
        scanAddKey(tr, init)(j - 1)._1,
        scanAddKey(tr, init)(j - 1)._2._2,
        scanAddKey(tr, init)(i)._2._2,
      )
    }

  }.ensuring(containsKey(scanAddKey(tr, init)(j)._1)(scanAddKey(tr, init)(i)._2._2))

  /** If a state contains all the keys of a tree up to a given point, then adding a key to the global keys of the state
    * does not change the truth of the statement.
    */
  @pure
  @opaque
  def containsAllKeysBeforeAddKeyBeforeNode(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      n: Node,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(containsAllKeysBefore(tr, init, i))

    scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), i)
    scanIndexingState(
      tr,
      true,
      containsAllKeysFun(addKeyBeforeNode(init, n)),
      containsAllKeysFun(addKeyBeforeNode(init, n)),
      i,
    )
    if (i == 0) {
      Trivial()
    } else {
      scanIndexingNode(
        tr,
        true,
        true,
        containsAllKeysFun(init),
        containsAllKeysFun(init),
        containsAllKeysFun(addKeyBeforeNode(init, n)),
        containsAllKeysFun(addKeyBeforeNode(init, n)),
        i - 1,
      )
      val (s1, ni, dir) = tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i - 1)
      val s2 = tr
        .scan(
          true,
          containsAllKeysFun(addKeyBeforeNode(init, n)),
          containsAllKeysFun(addKeyBeforeNode(init, n)),
        )(i - 1)
        ._1
      unfold(containsAllKeysFun(init)(s1, ni))
      unfold(containsAllKeysFun(addKeyBeforeNode(init, n))(s2, ni))
      containsKeyAddKeyBeforeNode(init, n, ni._2)
      containsAllKeysBeforeAddKeyBeforeNode(tr, init, n, i - 1)
    }

  }.ensuring(containsAllKeysBefore(tr, addKeyBeforeNode(init, n), i))

  /** Any intermediate state of the traversal that adds all the keys of the tree to the initial state contains all the keys
    * of the tree up to that point
    *
    * @param tr Tne tree that is being traversed
    * @param init The initial state of the traversal
    * @param i The step number of the intermediate state
    */
  @pure
  @opaque
  def scanAddKeyContainsAllKeysBefore(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)

    containsAllKeysBeforeAlt(tr, scanAddKey(tr, init)(i)._1, i)
    scanIndexingState(tr, true, containsAllKeysFun(scanAddKey(tr, init)(i)._1), (z, t) => z, i)
    scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, i)
    if (i == 0) {
      Trivial()
    } else {
      containsAllKeysBeforeAlt(tr, scanAddKey(tr, init)(i)._1, i - 1)
      scanIndexingNode(
        tr,
        init,
        true,
        addKeyBeforeNode,
        (z, t) => z,
        containsAllKeysFun(scanAddKey(tr, init)(i)._1),
        (z, t) => z,
        i - 1,
      )
      scanAddKeyContainsAllKeysBefore(tr, init, i - 1)

      val si = tr.scan(init, addKeyBeforeNode, (z, t) => z)(i - 1)._1
      val (ci, ni, dir) =
        tr.scan(true, containsAllKeysFun(scanAddKey(tr, init)(i)._1), (z, t) => z)(i - 1)

      if (dir == TraversalDirection.Down) {
        unfold(containsAllKeysFun(scanAddKey(tr, init)(i)._1)(ci, ni))
        scanAddKeyContains(tr, init, i - 1, i)
        containsAllKeysBeforeAddKeyBeforeNode(tr, si, ni._2, i - 1)
      }
    }

  }.ensuring(containsAllKeysBefore(tr, scanAddKey(tr, init)(i)._1, i))

  /** The final state of the traversal that adds all the keys of the tree to the initial state contains all the keys
    * of the tree.
    *
    * @param tr   Tne tree that is being traversed
    * @param init The initial state of the traversal
    */
  @pure
  @opaque
  def traverseAddKeyContainsAllKeys(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {

    if (tr.size == 0) {
      Trivial()
    } else {
      containsAllKeysAlt(tr, traverseAddKey(tr, init))
      scanIndexingState(tr, true, containsAllKeysFun(traverseAddKey(tr, init)), (z, t) => z, 0)
      scanIndexingState(tr, init, addKeyBeforeNode, (z, t) => z, 0)
      containsAllKeysBeforeAlt(tr, traverseAddKey(tr, init), 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        init,
        true,
        addKeyBeforeNode,
        (z, t) => z,
        containsAllKeysFun(traverseAddKey(tr, init)),
        (z, t) => z,
        2 * tr.size - 1,
      )
      scanAddKeyContainsAllKeysBefore(tr, init, 2 * tr.size - 1)

      val si = tr.scan(init, addKeyBeforeNode, (z, t) => z)(2 * tr.size - 1)._1
      val (ci, ni, dir) =
        tr.scan(true, containsAllKeysFun(traverseAddKey(tr, init)), (z, t) => z)(2 * tr.size - 1)

      if (dir == TraversalDirection.Down) {
        unfold(containsAllKeysFun(scanAddKey(tr, init)(2 * tr.size - 1)._1)(ci, ni))
//        scanAddKeyContains(tr, init, i - 1, i)
        containsAllKeysBeforeAddKeyBeforeNode(tr, si, ni._2, 2 * tr.size - 1)
      }
    }

  }.ensuring(containsAllKeys(tr, traverseAddKey(tr, init)))

  @pure @opaque
  def traverseTransactionFullEmpty(tr: Tree[(NodeId, Node)]): Unit = {

    val remptyNoKey = Right[KeyInputError, State](State.empty)
    val trEmpty = traverseAddKey(tr, remptyNoKey)

    traverseTransactionFullCommute(tr, remptyNoKey)
    traverseAddKeyIsConcatLeftGlobalKey(tr, remptyNoKey)

    unfold(concatLeftGlobalKeys(State.empty, collect(tr)))

    MapProperties.concatEmpty(collect(tr))
    MapAxioms.extensionality(collect(tr) ++ Map.empty[GlobalKey, KeyMapping], collect(tr))

    unfold(emptyState(tr))

    unfold(sameConsumed(remptyNoKey, trEmpty))
    unfold(sameConsumed(State.empty, trEmpty))

    unfold(sameActiveState(remptyNoKey, trEmpty))
    unfold(sameActiveState(State.empty, trEmpty))

    unfold(sameLocallyCreated(remptyNoKey, trEmpty))
    unfold(sameLocallyCreated(State.empty, trEmpty))

    unfold(sameStack(remptyNoKey, trEmpty))
    unfold(sameStack(State.empty, trEmpty))

  }.ensuring(
    traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)) ==
      traverseTransaction(tr, Right[KeyInputError, State](emptyState(tr)))
  )

  /** The globalKeys of the resulting state of a transaction traversal starting from the empty state is the map
    * obtained from collecting the keys of the tree.
    */
  @pure
  @opaque
  def traverseTransactionFullEmptyKeys(tr: Tree[(NodeId, Node)]): Unit = {

    require(traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).isRight)

    val remptyNoKey = Right[KeyInputError, State](State.empty)
    val rempty = Right[KeyInputError, State](emptyState(tr))
    val trFullEmpty = traverseTransactionFull(tr, remptyNoKey)

    traverseTransactionFullEmpty(tr)
    traverseTransactionProp(tr, rempty)
    unfold(sameGlobalKeys(rempty, trFullEmpty))
    unfold(sameGlobalKeys(emptyState(tr), trFullEmpty))
    unfold(emptyState(tr))

  }.ensuring(
    traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).get.globalKeys == collect(
      tr
    )
  )

  /** Advance is defined if and only if the map obtained by gathering the keys of the trees is a submap of the active keys
    * of the state on which advance is applied to.
    *
    * Unfortunately due to a bug with lambdas in stainless the proof does not verify so we ignore it.
    */
  @pure
  @opaque
  @dropVCs
  def traverseTransactionAdvanceDefined(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {

    require(init.isRight)
    require(traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).isRight)
    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(traverseAddKey(tr, init))(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(
      stateInvariant(Right[KeyInputError, State](emptyState(tr)))(
        traverseUnbound(tr)._1,
        traverseLC(tr, Set.empty[ContractId], Set.empty[ContractId], true)._1,
      )
    )
    require(
      !traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).get.withinRollbackScope
    )

    val remptyNoKey = Right[KeyInputError, State](State.empty)
    val trFullEmpty = traverseTransactionFull(tr, remptyNoKey)
    val trFull = traverseTransactionFull(tr, init)
    val trKey = traverseAddKey(tr, init)
    val rempty = Right[KeyInputError, State](emptyState(tr))

    traverseAddKeyIsConcatLeftGlobalKey(tr, init)

    @pure @opaque
    def trKeyProp: Unit = {
      unfold(propagatesError(init, trKey))

      unfold(sameActiveState(init, trKey))
      unfold(sameActiveState(init.get, trKey))

      unfold(sameConsumed(init, trKey))
      unfold(sameConsumed(init.get, trKey))

    }.ensuring(
      trKey.isRight &&
        (trKey.get.consumed == init.get.consumed) &&
        (trKey.get.activeState == init.get.activeState)
    )

    trKeyProp

    @pure
    @opaque
    def traverseTransactionAdvanceCondition: Unit = {

      unfold(emptyState(tr))
      unfold(concatLeftGlobalKeys(init.get, collect(tr)))

      traverseTransactionFullEmptyKeys(tr)

      val p1: GlobalKey => Boolean = k => trKey.get.activeKeys.get(k) == collect(tr).get(k)
      val p2: GlobalKey => Boolean =
        k => init.get.activeKeys.get(k).forall(m => Some(m) == trFullEmpty.get.globalKeys.get(k))

      if (collect(tr).keySet.forall(p1) && !trFullEmpty.get.globalKeys.keySet.forall(p2)) {
        val k = SetProperties.notForallWitness(collect(tr).keySet, p2)
        SetProperties.forallContains(collect(tr).keySet, p1, k)
        activeKeysGet(trKey.get, k)
        activeKeysGet(init.get, k)
        MapAxioms.concatGet(collect(tr), init.get.globalKeys, k)
      } else if (!collect(tr).keySet.forall(p1) && trFullEmpty.get.globalKeys.keySet.forall(p2)) {
        val k = SetProperties.notForallWitness(collect(tr).keySet, p1)
        SetProperties.forallContains(collect(tr).keySet, p2, k)
        activeKeysGet(trKey.get, k)
        activeKeysGet(init.get, k)
        MapAxioms.concatGet(collect(tr), init.get.globalKeys, k)

        if (!init.get.activeKeys.get(k).isDefined) {
          // true bc of invariant
          assert(trKey.get.activeKeys.get(k) == collect(tr).get(k))
        }
      }
    }.ensuring(
      emptyState(tr).globalKeys.keySet.forall(k =>
        trKey.get.activeKeys.get(k) == emptyState(tr).globalKeys.get(k)
      ) ==
        trFullEmpty.get.globalKeys.keySet.forall(k =>
          init.get.activeKeys.get(k).forall(m => Some(m) == trFullEmpty.get.globalKeys.get(k))
        )
    )

    advanceIsDefined(init.get, trFullEmpty.get)
    assert(
      init.get.advance(trFullEmpty.get).isRight ==
        trFullEmpty.get.globalKeys.keySet.forall(k =>
          init.get.activeKeys.get(k).forall(m => Some(m) == trFullEmpty.get.globalKeys.get(k))
        )
    )

    traverseTransactionAdvanceCondition
    assert(
      emptyState(tr).globalKeys.keySet.forall(k =>
        trKey.get.activeKeys.get(k) == emptyState(tr).globalKeys.get(k)
      ) ==
        trFullEmpty.get.globalKeys.keySet.forall(k =>
          init.get.activeKeys.get(k).forall(m => Some(m) == trFullEmpty.get.globalKeys.get(k))
        )
    )

    traverseTransactionFullEmpty(tr)
    traverseAddKeyContainsAllKeys(tr, init)
    traverseTransactionEmptyDefined(tr, trKey)
    assert(
      emptyState(tr).globalKeys.keySet.forall(k =>
        trKey.get.activeKeys.get(k) == emptyState(tr).globalKeys.get(k)
      ) ==
        traverseTransaction(tr, trKey).isRight
    )

    traverseTransactionFullCommute(tr, init)
    assert(
      traverseTransactionFull(tr, init).isRight ==
        traverseTransaction(tr, trKey).isRight
    )

  }.ensuring(
    (init.get
      .advance(traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).get)
      .isRight ==
      traverseTransactionFull(tr, init).isRight)
  )

  /** If the advance method is defined, the traversing a transaction from a given initial state is the same as calling
    * advance on this state with the final state of the empty state traversal as argument.
    *
    * @param tr The tree being traversed
    * @param init The initial state of the traversal
    */
  @pure
  @opaque
  def traverseTransactionAdvance(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {

    require(init.isRight)
    require(traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).isRight)
    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(traverseAddKey(tr, init))(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(
      stateInvariant(Right[KeyInputError, State](emptyState(tr)))(
        traverseUnbound(tr)._1,
        traverseLC(tr, Set.empty[ContractId], Set.empty[ContractId], true)._1,
      )
    )
    require(
      !traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).get.withinRollbackScope
    )

    val remptyNoKey = Right[KeyInputError, State](State.empty)
    val trFullEmpty = traverseTransactionFull(tr, remptyNoKey)
    val trFull = traverseTransactionFull(tr, init)
    val trKey = traverseAddKey(tr, init)
    val rempty = Right[KeyInputError, State](emptyState(tr))

    @pure
    @opaque
    def trKeyProp: Unit = {
      traverseAddKeyIsConcatLeftGlobalKey(tr, init)
      unfold(propagatesError(init, trKey))

      unfold(sameActiveState(init, trKey))
      unfold(sameActiveState(init.get, trKey))

      unfold(sameConsumed(init, trKey))
      unfold(sameConsumed(init.get, trKey))

    }.ensuring(
      trKey.isRight &&
        (trKey.get.consumed == init.get.consumed) &&
        (trKey.get.activeState == init.get.activeState)
    )

    traverseTransactionAdvanceDefined(tr, init)

    if (traverseTransactionFull(tr, init).isRight) {

      trKeyProp
      unfold(init.get.advance(trFullEmpty.get))
      traverseTransactionFullCommute(tr, init)
      traverseTransactionFullEmpty(tr)
      unfold(emptyState(tr))

      // activeState
      traverseActiveStateAdvance(tr, trKey)
      traverseActiveStateAdvance(tr, rempty)
      unfold(sameActiveState(remptyNoKey, rempty))
      unfold(sameActiveState(State.empty, rempty))
      emptyAdvance(traverseActiveState(tr)._1)

      // key
      traverseAddKeyIsConcatLeftGlobalKey(tr, init)
      unfold(concatLeftGlobalKeys(init.get, collect(tr)))
      traverseTransactionProp(tr, trKey)
      unfold(sameGlobalKeys(trKey, trFull))
      unfold(sameGlobalKeys(trKey.get, trFull))
      traverseTransactionFullEmptyKeys(tr)

      // locallyCreated and consumed
      traverseTransactionLC(tr, trKey, true)
      traverseTransactionLC(tr, rempty, true)
      traverseLCExtractInitLC(
        tr,
        init.get.locallyCreated,
        init.get.consumed,
        Set.empty[ContractId],
        true,
      )
      SetProperties.unionCommutativity(
        traverseTransaction(tr, rempty).get.locallyCreated,
        init.get.locallyCreated,
      )
      SetProperties.equalsTransitivity(
        init.get.locallyCreated ++ traverseTransaction(tr, rempty).get.locallyCreated,
        traverseTransaction(tr, rempty).get.locallyCreated ++ init.get.locallyCreated,
        traverseTransactionFull(tr, init).get.locallyCreated,
      )
      SetAxioms.extensionality(
        init.get.locallyCreated ++ traverseTransaction(tr, rempty).get.locallyCreated,
        traverseTransactionFull(tr, init).get.locallyCreated,
      )

      traverseLCExtractInitConsumed(
        tr,
        init.get.locallyCreated,
        Set.empty[ContractId],
        init.get.consumed,
        true,
      )
      SetProperties.unionCommutativity(
        traverseTransaction(tr, rempty).get.consumed,
        init.get.consumed,
      )
      SetProperties.equalsTransitivity(
        init.get.consumed ++ traverseTransaction(tr, rempty).get.consumed,
        traverseTransaction(tr, rempty).get.consumed ++ init.get.consumed,
        traverseTransactionFull(tr, init).get.consumed,
      )
      SetAxioms.extensionality(
        init.get.consumed ++ traverseTransaction(tr, rempty).get.consumed,
        traverseTransactionFull(tr, init).get.consumed,
      )

      // rollbackStack
      traverseTransactionProp(tr, trKey)
      sameStackTransitivity(init, trKey, trFull)
      unfold(sameStack(init, trFull))
      unfold(sameStack(init.get, trFull))

    }

  }.ensuring(
    (traverseTransactionFull(tr, init).isRight ==>
      (init.get
        .advance(traverseTransactionFull(tr, Right[KeyInputError, State](State.empty)).get)
        .get ==
        traverseTransactionFull(tr, init).get))
  )

}
