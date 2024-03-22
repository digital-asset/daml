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

import transaction.{State}
import transaction.CSMHelpers._
import transaction.CSMKeysPropertiesDef._
import transaction.CSMKeysProperties._
import transaction.ContractStateMachine.{KeyMapping, ActiveLedgerState}

import TransactionTreeDef._
import TransactionTree._

/** This files introduces two tree traversals:
  * - [[TransactionTreeKeysDef.containsAllKeys] takes a state and checks that it contains all the keys
  * that appear in the tree.
  * - [[TransactionTreeKeysDef.collect]] collects all the keys, with the mapping of the node in which
  *  they appeared for the first time, into a map.
  *
  *  In fact in [[TransactionTreeFull]] we prove that handling a transaction is equivalent to collecting first all the
  *  keys into a map, concatenating it with the global keys of the initial state and then finally traversing the tree
  *  without modifying the globalKeys.
  */
object TransactionTreeKeysDef {

  /** Indicates whether a [[GlobalKey]] k is equals to the key of the [[Node]] that is being traversed for the first time
    * during the i-th step of a [[utils.Tree]] traversal. If that's the second time the node is visited or if the node
    * has not a well-defined key, returns false.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param f1   The function that is executed when a node of the tree is traversed for the first time
    * @param f2   The function that is executed when a node of the tree is traversed for the second time
    * @param k    The key we are querying
    * @param i    The step number in the traversal
    */
  @pure
  @opaque
  def appearsAtIndex[Z](
      tr: Tree[(NodeId, Node)],
      init: Z,
      f1: (Z, (NodeId, Node)) => Z,
      f2: (Z, (NodeId, Node)) => Z,
      k: GlobalKey,
      i: BigInt,
  ): Boolean = {
    require(i >= 0)
    require(i < 2 * tr.size)

    tr.scan(init, f1, f2)(i)._2._2 match {
      case a: Node.Action
          if (tr.scan(init, f1, f2)(i)._3 == TraversalDirection.Down) && (a.gkeyOpt == Some(k)) =>
        true
      case _ => false
    }
  }

  /** Indicates whether a [[GlobalKey]] did not yet appear before a given step in a tree traversal.
    *
    * More precisely, asserts that k is not equals to the key of the [[Node]] that is being traversed for the first
    * time during all the steps between 0 and i excluded of a [[utils.Tree]] traversal. If that's the second time a node
    * is visited or if the node has not a well-defined key, it is ignored.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param f1   The function that is executed when a node of the tree is traversed for the first time
    * @param f2   The function that is executed when a node of the tree is traversed for the second time
    * @param k    The key we are querying
    * @param i    The strict upper bound on the checked steps
    * @see The corresponding latex document for a pen and paper definition
    */
  @pure
  @opaque
  def doesNotAppearBefore[Z](
      tr: Tree[(NodeId, Node)],
      init: Z,
      f1: (Z, (NodeId, Node)) => Z,
      f2: (Z, (NodeId, Node)) => Z,
      k: GlobalKey,
      i: BigInt,
  ): Boolean = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)

    if (i == 0) {
      true
    } else {
      !appearsAtIndex(tr, init, f1, f2, k, i - 1) && doesNotAppearBefore(tr, init, f1, f2, k, i - 1)
    }
  }

  /** Indicates whether a [[GlobalKey]] apears for the first time at step i in a tree traversal
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param f1   The function that is executed when a node of the tree is traversed for the first time
    * @param f2   The function that is executed when a node of the tree is traversed for the second time
    * @param k    The key we are querying
    * @param i    The step in which the key appears for the first time
    * @see The corresponding latex document for a pen and paper definition
    */
  @pure
  @opaque
  def firstAppears[Z](
      tr: Tree[(NodeId, Node)],
      init: Z,
      f1: (Z, (NodeId, Node)) => Z,
      f2: (Z, (NodeId, Node)) => Z,
      k: GlobalKey,
      i: BigInt,
  ): Boolean = {
    require(i >= 0)
    require(i < 2 * tr.size)

    appearsAtIndex(tr, init, f1, f2, k, i) && doesNotAppearBefore(tr, init, f1, f2, k, i)
  }

  /** Function used the first time we visit a [[Node]] during the [[Tree]] traversal that checks whether a [[State]]
    * contains all the [[GlobalKey]] of the tree.
    *
    * Since the result of the function is cumulative, then the function can returns true only if b is true as well.
    *
    * @param init The state that is being checked. This is an hyperparameter of the function that does not change
    *             throughout the traversal
    * @param b The status of the condition we are checking. If it is set to false then we already found earlier
    *          in the tree a key such that init does not contain it
    * @param n The node we are currently checking
    */
  @pure
  @opaque
  def containsAllKeysFun(
      init: Either[KeyInputError, State]
  )(b: Boolean, n: (NodeId, Node)): Boolean = {
    b && containsKey(init)(n._2)
  }.ensuring(res => (res ==> b))

  /** Checks whether a state contains all the keys of a tree up to a given step of the traversal.
    *
    * Note that in the traversal [[containsAllKeysFun]] is used when visiting a [[Node]] for both the first and the second
    * time. This choice makes some statements easier to prove. Others are easier to prove with the alternative version
    * [[scanContainsAllKeys]] where the identity function is used when visiting a [[Node]] for the second time.
    *
    * @param tr The tree that is being traversed
    * @param init The states for which we are checking the keys
    * @param i The step at which we stop the traversal (exclusive)
    */
  @pure
  def containsAllKeysBefore(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Boolean = {
    require(i >= 0)
    require(i < 2 * tr.size)
    tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i)._1
  }

  /** Checks whether a state contains all the keys of a tree.
    *
    * Note that in the traversal [[containsAllKeysFun]] is used when visiting a [[Node]] for both the first and the second
    * time. This choice makes some statements easier to prove. Others are easier to prove with the alternative version
    * [[traverseContainsAllKeys]] where the identity function is used when visiting a [[Node]] for the second time.
    *
    * @param tr   The tree that is being traversed
    * @param init The states which we are checking the keys
    */
  @pure
  def containsAllKeys(tr: Tree[(NodeId, Node)], init: Either[KeyInputError, State]): Boolean = {
    tr.traverse(true, containsAllKeysFun(init), containsAllKeysFun(init))
  }

  /** List of triples whose entries are:
    *  - Whether the state given as argument contains all the keys of a tree up to a given step of the traversal
    *  - The node that is being processed during this step
    *  - A traversal direction i.e. whether this is the first or the second time we visit the node
    *
    * Note that in the traversal [[containsAllKeysFun]] is used when visiting a [[Node]] only the first
    * time. This choice makes some statements easier to prove. Others are easier to prove with the alternative version
    * [[containsAllKeys]] where the function is used when visiting a [[Node]] both the first and the second time.
    *
    * @param tr   The tree that is being traversed
    * @param init The states which we are checking the keys
    */
  @pure
  def scanContainsAllKeys(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): List[(Boolean, (NodeId, Node), TraversalDirection)] = {
    tr.scan(true, containsAllKeysFun(init), (z, t) => z)
  }

  /** Checks whether a state contains all the keys of a tree.
    *
    * Note that in the traversal [[containsAllKeysFun]] is used when visiting a [[Node]] only the first
    * time. This choice makes some statements easier to prove. Others are easier to prove with the alternative version
    * [[containsAllKeys]] where the function is used when visiting a [[Node]] both the first and the second time.
    *
    * @param tr   The tree that is being traversed
    * @param init The states which we are checking the keys
    */
  @pure
  def traverseContainsAllKeys(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Boolean = {
    tr.traverse(true, containsAllKeysFun(init), (z, t) => z)
  }

  /** Function used the first time we visit a [[Node]] during the [[Tree]] traversal that collect all the global keys
    * of tree. Only collects the key if it did not appear before.
    *
    * @param m    The state of the traversal before reaching the node, i.e. the map with all the previous mappings.
    * @param n    The node we are currently processing
    */
  @pure
  @opaque
  def collectFun(m: Map[GlobalKey, KeyMapping], n: (NodeId, Node)): Map[GlobalKey, KeyMapping] = {
    n._2 match {
      case a: Node.Action if a.gkeyOpt.isDefined && !m.contains(a.gkeyOpt.get) =>
        m.updated(a.gkeyOpt.get, nodeActionKeyMapping(a))
      case _ => m
    }
  }

  /** Map containing all keys of the tree with the contract bound to them the first time they appeared in the
    * traversal.
    *
    * @param tr The tree that is being traversed
    */
  @pure
  def collect(tr: Tree[(NodeId, Node)]): Map[GlobalKey, KeyMapping] = {
    tr.traverse(Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z)
  }

  /** List of triples whose respective entries are:
    *  - The map of keys-contracts before the i-th step of the traversal
    *  - The pair node id - node that is handle during the i-th step
    *  - The direction i.e. if that's the first or the second time we enter the node
    *
    * @param tr The tree that is being traversed
    */
  @pure
  def collectTrace(
      tr: Tree[(NodeId, Node)]
  ): List[(Map[GlobalKey, KeyMapping], (NodeId, Node), TraversalDirection)] = {
    tr.scan(Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z)
  }

  /** Result of [[State.empty]] after having traversed the tree a first time to collect the keys.
    *
    * @param tr The tree that has been traversed
    */
  @pure
  @opaque
  def emptyState(tr: Tree[(NodeId, Node)]): State = {
    State(Set.empty[ContractId], Set.empty[ContractId], collect(tr), ActiveLedgerState.empty, Nil())
  }

}

object TransactionTreeKeys {

  import TransactionTreeKeysDef._

  /** [[appearsAtIndex]] only depends on the shape of the tree and is independent from the initial state or the
    * functions used during the travesal
    *
    * @param tr   The tree that is being traversed
    * @param init1 The initial state of the first traversal.
    * @param init2 The initial state of the second traversal.
    * @param f11
    *  The function that is executed when a node of the tree is traversed for the first time in the first traversal
    * @param f12
    *  The function that is executed when a node of the tree is traversed for the second time in the first traversal
    * @param f21
    *  The function that is executed when a node of the tree is traversed for the first time in the second traversal
    * @param f22
    *  The function that is executed when a node of the tree is traversed for the second time in the second traversal
    * @param k The key that is being queried
    * @param i The step number we are looking at
    */
  @pure
  @opaque
  def appearsAtIndexSame[Z1, Z2](
      tr: Tree[(NodeId, Node)],
      init1: Z1,
      init2: Z2,
      f11: (Z1, (NodeId, Node)) => Z1,
      f12: (Z1, (NodeId, Node)) => Z1,
      f21: (Z2, (NodeId, Node)) => Z2,
      f22: (Z2, (NodeId, Node)) => Z2,
      k: GlobalKey,
      i: BigInt,
  ): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    unfold(appearsAtIndex(tr, init1, f11, f12, k, i))
    unfold(appearsAtIndex(tr, init2, f21, f22, k, i))
    scanIndexingNode(tr, init1, init2, f11, f12, f21, f22, i)

  }.ensuring(
    appearsAtIndex(tr, init1, f11, f12, k, i) == appearsAtIndex(tr, init2, f21, f22, k, i)
  )

  /** [[doesNotAppearBefore]] only depends on the shape of the tree and is independent from the initial state or the
    * functions used during the travesal
    *
    * @param tr    The tree that is being traversed
    * @param init1 The initial state of the first traversal.
    * @param init2 The initial state of the second traversal.
    * @param f11
    *              The function that is executed when a node of the tree is traversed for the first time in the first traversal
    * @param f12
    *              The function that is executed when a node of the tree is traversed for the second time in the first traversal
    * @param f21
    *              The function that is executed when a node of the tree is traversed for the first time in the second traversal
    * @param f22
    *              The function that is executed when a node of the tree is traversed for the second time in the second traversal
    * @param k     The key that is being queried
    * @param i     The stric upper bound on the steps
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def doesNotAppearBeforeSame[Z1, Z2](
      tr: Tree[(NodeId, Node)],
      init1: Z1,
      init2: Z2,
      f11: (Z1, (NodeId, Node)) => Z1,
      f12: (Z1, (NodeId, Node)) => Z1,
      f21: (Z2, (NodeId, Node)) => Z2,
      f22: (Z2, (NodeId, Node)) => Z2,
      k: GlobalKey,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)
    unfold(doesNotAppearBefore(tr, init1, f11, f12, k, i))
    unfold(doesNotAppearBefore(tr, init2, f21, f22, k, i))

    if (i == 0) {
      Trivial()
    } else {
      appearsAtIndexSame(tr, init1, init2, f11, f12, f21, f22, k, i - 1)
      doesNotAppearBeforeSame(tr, init1, init2, f11, f12, f21, f22, k, i - 1)
    }

  }.ensuring(
    doesNotAppearBefore(tr, init1, f11, f12, k, i) == doesNotAppearBefore(tr, init2, f21, f22, k, i)
  )

  /** [[firstAppears]] only depends on the shape of the tree and is independent from the initial state or the
    * functions used during the travesal
    *
    * @param tr    The tree that is being traversed
    * @param init1 The initial state of the first traversal.
    * @param init2 The initial state of the second traversal.
    * @param f11
    *              The function that is executed when a node of the tree is traversed for the first time in the first traversal
    * @param f12
    *              The function that is executed when a node of the tree is traversed for the second time in the first traversal
    * @param f21
    *              The function that is executed when a node of the tree is traversed for the first time in the second traversal
    * @param f22
    *              The function that is executed when a node of the tree is traversed for the second time in the second traversal
    * @param k     The key that is being queried
    * @param i     The step number we are looking at
    * @see The corresponding latex document for a pen and paper proof
    */

  @pure
  @opaque
  def firstAppearsSame[Z1, Z2](
      tr: Tree[(NodeId, Node)],
      init1: Z1,
      init2: Z2,
      f11: (Z1, (NodeId, Node)) => Z1,
      f12: (Z1, (NodeId, Node)) => Z1,
      f21: (Z2, (NodeId, Node)) => Z2,
      f22: (Z2, (NodeId, Node)) => Z2,
      k: GlobalKey,
      i: BigInt,
  ): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    unfold(firstAppears(tr, init1, f11, f12, k, i))
    unfold(firstAppears(tr, init2, f21, f22, k, i))

    appearsAtIndexSame(tr, init1, init2, f11, f12, f21, f22, k, i)
    doesNotAppearBeforeSame(tr, init1, init2, f11, f12, f21, f22, k, i)

  }.ensuring(
    firstAppears(tr, init1, f11, f12, k, i) == firstAppears(tr, init2, f21, f22, k, i)
  )

  /** If a key k does not appear in a tree traversal before step j then it does not appear either for any 0 ≤ i ≤ j.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param f1   The function that is executed when a node of the tree is traversed for the first time
    * @param f2   The function that is executed when a node of the tree is traversed for the second time
    */
  @pure
  @opaque
  def doesNotAppearBeforeDiffIndex[Z](
      tr: Tree[(NodeId, Node)],
      init: Z,
      f1: (Z, (NodeId, Node)) => Z,
      f2: (Z, (NodeId, Node)) => Z,
      k: GlobalKey,
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(j - i)
    require(0 <= i)
    require(i <= j)
    require(j < 2 * tr.size)
    require(doesNotAppearBefore(tr, init, f1, f2, k, j))

    unfold(doesNotAppearBefore(tr, init, f1, f2, k, j))
    if (j == i) {
      Trivial()
    } else {
      doesNotAppearBeforeDiffIndex(tr, init, f1, f2, k, i, j - 1)
    }
  }.ensuring(doesNotAppearBefore(tr, init, f1, f2, k, i))

  /** If a key k does not appear in a tree traversal before the i1-th step but appears before the i2-th step, then
    * there exists a j such that:
    *  - i1 ≤ j < i2
    *  - k first appears at step j
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param f1   The function that is executed when a node of the tree is traversed for the first time
    * @param f2   The function that is executed when a node of the tree is traversed for the second time
    *
    * @return j the step during which the key appears for the first time
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def findFirstAppears[Z](
      tr: Tree[(NodeId, Node)],
      init: Z,
      f1: (Z, (NodeId, Node)) => Z,
      f2: (Z, (NodeId, Node)) => Z,
      k: GlobalKey,
      i1: BigInt,
      i2: BigInt,
  ): BigInt = {
    decreases(i2 - i1)
    require(0 <= i1)
    require(i1 < i2)
    require(i2 < 2 * tr.size)
    require(doesNotAppearBefore(tr, init, f1, f2, k, i1))
    require(!doesNotAppearBefore(tr, init, f1, f2, k, i2))

    unfold(doesNotAppearBefore(tr, init, f1, f2, k, i1 + 1))

    if (appearsAtIndex(tr, init, f1, f2, k, i1)) {
      unfold(firstAppears(tr, init, f1, f2, k, i1))
      i1
    } else {
      findFirstAppears(tr, init, f1, f2, k, i1 + 1, i2)
    }

  }.ensuring(j =>
    i1 <= j && j < i2 &&
      firstAppears(tr, init, f1, f2, k, j)
  )

  /** If a state contains all the keys of a tree up to a given point, then it contains all the keys of the tree
    * up to any previous point.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param i The point such that init contains all the keys before it.
    * @param j A point in the traversal before i
    */
  @pure
  @opaque
  def containsAllKeysBeforeImplies(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(i - j)
    require(0 <= j)
    require(j <= i)
    require(i < 2 * tr.size)
    require(containsAllKeysBefore(tr, init, i))

    if (j == i) {
      Trivial()
    } else {
      scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), i)
      unfold(
        containsAllKeysFun(init)(
          containsAllKeysBefore(tr, init, i - 1),
          tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i - 1)._2,
        )
      )
      containsAllKeysBeforeImplies(tr, init, i - 1, j)
    }

  }.ensuring(containsAllKeysBefore(tr, init, j))

  /** If a state contains all the keys of a tree, then it contains all the keys of the tree up to any previous point.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param i    A point in the traversal.
    */
  @pure
  @opaque
  def containsAllKeysImplies(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(containsAllKeys(tr, init))
    require(0 <= i)
    require(i < 2 * tr.size)

    scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), 0)
    unfold(
      containsAllKeysFun(init)(
        containsAllKeysBefore(tr, init, 2 * tr.size - 1),
        tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(2 * tr.size - 1)._2,
      )
    )
    containsAllKeysBeforeImplies(tr, init, 2 * tr.size - 1, i)

  }.ensuring(containsAllKeysBefore(tr, init, i))

  /** If a state contains all the keys of a tree and the same state is the inital state of a transaction traversal, then
    * any at any step of this traversal the intermediate state contains the node that is visited during the step.
    *
    * @param tr   The transaction that is being processed
    * @param init The initial state of the traversal which contains all the keys of the tree.
    * @param i    A step in the traversal
    */
  @pure
  @opaque
  def containsAllKeysImpliesDown(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(containsAllKeys(tr, init))
    require(0 <= i)
    require(i < 2 * tr.size)

    scanIndexingNode(
      tr,
      init,
      true,
      traverseInFun,
      traverseOutFun,
      containsAllKeysFun(init),
      containsAllKeysFun(init),
      i,
    )
    unfold(
      containsAllKeysFun(init)(containsAllKeysBefore(tr, init, i), scanTransaction(tr, init)(i)._2)
    )
    scanTransactionProp(tr, init, i)
    containsKeySameGlobalKeys(
      init,
      scanTransaction(tr, init)(i)._1,
      scanTransaction(tr, init)(i)._2._2,
    )

    if (i == 2 * tr.size - 1) {
      scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), 0)
    } else {
      scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), i + 1)
      containsAllKeysImplies(tr, init, i + 1)
    }

  }.ensuring(
    containsKey(scanTransaction(tr, init)(i)._1)(scanTransaction(tr, init)(i)._2._2)
  )

  /** If a state contains all the keys of a tree up to a given point, then it contains all the keys of the tree
    * up to any previous point.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param i    The point such that init contains all the keys before it.
    * @param j    A point in the traversal before i
    */
  @pure
  @opaque
  def containsAllKeysBeforeAltImplies(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(i - j)
    require(0 <= j)
    require(j <= i)
    require(i < 2 * tr.size)
    require(scanContainsAllKeys(tr, init)(i)._1)

    if (j == i) {
      Trivial()
    } else {
      scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, i)
      unfold(
        containsAllKeysFun(init)(
          tr.scan(true, containsAllKeysFun(init), (z, t) => z)(i - 1)._1,
          tr.scan(true, containsAllKeysFun(init), (z, t) => z)(i - 1)._2,
        )
      )
      containsAllKeysBeforeAltImplies(tr, init, i - 1, j)
    }

  }.ensuring(scanContainsAllKeys(tr, init)(j)._1)

  /** If a state contains all the keys of a tree, then it contains all the keys of the tree up to any previous point.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    * @param i    A point in the traversal.
    */
  @pure
  @opaque
  def containsAllKeysAltImplies(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(traverseContainsAllKeys(tr, init))
    require(0 <= i)
    require(i < 2 * tr.size)

    scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, 0)
    containsAllKeysBeforeAltImplies(tr, init, 2 * tr.size - 1, i)

  }.ensuring(scanContainsAllKeys(tr, init)(i)._1)

  /** States the equivalence between [[TransactionTreeKeysDef.containsAllKeysBefore]] and
    * [[TransactionTreeKeysDef.scanContainsAllKeys]]. Both functions behave in a similar manner when visiting each node for
    * the first time. However, the former also checks that the state given in argument contains the node key when visiting
    * it for the second time whereas the latter does not do anything. Some claims are easier to prove using one version
    * or the other.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    */
  @pure @opaque
  def containsAllKeysBeforeAlt(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)
    scanIndexingNode(
      tr,
      true,
      true,
      containsAllKeysFun(init),
      containsAllKeysFun(init),
      containsAllKeysFun(init),
      (z, t) => z,
      i,
    )
    if (i == 0) {
      scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, 0)
      scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), 0)
    } else {
      scanIndexingNode(
        tr,
        true,
        true,
        containsAllKeysFun(init),
        containsAllKeysFun(init),
        containsAllKeysFun(init),
        (z, t) => z,
        i - 1,
      )
      containsAllKeysBeforeAlt(tr, init, i - 1)
      scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, i)
      scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), i)
      unfold(
        containsAllKeysFun(init)(
          tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i - 1)._1,
          tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i - 1)._2,
        )
      )
      if (
        tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i - 1)
          ._3 == TraversalDirection.Up
      ) {

        val j = findDown(tr, true, containsAllKeysFun(init), (z, t) => z, i - 1)
        scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, j + 1)
        unfold(
          containsAllKeysFun(init)(
            tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(j)._1,
            tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(j)._2,
          )
        )
        if (tr.scan(true, containsAllKeysFun(init), (z, t) => z)(i)._1) {
          containsAllKeysBeforeAltImplies(tr, init, i, j + 1)
        }
      } else {
        Trivial()
      }
    }
  }.ensuring(
    tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(i) == scanContainsAllKeys(
      tr,
      init,
    )(i)
  )

  /** States the equivalence between [[TransactionTreeKeysDef.containsAllKeys]] and
    * [[TransactionTreeKeysDef.traverseContainsAllKeys]]. Both functions behave in a similar manner when visiting each node for
    * the first time. However, the former also checks that the state given in argument contains the node key when visiting
    * it for the second time whereas the latter does not do anything. Some claims are easier to prove using one version
    * or the other.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal.
    */
  @pure
  @opaque
  def containsAllKeysAlt(tr: Tree[(NodeId, Node)], init: Either[KeyInputError, State]): Unit = {
    if (tr.size > 0) {
      scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, 0)
      scanIndexingState(tr, true, containsAllKeysFun(init), containsAllKeysFun(init), 0)

      scanIndexingNode(
        tr,
        true,
        true,
        containsAllKeysFun(init),
        containsAllKeysFun(init),
        containsAllKeysFun(init),
        (z, t) => z,
        2 * tr.size - 1,
      )
      containsAllKeysBeforeAlt(tr, init, 2 * tr.size - 1)
      unfold(
        containsAllKeysFun(init)(
          tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(2 * tr.size - 1)._1,
          tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(2 * tr.size - 1)._2,
        )
      )
      if (
        tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(2 * tr.size - 1)
          ._3 == TraversalDirection.Up
      ) {

        val j = findDown(tr, true, containsAllKeysFun(init), (z, t) => z, 2 * tr.size - 1)
        scanIndexingState(tr, true, containsAllKeysFun(init), (z, t) => z, j + 1)
        unfold(
          containsAllKeysFun(init)(
            tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(j)._1,
            tr.scan(true, containsAllKeysFun(init), containsAllKeysFun(init))(j)._2,
          )
        )
        if (tr.traverse(true, containsAllKeysFun(init), (z, t) => z)) {
          containsAllKeysAltImplies(tr, init, j + 1)
        }
      } else {
        Trivial()
      }
    } else {
      Trivial()
    }
  }.ensuring(containsAllKeys(tr, init) == traverseContainsAllKeys(tr, init))

  /** Expresses [[TransactionTreeKeysDef.collectFun]] as the concatenation of the map given as argument and a map
    * representing the node contribution.
    *
    * @param m The map of the previous state
    * @param n The node that is being processed
    */
  @pure
  @opaque
  def collectFunConcat(m: Map[GlobalKey, KeyMapping], n: (NodeId, Node)): Unit = {
    unfold(collectFun(m, n))
    unfold(nodeKeyMap(n._2))
    n._2 match {
      case a: Node.Action =>
        unfold(actionKeyMap(a))
        unfold(optionKeyMap(a.gkeyOpt, nodeActionKeyMapping(a)))
        if (a.gkeyOpt.isDefined) {
          val k = a.gkeyOpt.get
          if (!m.contains(a.gkeyOpt.get)) {
            MapProperties.updatedCommutativity(m, k, nodeActionKeyMapping(a))
            MapAxioms.extensionality(
              Map[GlobalKey, KeyMapping](k -> nodeActionKeyMapping(a)) ++ m,
              m.updated(k, nodeActionKeyMapping(a)),
            )
          } else {
            MapProperties.keySetContains(m, k)
            MapProperties.singletonKeySet(k, nodeActionKeyMapping(a))
            SetProperties.singletonSubsetOf(m.keySet, k)
            SetProperties.equalsSubsetOfTransitivity(
              Set(k),
              Map[GlobalKey, KeyMapping](k -> nodeActionKeyMapping(a)).keySet,
              m.keySet,
            )
            MapProperties.concatSubsetOfEquals(
              Map[GlobalKey, KeyMapping](k -> nodeActionKeyMapping(a)),
              m,
            )
            MapAxioms.extensionality(
              Map[GlobalKey, KeyMapping](k -> nodeActionKeyMapping(a)) ++ m,
              m,
            )
          }
        } else {
          MapProperties.concatEmpty(m)
          MapAxioms.extensionality(Map.empty[GlobalKey, KeyMapping] ++ m, m)
        }
      case _ =>
        MapProperties.concatEmpty(m)
        MapAxioms.extensionality(Map.empty[GlobalKey, KeyMapping] ++ m, m)
    }
  }.ensuring(collectFun(m, n) == nodeKeyMap(n._2) ++ m)

  /** Any intermediate state of the map that collects the keys of the tree, is a submap of the map obtained at the end
    * of the traversal
    *
    * @param tr The tree that is being traversed
    * @param i The step number of the intermediate map
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def collectTraceProp(tr: Tree[(NodeId, Node)], i: BigInt): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    MapProperties.submapOfReflexivity(collect(tr))
    if (!collectTrace(tr)(i)._1.submapOf(collect(tr))) {
      val j = scanNotPropRev(
        tr,
        Map.empty[GlobalKey, KeyMapping],
        collectFun,
        (z, t) => z,
        x => x.submapOf(collect(tr)),
        i,
      )
      val sj = collectTrace(tr)(j)._1
      val n = collectTrace(tr)(j)._2
      MapProperties.submapOfReflexivity(sj)
      if (j == 2 * tr.size - 1) {
        scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, 0)
      } else {
        scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, j + 1)
        unfold(collectFun(sj, n))
        MapProperties.submapOfTransitivity(sj, collectFun(sj, n), collect(tr))
      }
    }
  }.ensuring(collectTrace(tr)(i)._1.submapOf(collect(tr)))

  /** A key does not appear in the tree before a given step if and only if the map obtained by collecting all the key
    * up to that step does not contain it.
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def collectTraceDoesNotAppear(tr: Tree[(NodeId, Node)], k: GlobalKey, i: BigInt): Unit = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)

    unfold(doesNotAppearBefore(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, k, i))
    if (i == 0) {
      scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, 0)
      MapProperties.emptyContains[GlobalKey, KeyMapping](k)

    } else {
      val si = collectTrace(tr)(i - 1)._1
      val n = collectTrace(tr)(i - 1)._2

      collectTraceDoesNotAppear(tr, k, i - 1)
      scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, i)
      unfold(
        appearsAtIndex(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, k, i - 1)
      )
      unfold(collectFun(si, n))
      n._2 match {
        case a: Node.Action if a.gkeyOpt.isDefined && !si.contains(a.gkeyOpt.get) =>
          MapProperties.updatedContains(si, a.gkeyOpt.get, nodeActionKeyMapping(a), k)
        case a: Node.Action if a.gkeyOpt.isDefined =>
          MapProperties.updatedContains(si, a.gkeyOpt.get, nodeActionKeyMapping(a), k)
        case _ => Trivial()
      }
    }
  }.ensuring(
    doesNotAppearBefore(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, k, i) ==
      !collectTrace(tr)(i)._1.contains(k)
  )

  /** A key does not appear in the tree if and only if the map obtained by collecting all the key does not contain it.
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def collectDoesNotAppear(tr: Tree[(NodeId, Node)], k: GlobalKey): Unit = {
    require(tr.size > 0)
    collectTraceDoesNotAppear(tr, k, 2 * tr.size - 1)
    scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, 0)
    unfold(collectFun(collectTrace(tr)(2 * tr.size - 1)._1, collectTrace(tr)(2 * tr.size - 1)._2))
  }.ensuring(
    doesNotAppearBefore(
      tr,
      Map.empty[GlobalKey, KeyMapping],
      collectFun,
      (z, t) => z,
      k,
      2 * tr.size - 1,
    ) ==
      !collect(tr).contains(k)
  )

  /** If the key of a node appears for the first time at a given step of the traversal, then the map containing all the
    * keys of the tree, will have an entry with the key and the contract of the node.
    *
    * @param tr The tree that is being traversed
    * @param i  The step number during which the node is processed
    * @param n The node whose key appeared for the first time at step i
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def collectGet(tr: Tree[(NodeId, Node)], i: BigInt, n: Node.Action): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    require(n.gkeyOpt.isDefined)
    require(collectTrace(tr)(i)._2._2 == n)
    require(
      firstAppears(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, n.gkeyOpt.get, i)
    )

    unfold(
      firstAppears(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, n.gkeyOpt.get, i)
    )
    collectTraceDoesNotAppear(tr, n.gkeyOpt.get, i)

    unfold(
      appearsAtIndex(
        tr,
        Map.empty[GlobalKey, KeyMapping],
        collectFun,
        (z, t) => z,
        n.gkeyOpt.get,
        i,
      )
    )
    if (i == 2 * tr.size - 1) {
      Unreachable()
    } else {
      scanIndexingState(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, i + 1)
      unfold(collectFun(collectTrace(tr)(i)._1, collectTrace(tr)(i)._2))
      collectTraceProp(tr, i + 1)
      MapAxioms.submapOfGet(collectTrace(tr)(i + 1)._1, collect(tr), n.gkeyOpt.get)
    }

  }.ensuring(
    collect(tr).get(n.gkeyOpt.get) == Some(nodeActionKeyMapping(n))
  )

  /** If [[TransactionTreeKeysDef.collect]] contains a key after having traversed a transaction, then there exists a
    * step during which the key appeared for the first time.
    * @param tr The tree that is being traversed
    * @param k The key contained in traversal result
    * @return The step number where the key appeared for the first time
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def collectContains(tr: Tree[(NodeId, Node)], k: GlobalKey): BigInt = {
    require(collect(tr).contains(k))

    if (tr.size == 0) {
      MapProperties.emptyContains[GlobalKey, KeyMapping](k)
      Unreachable()
    } else {
      collectDoesNotAppear(tr, k)
      unfold(
        doesNotAppearBefore(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, k, 0)
      )
      findFirstAppears(
        tr,
        Map.empty[GlobalKey, KeyMapping],
        collectFun,
        (z, t) => z,
        k,
        0,
        2 * tr.size - 1,
      )
    }
  }.ensuring(i =>
    (tr.size > BigInt(0))
      i >= BigInt (0) && i < 2 * tr.size - 1 &&
      firstAppears(tr, Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z, k, i)
  )

  /** The empty state whose global keys are replaced with the collection of all the keys, contains all the keys of the
    * tree.
    *
    * @param tr The tree whose keys have been collected.
    */
  @pure @opaque
  def emptyContainsAllKeys(tr: Tree[(NodeId, Node)]): Unit = {

    val rempty: Either[KeyInputError, State] = Right[KeyInputError, State](emptyState(tr))

    containsAllKeysAlt(tr, rempty)
    if (!containsAllKeys(tr, rempty)) {
      val j = traverseNotProp(tr, true, containsAllKeysFun(rempty), (z, t) => z, z => z)

      if (j == 2 * tr.size - 1) {
        scanIndexingState(tr, true, containsAllKeysFun(rempty), (z, t) => z, 0)
      } else {
        scanIndexingState(tr, true, containsAllKeysFun(rempty), (z, t) => z, j + 1)
        scanIndexingNode(
          tr,
          true,
          Map.empty[GlobalKey, KeyMapping],
          containsAllKeysFun(rempty),
          (z, t) => z,
          collectFun,
          (z, t) => z,
          j,
        )
        unfold(
          containsAllKeysFun(rempty)(
            tr.scan(true, containsAllKeysFun(rempty), (z, t) => z)(j)._1,
            tr.scan(true, containsAllKeysFun(rempty), (z, t) => z)(j)._2,
          )
        )
        unfold(containsKey(rempty)(tr.scan(true, containsAllKeysFun(rempty), (z, t) => z)(j)._2._2))
        unfold(
          containsNodeKey(emptyState(tr))(
            tr.scan(true, containsAllKeysFun(rempty), (z, t) => z)(j)._2._2
          )
        )
        tr.scan(true, containsAllKeysFun(rempty), (z, t) => z)(j)._2._2 match {
          case a: Node.Action =>
            unfold(containsActionKey(emptyState(tr))(a))
            unfold(containsOptionKey(emptyState(tr))(a.gkeyOpt))
            a.gkeyOpt match {
              case None() => Trivial()
              case Some(k) =>
                unfold(containsKey(emptyState(tr))(k))
                unfold(emptyState(tr))

                scanIndexingState(
                  tr,
                  Map.empty[GlobalKey, KeyMapping],
                  collectFun,
                  (z, t) => z,
                  j + 1,
                )
                unfold(
                  collectFun(
                    tr.scan(Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z)(j)._1,
                    tr.scan(Map.empty[GlobalKey, KeyMapping], collectFun, (z, t) => z)(j)._2,
                  )
                )
                collectTraceProp(tr, j + 1)
                MapProperties.submapOfContains(collectTrace(tr)(j + 1)._1, collect(tr), k)
            }
          case r: Node.Rollback => Trivial()
        }
      }
    }
  }.ensuring(containsAllKeys(tr, Right[KeyInputError, State](emptyState(tr))))

}
