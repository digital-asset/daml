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
import transaction.{State}

/** Definitions and basic properties of simplified transaction traversal.
  *
  * In the contract state maching, handling a node comes in two major step:
  * - Adding the node's key to the global keys with its corresponding mapping
  * - Processing the node
  * In the simplified version of the contract state machine, this behavior is respectively split in two different
  * functions [[transaction.CSMKeysPropertiesDef.addKeyBeforeNode]] and [[transaction.State.handleNode]]
  *
  * A key property of transaction traversal is that one can first add the key-mapping pairs of every node in the
  * globalKeys and then process the transaction. The proof of this claims lies in [[TransactionTreeFull]].
  * We therefore define the processing part of a transaction assuming that the initial state already contains
  * all the key mappings it needs.
  */
object TransactionTreeDef {

  /** Function called when a node is entered for the first time ([[utils.TraversalDirection.Down]]).
    *  - If the node is an instance of [[transaction.Node.Action]] we handle it.
    *  - If it is a [[transaction.Node.Rollback]] node we call [[transaction.State.beginRollback]].
    *
    * Among the direct properties one can deduce we have that
    *  - the [[transaction.State.globalKeys]] are not modified (since we did before processing the transaction)
    *  - if the inital state is an error then the result is also an error
    *
    * @param s State before entering the node for the first time
    * @param p Node and its id
    * @see f,,down,, in the associated latex document
    */
  @pure @opaque
  def traverseInFun(
      s: Either[KeyInputError, State],
      p: (NodeId, Node),
  ): Either[KeyInputError, State] = {
    p._2 match {
      case r: Node.Rollback => beginRollback(s)
      case a: Node.Action => handleNode(s, p._1, a)
    }
  }.ensuring(res =>
    sameGlobalKeys(s, res) &&
      propagatesError(s, res)
  )

  /** Function called when a node is entered for the second time ([[utils.TraversalDirection.Up]]).
    * - If the node is an instance of [[transaction.Node.Action]] nothing happens
    * - If it is a [[transaction.Node.Rollback]] node we call [[transaction.State.endRollback]]
    *
    * Among the direct properties one can deduce we have that
    *  - the [[transaction.State.globalKeys]] are not modified (since we did before processing the transaction)
    *  - the [[transaction.State.locallyCreated]] and [[transaction.State.consumed]] sets
    *    are not modified as well.
    *  - if the inital state is an error then the result is also an error
    *
    * @param s State before entering the node for the second time
    * @param p Node and its id
    * @see f,,up,, in the associated latex document
    */
  @pure @opaque
  def traverseOutFun(
      s: Either[KeyInputError, State],
      p: (NodeId, Node),
  ): Either[KeyInputError, State] = {

    val res = p._2 match {
      case r: Node.Rollback => endRollback(s)
      case a: Node.Action => s
    }

    @pure @opaque
    def traverseOutFunProperties: Unit = {

      // case Rollback
      endRollbackProp(s)

      // case Action
      sameGlobalKeysReflexivity(s)
      sameLocallyCreatedReflexivity(s)
      sameConsumedReflexivity(s)
      propagatesErrorReflexivity(s)

    }.ensuring(
      sameGlobalKeys(s, res) &&
        sameLocallyCreated(s, res) &&
        sameConsumed(s, res) &&
        propagatesError(s, res)
    )

    traverseOutFunProperties
    res

  }.ensuring(res =>
    sameGlobalKeys(s, res) &&
      sameLocallyCreated(s, res) &&
      sameConsumed(s, res) &&
      propagatesError(s, res)
  )

  /** List of triples whose respective entries are:
    *  - The state before the i-th step of the traversal
    *  - The pair node id - node that is handle during the i-th step
    *  - The direction i.e. if that's the first or the second time we enter the node
    *
    *  @param tr The transaction that is being processed
    *  @param init The initial state (containing already all the necessary key-mapping pairs in the global keys)
    */
  @pure
  def scanTransaction(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): List[(Either[KeyInputError, State], (NodeId, Node), TraversalDirection)] = {
    tr.scan(init, traverseInFun, traverseOutFun)
  }

  /** Resulting state after a transaction traversal.
    *
    * @param tr   The transaction that is being processed
    * @param init The initial state (containing already all the necessary key-mapping pairs in the global keys)
    */
  @pure
  def traverseTransaction(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Either[KeyInputError, State] = {
    tr.traverse(init, traverseInFun, traverseOutFun)
  }

}

object TransactionTree {

  import TransactionTreeDef._

  /** Let i less or equal than j two integers.
    * The states before the i-th and j-th step of the transaction traversal:
    *  - have the same [[transaction.State.globalKeys]]
    *  - are both erroneous if the state before the i-th step is erroneous
    *
    * @param tr   The transaction that is being processed.
    * @param init The initial state (containing already all the necessary key-mapping pairs in the global keys).
    * @see The corresponding latex document for a pen and paper proof.
    */
  @pure
  @opaque
  def scanTransactionProp(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(j)
    require(0 <= i)
    require(i <= j)
    require(j < 2 * tr.size)

    val si = scanTransaction(tr, init)(i)._1

    if (j == i) {
      propagatesErrorReflexivity(si)
      sameGlobalKeysReflexivity(si)
    } else {
      val sjm1 = scanTransaction(tr, init)(j - 1)._1
      val sj = scanTransaction(tr, init)(j)._1

      scanTransactionProp(tr, init, i, j - 1)
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, j)
      propagatesErrorTransitivity(si, sjm1, sj)
      sameGlobalKeysTransitivity(si, sjm1, sj)
    }
  }.ensuring(
    propagatesError(scanTransaction(tr, init)(i)._1, scanTransaction(tr, init)(j)._1) &&
      sameGlobalKeys(scanTransaction(tr, init)(i)._1, scanTransaction(tr, init)(j)._1)
  )

  /** Let i an integer. The inital state and the state before the i-th step of the transaction traversal:
    *  - have the same [[transaction.State.globalKeys]]
    *  - are both erroneous if the initial state is erroneous
    *
    * @param tr   The transaction that is being processed.
    * @param init The initial state (containing already all the necessary key-mapping pairs in the global keys).
    * @see The corresponding latex document for a pen and paper proof.
    */
  @pure
  @opaque
  def scanTransactionProp(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(0 <= i)
    require(i < 2 * tr.size)
    scanTransactionProp(tr, init, 0, i)
    scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
  }.ensuring(
    propagatesError(init, scanTransaction(tr, init)(i)._1) &&
      sameGlobalKeys(init, scanTransaction(tr, init)(i)._1)
  )

  /** Basic properties of transaction processing:
    *  - The [[transaction.State.globalKeys]] are not modified throughout the traversal
    *  - The [[transaction.State.rollbackStack]] is the same before and after the transaction
    *  - If the initial state is erroneous, so is the result of the traversal
    *
    * @param tr   The transaction that is being processed.
    * @param init The initial state (containing already all the necessary key-mapping pairs in the global keys).
    *
    * @note This is one of the only structural induction proof in the codebase. Whereas structural induction is
    *       not needed for the first and third property, it is for the second one. In fact, the property is not
    *       a local claim that is preserved every step. It is due to the symmetrical nature of the traversal (for
    *       rollbacks) and it would therefore not make sense finding a i such that the property is true at the
    *       i-th step but not the i+1-th one.
    *       Please also note that since we are working with opaqueness (almost) everywhere we will need to unfold
    *       many definition when proving such global properties.
    *
    * @see The corresponding latex document for a pen and paper proof.
    * @see [[findBeginRollback]] and [[TransactionTreeChecks.traverseTransactionLC]] for other examples of
    *      structural induction proofs.
    */
  @pure
  @opaque
  def traverseTransactionProp(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {
    decreases(tr)
    unfold(tr.traverse(init, traverseInFun, traverseOutFun))
    tr match {
      case Endpoint() =>
        sameGlobalKeysReflexivity(init)
        sameStackReflexivity(init)
        propagatesErrorReflexivity(init)
      case ContentNode(n, sub) =>
        val e1 = traverseInFun(init, n)
        val e2 = traverseTransaction(sub, e1)
        val e3 = traverseTransaction(tr, init)

        traverseTransactionProp(sub, e1)

        // As the key property is a local one applying transitivity is sufficient
        // we could have done the same with propagatesError but we need to unfold
        // the latter to prove the stack equality property (which is global)
        sameGlobalKeysTransitivity(init, e1, e2)
        sameGlobalKeysTransitivity(init, e2, e3)

        unfold(traverseInFun(init, n))
        unfold(traverseOutFun(e2, n))
        unfold(beginRollback(init))
        unfold(endRollback(e2))

        unfold(propagatesError(init, e1))
        unfold(propagatesError(e1, e2))
        unfold(propagatesError(e2, e3))
        unfold(propagatesError(init, e3))

        unfold(sameStack(init, e1))
        unfold(sameStack(e1, e2))
        unfold(sameStack(e2, e3))
        unfold(sameStack(init, e3))

        // If any of the intermediate states is not defined the result is immediate
        // because of error propagation. Otherwise, we need to unfold the definition
        // of beginRollback and endRollback as the result depends on the content of
        // their body
        (init, e1, e2) match {
          case (Right(s0), Right(s1), Right(s2)) =>
            unfold(s0.beginRollback())
            unfold(s2.endRollback())

            unfold(sameStack(s0, e1))
            unfold(sameStack(s1, e2))
            unfold(sameStack(s2, e3))
            unfold(sameStack(s0, e3))
          case (Right(s0), _, _) => unfold(sameStack(s0, e3))
          case _ => Trivial()
        }
      case ArticulationNode(l, r) =>
        val el = traverseTransaction(l, init)
        val er = traverseTransaction(tr, init)

        traverseTransactionProp(l, init)
        traverseTransactionProp(r, el)

        propagatesErrorTransitivity(init, el, er)
        sameStackTransitivity(init, el, er)
        sameGlobalKeysTransitivity(init, el, er)
    }
  }.ensuring(
    sameGlobalKeys(init, traverseTransaction(tr, init)) &&
      sameStack(init, traverseTransaction(tr, init)) &&
      propagatesError(init, traverseTransaction(tr, init))
  )

  /** If any state of the transaction traversal is erroneous, then the result of the traversal is erroneous as well
    *
    * @param tr   The transaction that is being processed.
    * @param init The initial state (containing already all the necessary key-mapping pairs in the global keys).
    * @param i The number of the step during the traversal
    * @see The corresponding latex document for a pen and paper proof.
    */
  @pure
  @opaque
  def traverseTransactionDefined(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): Unit = {
    require(0 <= i)
    require(i < 2 * tr.size)

    if (tr.size > 0) {
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
    }
    scanTransactionProp(tr, init, i, 2 * tr.size - 1)
    propagatesErrorTransitivity(
      scanTransaction(tr, init)(i)._1,
      scanTransaction(tr, init)(2 * tr.size - 1)._1,
      traverseTransaction(tr, init),
    )
  }.ensuring(
    propagatesError(scanTransaction(tr, init)(i)._1, traverseTransaction(tr, init))
  )

  /** Given a step number in which [[transaction.Node.Rollback]] is entered for the second time, returns the step
    * number in which it is has been entered for the first time. Also returns a subtree with the following convenient
    * properties.
    *
    * If the result of the function is (j, sub) then:
    *  - The size of sub is strictly smaller than the size of the transaction tree.
    *  - Due to causality, j is strictly smaller than the step number given in argument.
    *  - The state before entering the node for the second time is the result of traversing sub. The
    *    inital state of the traversal is the state after step j.
    *
    * This version of findBeginRollback deals with two traversal simultaneously. This is due to the fact that the result
    * only depends on the shape of the transaction and is independent of the initial state.
    * For a simplified version cf. below.
    *
    * Please note that the claim is valid only if the tree is unique.
    *
    * @param tr   The transaction that is being processed.
    * @param init1 The initial state of the first traversal.
    * @param init2 The initial state of the second traversal.
    * @param i The step number during which the node is entered for the second time.
    *
    * @note This is one of the only structural induction proof in the codebase. Because the global nature of the property,
    *       it is not possible to prove a local claim that is preserved during every step. This is due to the symmetry
    *       of the traversal (for rollbacks) and it would therefore not make sense finding a i such that the
    *       property is true at the i-th step but not the i+1-th one.
    *
    * @see The corresponding latex document for a pen and paper proof.
    * @see [[traverseTransactionProp]] and [[TransactionTreeChecks.traverseTransactionLC]] for other examples of
    *      structural induction proofs.
    */
  @pure
  @opaque
  def findBeginRollback(
      tr: Tree[(NodeId, Node)],
      init1: Either[KeyInputError, State],
      init2: Either[KeyInputError, State],
      i: BigInt,
  ): (BigInt, Tree[(NodeId, Node)]) = {
    decreases(tr)
    require(tr.isUnique)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(scanTransaction(tr, init1)(i)._2._2.isInstanceOf[Node.Rollback])
    require(scanTransaction(tr, init1)(i)._3 == TraversalDirection.Up)

    unfold(tr.size)
    unfold(tr.isUnique)

    scanIndexingNode(
      tr,
      init1,
      init2,
      traverseInFun,
      traverseOutFun,
      traverseInFun,
      traverseOutFun,
      i,
    )

    tr match {
      case Endpoint() => Unreachable()
      case ContentNode(c, str) =>
        scanIndexing(c, str, init1, traverseInFun, traverseOutFun, i)
        scanIndexing(c, str, init2, traverseInFun, traverseOutFun, i)

        if (c == scanTransaction(tr, init1)(i)._2) {
          scanIndexing(c, str, init1, traverseInFun, traverseOutFun, 0)
          scanIndexing(c, str, init2, traverseInFun, traverseOutFun, 0)
          scanIndexing(c, str, init1, traverseInFun, traverseOutFun, 2 * tr.size - 1)
          scanIndexing(c, str, init2, traverseInFun, traverseOutFun, 2 * tr.size - 1)
          isUniqueIndexing(tr, init1, traverseInFun, traverseOutFun, i, 2 * tr.size - 1)
          isUniqueIndexing(tr, init2, traverseInFun, traverseOutFun, i, 2 * tr.size - 1)

          unfold(traverseInFun(init1, c))
          unfold(traverseInFun(init2, c))
          unfold(traverseOutFun(init1, c))
          unfold(traverseOutFun(init2, c))

          (BigInt(0), str)
        } else {
          val (j, sub) =
            findBeginRollback(str, traverseInFun(init1, c), traverseInFun(init2, c), i - 1)
          scanIndexing(c, str, init1, traverseInFun, traverseOutFun, j + 1)
          scanIndexing(c, str, init2, traverseInFun, traverseOutFun, j + 1)

          (j + 1, sub)
        }
      case ArticulationNode(l, r) =>
        scanIndexing(l, r, init1, traverseInFun, traverseOutFun, i)
        scanIndexing(l, r, init2, traverseInFun, traverseOutFun, i)

        if (i < 2 * l.size) {
          val (j, sub) = findBeginRollback(l, init1, init2, i)
          scanIndexing(l, r, init1, traverseInFun, traverseOutFun, j)
          scanIndexing(l, r, init2, traverseInFun, traverseOutFun, j)

          // Required for performance (timeout: 2)
          assert(
            scanTransaction(tr, init1)(i)._1 == traverseTransaction(
              sub,
              beginRollback(scanTransaction(tr, init1)(j)._1),
            )
          )
          assert(
            scanTransaction(tr, init2)(i)._1 == traverseTransaction(
              sub,
              beginRollback(scanTransaction(tr, init2)(j)._1),
            )
          )

          (j, sub)
        } else {
          val (j, sub) = findBeginRollback(
            r,
            traverseTransaction(l, init1),
            traverseTransaction(l, init2),
            i - 2 * l.size,
          )
          scanIndexing(l, r, init1, traverseInFun, traverseOutFun, j + 2 * l.size)
          scanIndexing(l, r, init2, traverseInFun, traverseOutFun, j + 2 * l.size)

          (j + 2 * l.size, sub)
        }
    }
  }.ensuring((j, sub) =>
    0 <= j && j < i && sub.size < tr.size &&
      (scanTransaction(tr, init1)(j)._2 == scanTransaction(tr, init1)(i)._2) &&
      (scanTransaction(tr, init2)(j)._2 == scanTransaction(tr, init2)(i)._2) &&
      (scanTransaction(tr, init1)(j)._3 == TraversalDirection.Down) &&
      (scanTransaction(tr, init2)(j)._3 == TraversalDirection.Down) &&
      (scanTransaction(tr, init1)(i)._1 == traverseTransaction(
        sub,
        beginRollback(scanTransaction(tr, init1)(j)._1),
      )) &&
      (scanTransaction(tr, init2)(i)._1 == traverseTransaction(
        sub,
        beginRollback(scanTransaction(tr, init2)(j)._1),
      ))
  )

  /** Given a step number in which [[transaction.Node.Rollback]] is entered for the second time, returns the step
    * number in which it is has been entered for the first time. Also returns a subtree with the following convenient
    * properties.
    *
    * If the result of the function is (j, sub) then:
    *  - The size of sub is strictly smaller than the size of the transaction tree.
    *  - Due to causality, j is strictly smaller than the step number given in argument.
    *  - The state before entering the node for the second time is the result of traversing sub. The
    *    inital state of the traversal is the state after step j.
    *
    * This results only depends on the shape of the transaction and is independent of the initial state.
    * For a version dealing with multiple transaction at the same time cf. the more complex version above.
    *
    * Please note that the claim is valid only if the tree is unique.
    *
    * @param tr    The transaction that is being processed.
    * @param init The initial state of the traversal.
    * @param i     The step number during which the node is entered for the second time.
    * @see The corresponding latex document for a pen and paper proof.
    */
  @pure
  @opaque
  def findBeginRollback(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      i: BigInt,
  ): (BigInt, Tree[(NodeId, Node)]) = {
    require(tr.isUnique)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(scanTransaction(tr, init)(i)._2._2.isInstanceOf[Node.Rollback])
    require(scanTransaction(tr, init)(i)._3 == TraversalDirection.Up)

    findBeginRollback(tr, init, init, i)
  }.ensuring((j, sub) =>
    0 <= j && j < i && sub.size < tr.size &&
      (scanTransaction(tr, init)(j)._2 == scanTransaction(tr, init)(i)._2) &&
      (scanTransaction(tr, init)(j)._3 == TraversalDirection.Down) &&
      (scanTransaction(tr, init)(i)._1 == traverseTransaction(
        sub,
        beginRollback(scanTransaction(tr, init)(j)._1),
      ))
  )
}
