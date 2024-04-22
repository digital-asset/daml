// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified.tree

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
import transaction.CSMEither._
import transaction.CSMLocallyCreatedProperties._
import transaction.{State}
import transaction.CSMEitherDef._

import TransactionTreeDef._
import TransactionTree._

/** This file introduces two traversals whose purpose is to check whether the transaction is well-formed.
  *
  * The first one, [[TransactionTreeChecksDef.traverseUnbound]] checks whether for every contract in the
  * transaction is either bound to a key every time it appears in a node (the key should be the same but
  * this is not checked here yet) or it is not bound to any key every time it appears inside a node.
  *
  * The first one, [[TransactionTreeChecksDef.traverseLC]] checks whether for every contract in the
  * transaction is created only once and it has not been consumed before. By doing so, we collect all the
  * locally created and consumed contracts. We show later that the collected sets are actually the
  * [[State.locallyCreated]] and [[State.consumed]] fields of the state obtained after processing a
  * transaction. This allows us to isolate the two fields and manipulate them more easily.
  */
object TransactionTreeChecksDef {

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------UNBOUND-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** Function used the first time we visit a [[Node]] during the [[Tree]] traversal that gathers all the unbound
    * contracts i.e. those that are not associated to any key.
    *
    * @param s The state of the traversal before processing the node. It is a triple whose entries are:
    *          - The unbound contracts
    *          - The contracts not bound to any key
    *          - A boolean checking that no contract belongs to the two sets at the same time. In fact a tree where a
    *            contract appears both in a node with a well-defined key and at the same time in a node with no key, is
    *            not valid
    * @param p The node we are currently processing
    * @return The triple updated after processing the node. In particular checks that the new entry is not already
    *         contained in the second set if it is added to the first one and vice-versa.
    */
  @pure
  @opaque
  def unboundFun(
      s: (Set[ContractId], Set[ContractId], Boolean),
      p: (NodeId, Node),
  ): (Set[ContractId], Set[ContractId], Boolean) = {
    SetProperties.subsetOfReflexivity(s._1)
    SetProperties.subsetOfReflexivity(s._2)
    p._2 match {
      case Node.Create(coid, None()) => (s._1 + coid, s._2, s._3 && !s._2.contains(coid))
      case Node.Create(coid, _) => (s._1, s._2 + coid, s._3 && !s._1.contains(coid))
      case Node.Fetch(coid, None(), _) => (s._1 + coid, s._2, s._3 && !s._2.contains(coid))
      case Node.Fetch(coid, _, _) => (s._1, s._2 + coid, s._3 && !s._1.contains(coid))
      case Node.Exercise(targetCoid, _, _, None(), _) =>
        (s._1 + targetCoid, s._2, s._3 && !s._2.contains(targetCoid))
      case Node.Exercise(targetCoid, _, _, _, _) =>
        (s._1, s._2 + targetCoid, s._3 && !s._1.contains(targetCoid))
      case Node.LookupByKey(_, Some(coid)) => (s._1, s._2 + coid, s._3 && !s._1.contains(coid))
      case _ => s
    }
  }.ensuring(res =>
    s._1.subsetOf(res._1) &&
      s._2.subsetOf(res._2) &&
      (res._3 ==> s._3)
  )

  /** List of triples whose respective entries are:
    *  - The unbound, bound contracts and whether an error has been found before the i-th step of the traversal.
    *  - The pair node id - node that is handle during the i-th step
    *  - The direction i.e. if that's the first or the second time we enter the node
    *
    * @param tr The transaction that is being processed
    */
  @pure
  def scanUnbound(
      tr: Tree[(NodeId, Node)]
  ): List[((Set[ContractId], Set[ContractId], Boolean), (NodeId, Node), TraversalDirection)] = {
    tr.scan((Set.empty[ContractId], Set.empty[ContractId], true), unboundFun, (z, t) => z)
  }

  /** Set of unbound and bound contracts in the tree. A bound contract is a contract that has a global key assigned to
    * it.The third entry of the triple indicates whether an error has been found while traversing the tree.
    * If an error arises (the boolean is false) it is due to the fact that a contract appeared in a node that did not
    * have a key and at the same time in a node with a well-defined key.
    *
    * In fact both sets should be disjoint as we prove in [[TransactionTreeChecks.traverseUnboundProp]].
    *
    * @param tr The transaction that is being processed
    */
  @pure
  def traverseUnbound(tr: Tree[(NodeId, Node)]): (Set[ContractId], Set[ContractId], Boolean) = {
    tr.traverse((Set.empty[ContractId], Set.empty[ContractId], true), unboundFun, (z, t) => z)
  }

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------LC---------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** Function used the first time we visit a [[Node]] during the [[Tree]] traversal that gathers all the created and
    * consumed contract in the tree.
    *
    * @param s The state of the traversal before processing the node. It is a triple whose entries are:
    *          - The created contracts in the tree until that point
    *          - The consumed contracts in the tree until that point
    *          - A boolean checking that a contract is not created twice or that a consumed contract is not created again.
    * @param p The node we are currently processing
    * @return The triple updated after processing the node.
    */
  @pure
  @opaque
  def buildLC(
      s: (Set[ContractId], Set[ContractId], Boolean),
      p: (NodeId, Node),
  ): (Set[ContractId], Set[ContractId], Boolean) = {
    SetProperties.subsetOfReflexivity(s._1)
    SetProperties.subsetOfReflexivity(s._2)
    p._2 match {
      case Node.Create(coid, _) =>
        (s._1 + coid, s._2, s._3 && !s._1.contains(coid) && !s._2.contains(coid))
      case exe: Node.Exercise if exe.consuming =>
        (s._1, s._2 + exe.targetCoid, s._3)
      case _ => s
    }
  }.ensuring(res => s._1.subsetOf(res._1) && s._2.subsetOf(res._2) && (!s._3 ==> !res._3))

  /** List of triples whose respective entries are:
    *  - The locally created, consumed contracts and whether an error has been found before the i-th step of the traversal.
    *  - The pair node id - node that is handle during the i-th step
    *  - The direction i.e. if that's the first or the second time we enter the node
    *
    * @param tr       The transaction that is being processed
    * @param lc       The inital set of locally created contracts. Should be empty by default.
    * @param consumed The inital set of consumed contracts. Should be empty by default.
    * @param defined  Indicates whether there is already an error or not. Should be true by default (i.e. no error)
    */
  @pure
  def scanLC(
      tr: Tree[(NodeId, Node)],
      lc: Set[ContractId],
      consumed: Set[ContractId],
      defined: Boolean,
  ): List[((Set[ContractId], Set[ContractId], Boolean), (NodeId, Node), TraversalDirection)] = {
    tr.scan((lc, consumed, defined), buildLC, (z, t) => z)
  }

  /** Set of locally created and consumed contracts in the transaction. The third entry of the triple indicates whether
    * an error has been found while traversing the tree. If an error arises (the boolean is false) it can either be due
    * to:
    *  - A contract has been created twice with the same id
    *  - A contract has been created after it was consumed
    *
    * @param tr       The transaction that is being processed
    * @param lc       The inital set of locally created contracts. Should be empty by default.
    * @param consumed The inital set of consumed contracts. Should be empty by default.
    * @param defined  Indicates whether there is already an error or not. Should be true by default (i.e. no error)
    */
  @pure
  def traverseLC(
      tr: Tree[(NodeId, Node)],
      lc: Set[ContractId],
      consumed: Set[ContractId],
      defined: Boolean,
  ): (Set[ContractId], Set[ContractId], Boolean) = {
    tr.traverse((lc, consumed, defined), buildLC, (z, t) => z)
  }

}

object TransactionTreeChecks {

  import TransactionTreeChecksDef._

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------UNBOUND-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** Properties between intermediate states of the unbound contracts traversal and the final one
    *  - Any intermediate state of the set of unbound contracts is a subset of the final state, i.e. the set of unbound
    *    contracts of the transaction.
    *  - The same claim holds for bound contracts.
    *  - If there is no error in the end, then there is no error during any intermediate step.
    *  - For every step of the traversal, if an error did not arise, the set of bound and unbound contracts is disjoint
    *
    * The proof goes by contradiction.
    *
    * @param tr The tree that is being traversed
    * @param i  The number of the step we are looking at in the traversal
    */
  @pure
  @opaque
  def scanTraverseUnboundProp(tr: Tree[(NodeId, Node)], i: BigInt): Unit = {
    require(i < 2 * tr.size)
    require(0 <= i)

    val init = (Set.empty[ContractId], Set.empty[ContractId], true)
    val (tr1, tr2, tr3) = traverseUnbound(tr)

    // proving the first three claims by contradiction
    val p1: ((Set[ContractId], Set[ContractId], Boolean)) => Boolean = (s1, s2, b) =>
      s1.subsetOf(tr1) &&
        s2.subsetOf(tr2) &&
        (tr3 ==> b)

    SetProperties.subsetOfReflexivity(tr1)
    SetProperties.subsetOfReflexivity(tr2)

    if (!p1(scanUnbound(tr)(i)._1)) {
      val j = scanNotPropRev(tr, init, unboundFun, (z, t) => z, p1, i)
      val (sj1, sj2, _) = scanUnbound(tr)(j)._1

      SetProperties.subsetOfReflexivity(sj1)
      SetProperties.subsetOfReflexivity(sj2)
      if (j == 2 * tr.size - 1) {
        scanIndexingState(tr, init, unboundFun, (z, t) => z, 0)
      } else {
        scanIndexingState(tr, init, unboundFun, (z, t) => z, j + 1)
        SetProperties.subsetOfTransitivity(sj1, scanUnbound(tr)(j + 1)._1._1, tr1)
        SetProperties.subsetOfTransitivity(sj2, scanUnbound(tr)(j + 1)._1._2, tr2)
      }
    }

    // proving the last claim by contradiction as well
    val p2: ((Set[ContractId], Set[ContractId], Boolean)) => Boolean =
      (s1, s2, b) => b ==> s1.disjoint(s2)

    SetProperties.disjointEmpty(Set.empty[ContractId])
    if (!p2(scanUnbound(tr)(i)._1)) {
      val j = scanNotProp(tr, init, unboundFun, (z, t) => z, p2, i)
      scanIndexingState(tr, init, unboundFun, (z, t) => z, j + 1)
      val (s, p, _) = scanUnbound(tr)(j)
      unfold(unboundFun(s, p))

      if (!s._3) {
        Trivial()
      } else {
        p._2 match {
          case Node.Create(coid, None()) =>
            SetProperties.disjointSymmetry(s._2, s._1)
            SetProperties.disjointIncl(s._2, s._1, coid)
            SetProperties.disjointSymmetry(s._2, s._1 + coid)
          case Node.Create(coid, _) =>
            SetProperties.disjointIncl(s._1, s._2, coid)
          case Node.Fetch(coid, None(), _) =>
            SetProperties.disjointSymmetry(s._2, s._1)
            SetProperties.disjointIncl(s._2, s._1, coid)
            SetProperties.disjointSymmetry(s._2, s._1 + coid)
          case Node.Fetch(coid, _, _) =>
            SetProperties.disjointIncl(s._1, s._2, coid)
          case Node.Exercise(targetCoid, _, _, None(), _) =>
            SetProperties.disjointSymmetry(s._2, s._1)
            SetProperties.disjointIncl(s._2, s._1, targetCoid)
            SetProperties.disjointSymmetry(s._2, s._1 + targetCoid)
          case Node.Exercise(targetCoid, _, _, _, _) =>
            SetProperties.disjointIncl(s._1, s._2, targetCoid)
          case Node.LookupByKey(_, Some(coid)) =>
            SetProperties.disjointIncl(s._1, s._2, coid)
          case _ => Trivial()
        }
      }
    }

  }.ensuring(
    scanUnbound(tr)(i)._1._1.subsetOf(traverseUnbound(tr)._1) &&
      scanUnbound(tr)(i)._1._2.subsetOf(traverseUnbound(tr)._2) &&
      (traverseUnbound(tr)._3 ==> scanUnbound(tr)(i)._1._3) &&
      (scanUnbound(tr)(i)._1._3 ==> scanUnbound(tr)(i)._1._1.disjoint(scanUnbound(tr)(i)._1._2))
  )

  /** If at the end of the unbound contracts traversal no error did arise, then the set of unbound and bound contracts
    * are disjoint.
    *
    * @param tr The tree that is being traversed
    */
  @pure
  @opaque
  def traverseUnboundProp(tr: Tree[(NodeId, Node)]): Unit = {
    require(traverseUnbound(tr)._3)

    if (tr.size > 0) {
      scanTraverseUnboundProp(tr, 2 * tr.size - 1)
      scanIndexingState(
        tr,
        (Set.empty[ContractId], Set.empty[ContractId], true),
        unboundFun,
        (z, t) => z,
        0,
      )
    } else {
      SetProperties.disjointEmpty(Set.empty[ContractId])
    }

  }.ensuring(traverseUnbound(tr)._1.disjoint(traverseUnbound(tr)._2))

  /** When a node is processed for the first time, then its contract is contained in the set of unbound contracts if and
    * only if the node has no key. Similarly, the contract belongs to the bound contracts if and only if the key of the
    * node is well-defined.
    *
    * The claim is valid if no error arose in the traversal.
    *
    * @param tr       The tree that is being traversed.
    * @param i        The step during which the node is processed.
    */
  @pure
  @opaque
  def scanTraverseUnboundPropDown(tr: Tree[(NodeId, Node)], i: BigInt): Unit = {
    require(i < 2 * tr.size)
    require(0 <= i)
    require(traverseUnbound(tr)._3)
    require(scanUnbound(tr)(i)._3 == TraversalDirection.Down)

    if (i == 2 * tr.size - 1) {
      Unreachable()
    } else {
      scanIndexingState(tr, (Set.empty, Set.empty, true), unboundFun, (z, t) => z, i + 1)

      val (s, p, _) = scanUnbound(tr)(i)
      val (spu, spb, _) = scanUnbound(tr)(i + 1)._1
      val (tru, trb, _) = traverseUnbound(tr)

      unfold(unboundFun(s, p))

      scanTraverseUnboundProp(tr, i + 1)
      traverseUnboundProp(tr)

      p._2 match {
        case Node.Create(coid, None()) =>
          SetProperties.subsetOfContains(spu, tru, coid)
          SetProperties.disjointContains(tru, trb, coid)
        case Node.Create(coid, _) =>
          SetProperties.subsetOfContains(spb, trb, coid)
          SetProperties.disjointContains(tru, trb, coid)
        case Node.Exercise(targetCoid, _, _, None(), _) =>
          SetProperties.subsetOfContains(spu, tru, targetCoid)
          SetProperties.disjointContains(tru, trb, targetCoid)
        case Node.Exercise(targetCoid, _, _, _, _) =>
          SetProperties.subsetOfContains(spb, trb, targetCoid)
          SetProperties.disjointContains(tru, trb, targetCoid)
        case _ => Trivial()
      }
    }
  }.ensuring(
    scanUnbound(tr)(i)._2._2 match {
      case Node.Create(coid, opt) =>
        (traverseUnbound(tr)._1.contains(coid) == !opt.isDefined) && (traverseUnbound(tr)._2
          .contains(coid) == opt.isDefined)
      case exe: Node.Exercise =>
        (traverseUnbound(tr)._1
          .contains(exe.targetCoid) == !exe.gkeyOpt.isDefined) && (traverseUnbound(tr)._2
          .contains(exe.targetCoid) == exe.gkeyOpt.isDefined)
      case _ => true
    }
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------LC------------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** Properties between intermediate states of the locally created/consumed contracts traversal and the final one
    *  - Any intermediate state of the set of locally created contracts is a subset of the final state, i.e. the set of
    *    locally created contracts of the transaction.
    *  - The same claim holds for consumed contracts.
    *  - If there is no error in the end, then there is no error during any intermediate step.
    *
    * The proof goes by contradiction.
    *
    * @param tr       The tree that is being traversed
    * @param lc       The inital set of locally created contracts. Should be empty by default.
    * @param consumed The inital set of consumed contracts. Should be empty by default.
    * @param defined  Indicates whether there is already an error or not. Should be true by default (i.e. no error)
    * @param i        The number of the step we are looking at in the traversal
    */
  @pure
  @opaque
  def scanTraverseLCProp(
      tr: Tree[(NodeId, Node)],
      lc: Set[ContractId],
      consumed: Set[ContractId],
      defined: Boolean,
      i: BigInt,
  ): Unit = {
    decreases(2 * tr.size - i)
    require(i < 2 * tr.size)
    require(0 <= i)

    val p: ((Set[ContractId], Set[ContractId], Boolean)) => Boolean = (s1, s2, b) =>
      s1.subsetOf(traverseLC(tr, lc, consumed, defined)._1) &&
        s2.subsetOf(traverseLC(tr, lc, consumed, defined)._2) &&
        (traverseLC(tr, lc, consumed, defined)._3 ==> b)

    SetProperties.subsetOfReflexivity(tr.traverse((lc, consumed, defined), buildLC, (z, t) => z)._1)
    SetProperties.subsetOfReflexivity(tr.traverse((lc, consumed, defined), buildLC, (z, t) => z)._2)

    if (!p(tr.scan((lc, consumed, defined), buildLC, (z, t) => z)(i)._1)) {
      val j = scanNotPropRev(tr, (lc, consumed, defined), buildLC, (z, t) => z, p, i)
      SetProperties.subsetOfReflexivity(
        tr.scan((lc, consumed, defined), buildLC, (z, t) => z)(j)._1._1
      )
      SetProperties.subsetOfReflexivity(
        tr.scan((lc, consumed, defined), buildLC, (z, t) => z)(j)._1._2
      )
      if (j == 2 * tr.size - 1) {
        scanIndexingState(tr, (lc, consumed, defined), buildLC, (z, t) => z, 0)
      } else {
        scanIndexingState(tr, (lc, consumed, defined), buildLC, (z, t) => z, j + 1)
        SetProperties.subsetOfTransitivity(
          scanLC(tr, lc, consumed, defined)(j)._1._1,
          scanLC(tr, lc, consumed, defined)(j + 1)._1._1,
          traverseLC(tr, lc, consumed, defined)._1,
        )
        SetProperties.subsetOfTransitivity(
          scanLC(tr, lc, consumed, defined)(j)._1._2,
          scanLC(tr, lc, consumed, defined)(j + 1)._1._2,
          traverseLC(tr, lc, consumed, defined)._2,
        )
      }
    }

  }.ensuring(
    scanLC(tr, lc, consumed, defined)(i)._1._1.subsetOf(traverseLC(tr, lc, consumed, defined)._1) &&
      scanLC(tr, lc, consumed, defined)(i)._1._2
        .subsetOf(traverseLC(tr, lc, consumed, defined)._2) &&
      (traverseLC(tr, lc, consumed, defined)._3 ==> scanLC(tr, lc, consumed, defined)(i)._1._3)
  )

  /** If a contract is locally created it belongs to the set of locally created contracts at the end of the traversal.
    * The same holds when a contract is consumed. Furthermore when a contract is created, the set of locally created
    * before reaching this step does not contain it.
    *
    * The claim holds obviously only when the node is visited for the first time and if no error arose during the
    * traversal.
    *
    * @param tr       The tree that is being traversed.
    * @param lc       The inital set of locally created contracts. Should be empty by default.
    * @param consumed The inital set of consumed contracts. Should be empty by default.
    * @param defined  Indicates whether there is already an error or not. Should be true by default (i.e. no error).
    * @param i        The step during which the create or exercise node is processed.
    */
  @pure
  @opaque
  def scanTraverseLCPropDown(
      tr: Tree[(NodeId, Node)],
      lc: Set[ContractId],
      consumed: Set[ContractId],
      defined: Boolean,
      i: BigInt,
  ): Unit = {
    require(i < 2 * tr.size)
    require(0 <= i)
    require(traverseLC(tr, lc, consumed, defined)._3)
    require(scanLC(tr, lc, consumed, defined)(i)._3 == TraversalDirection.Down)

    if (i == 2 * tr.size - 1) {
      Unreachable()
    } else {
      scanIndexingState(tr, (lc, consumed, defined), buildLC, (z, t) => z, i + 1)
      scanTraverseLCProp(tr, lc, consumed, defined, i + 1)
      scanTraverseLCProp(tr, lc, consumed, defined, i)

      val (s, p, _) = scanLC(tr, lc, consumed, defined)(i)

      unfold(buildLC(s, p))
      p._2 match {
        case Node.Create(coid, _) =>
          SetProperties.subsetOfContains(
            scanLC(tr, lc, consumed, defined)(i + 1)._1._1,
            traverseLC(tr, lc, consumed, defined)._1,
            coid,
          )
        case exe: Node.Exercise if exe.consuming =>
          SetProperties.subsetOfContains(
            scanLC(tr, lc, consumed, defined)(i + 1)._1._2,
            traverseLC(tr, lc, consumed, defined)._2,
            exe.targetCoid,
          )
        case _ => Trivial()
      }
    }
  }.ensuring(
    scanLC(tr, lc, consumed, defined)(i)._2._2 match {
      case Node.Create(coid, mbKey) =>
        traverseLC(tr, lc, consumed, defined)._1.contains(coid) &&
        !scanLC(tr, lc, consumed, defined)(i)._1._1.contains(coid) &&
        !scanLC(tr, lc, consumed, defined)(i)._1._2.contains(coid)
      case exe: Node.Exercise if exe.consuming =>
        traverseLC(tr, lc, consumed, defined)._2.contains(exe.targetCoid)
      case _ => true
    }
  )

  /** If the initial parameters of the traversal are subset of or is implied the one of another traversal, then every
    * intermediate state of the first traversal will be a subset of or will by implied the same intermediate state of
    * the second traversal.
    *
    * @param tr       The tree that is being traversed.
    * @param lc1       The inital set of locally created contracts of the first traversal.
    * @param lc2       The inital set of locally created contracts of the second traversal.
    * @param consumed1 The inital set of consumed contracts of the first traversal.
    * @param consumed2 The inital set of consumed contracts of the second traversal.
    * @param defined1  Indicates whether there is already an error or not.
    * @param defined2  Indicates whether there is already an error or not.
    * @param i         The step number of the intermediate states we are looking at.
    */
  @pure
  @opaque
  def scanLCSubsetOf(
      tr: Tree[(NodeId, Node)],
      lc1: Set[ContractId],
      consumed1: Set[ContractId],
      defined1: Boolean,
      lc2: Set[ContractId],
      consumed2: Set[ContractId],
      defined2: Boolean,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i < 2 * tr.size)
    require(0 <= i)
    require(lc1.subsetOf(lc2))
    require(consumed1.subsetOf(consumed2))
    require(defined2 ==> defined1)

    scanIndexingState(tr, (lc1, consumed1, defined1), buildLC, (z, t) => z, i)
    scanIndexingState(tr, (lc2, consumed2, defined2), buildLC, (z, t) => z, i)

    if (i == 0) {
      Trivial()
    } else {
      scanLCSubsetOf(tr, lc1, consumed1, defined1, lc2, consumed2, defined2, i - 1)
      val (s11, s12, b13) = scanLC(tr, lc1, consumed1, defined1)(i - 1)._1
      val (s21, s22, b23) = scanLC(tr, lc2, consumed2, defined2)(i - 1)._1
      val n = scanLC(tr, lc1, consumed1, defined1)(i - 1)._2

      scanIndexingNode(
        tr,
        (lc1, consumed1, defined1),
        (lc2, consumed2, defined2),
        buildLC,
        (z, t) => z,
        buildLC,
        (z, t) => z,
        i - 1,
      )
      unfold(buildLC((s11, s12, b13), n))
      unfold(buildLC((s21, s22, b23), n))

      n._2 match {
        case Node.Create(coid, _) =>
          SetProperties.subsetOfIncl(s11, s21, coid)
          if (s11.contains(coid)) {
            SetProperties.subsetOfContains(s11, s21, coid)
          } else if (s12.contains(coid)) {
            SetProperties.subsetOfContains(s12, s22, coid)
          }
        case exe: Node.Exercise if exe.consuming =>
          SetProperties.subsetOfIncl(s12, s22, exe.targetCoid)
        case _ => Trivial()
      }
    }

  }.ensuring(
    scanLC(tr, lc1, consumed1, defined1)(i)._1._1
      .subsetOf(scanLC(tr, lc2, consumed2, defined2)(i)._1._1) &&
      scanLC(tr, lc1, consumed1, defined1)(i)._1._2
        .subsetOf(scanLC(tr, lc2, consumed2, defined2)(i)._1._2) &&
      (scanLC(tr, lc2, consumed2, defined2)(i)._1._3 ==> scanLC(tr, lc1, consumed1, defined1)(
        i
      )._1._3)
  )

  /** If the initial parameters of the traversal are subset of or is implied the one of another traversal, then the
    * resulting state of the first traversal will be a subset of or will by implied the resulting state of the second
    * traversal.
    *
    * @param tr        The tree that is being traversed.
    * @param lc1       The inital set of locally created contracts of the first traversal.
    * @param lc2       The inital set of locally created contracts of the second traversal.
    * @param consumed1 The inital set of consumed contracts of the first traversal.
    * @param consumed2 The inital set of consumed contracts of the second traversal.
    * @param defined1  Indicates whether there is already an error or not.
    * @param defined2  Indicates whether there is already an error or not.
    */
  @pure
  @opaque
  def traverseLCSubsetOf(
      tr: Tree[(NodeId, Node)],
      lc1: Set[ContractId],
      consumed1: Set[ContractId],
      defined1: Boolean,
      lc2: Set[ContractId],
      consumed2: Set[ContractId],
      defined2: Boolean,
  ): Unit = {
    require(lc1.subsetOf(lc2))
    require(consumed1.subsetOf(consumed2))
    require(defined2 ==> defined1)

    if (tr.size == 0) {
      Trivial()
    } else {

      scanIndexingState(tr, (lc1, consumed1, defined1), buildLC, (z, t) => z, 0)
      scanIndexingState(tr, (lc2, consumed2, defined2), buildLC, (z, t) => z, 0)

      scanLCSubsetOf(tr, lc1, consumed1, defined1, lc2, consumed2, defined2, 2 * tr.size - 1)
      val (s11, s12, b13) = scanLC(tr, lc1, consumed1, defined1)(2 * tr.size - 1)._1
      val (s21, s22, b23) = scanLC(tr, lc2, consumed2, defined2)(2 * tr.size - 1)._1
      val n = scanLC(tr, lc1, consumed1, defined1)(2 * tr.size - 1)._2

      scanIndexingNode(
        tr,
        (lc1, consumed1, defined1),
        (lc2, consumed2, defined2),
        buildLC,
        (z, t) => z,
        buildLC,
        (z, t) => z,
        2 * tr.size - 1,
      )
      unfold(buildLC((s11, s12, b13), n))
      unfold(buildLC((s21, s22, b23), n))

      n._2 match {
        case Node.Create(coid, _) =>
          SetProperties.subsetOfIncl(s11, s21, coid)
          if (s11.contains(coid)) {
            SetProperties.subsetOfContains(s11, s21, coid)
          } else if (s12.contains(coid)) {
            SetProperties.subsetOfContains(s12, s22, coid)
          }
        case exe: Node.Exercise if exe.consuming =>
          SetProperties.subsetOfIncl(s12, s22, exe.targetCoid)
        case _ => Trivial()
      }

    }

  }.ensuring(
    traverseLC(tr, lc1, consumed1, defined1)._1
      .subsetOf(traverseLC(tr, lc2, consumed2, defined2)._1) &&
      traverseLC(tr, lc1, consumed1, defined1)._2.subsetOf(
        traverseLC(tr, lc2, consumed2, defined2)._2
      ) &&
      (traverseLC(tr, lc2, consumed2, defined2)._3 ==> traverseLC(tr, lc1, consumed1, defined1)._3)
  )

  /** Any intermediate set of locally created contracts in the traversal is independent of the initial set. That is
    * we can extract the set of the initial state, run the traversal with the empty set and afterward computing the union
    * between the result and the inital set.
    *
    * @param tr        The tree that is being traversed.
    * @param lc The inital set of locally created contracts of the original traversal.
    * @param consumed1 The inital set of consumed contracts of the original traversal.
    * @param consumed2 The inital set of consumed contracts of the traversal with an empty locally created contracts set.
    * @param defined  Indicates whether there is already an error or not.
    * @param i         The step number of the intermediate set we are looking at.
    */
  def scanLCExtractInitLC(
      tr: Tree[(NodeId, Node)],
      lc: Set[ContractId],
      consumed1: Set[ContractId],
      consumed2: Set[ContractId],
      defined: Boolean,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i < 2 * tr.size)
    require(0 <= i)

    scanIndexingState(tr, (lc, consumed1, defined), buildLC, (z, t) => z, i)
    scanIndexingState(tr, (Set.empty[ContractId], consumed2, defined), buildLC, (z, t) => z, i)

    if (i == 0) {
      SetProperties.unionEmpty(lc)
    } else {
      scanLCExtractInitLC(tr, lc, consumed1, consumed2, defined, i - 1)
      scanIndexingNode(
        tr,
        (lc, consumed1, defined),
        (Set.empty[ContractId], consumed2, defined),
        buildLC,
        (z, t) => z,
        buildLC,
        (z, t) => z,
        i - 1,
      )

      val (si, n, dir) = scanLC(tr, lc, consumed1, defined)(i - 1)
      val se = scanLC(tr, Set.empty[ContractId], consumed2, defined)(i - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(buildLC(si, n))
        unfold(buildLC(se, n))

        n._2 match {
          case Node.Create(coid, _) =>
            SetProperties.equalsIncl(se._1 ++ lc, si._1, coid)
            unfold((se._1 ++ lc).incl(coid))
            SetProperties.unionAssociativity(se._1, lc, Set(coid))
            SetProperties.unionCommutativity(lc, Set(coid))
            SetProperties.unionAssociativity(se._1, Set(coid), lc)
            unfold(si._1.incl(coid))
            unfold(se._1.incl(coid))
            SetProperties.unionEqualsRight(se._1, Set(coid) ++ lc, lc ++ Set(coid))
            SetProperties.equalsTransitivity(
              (se._1 ++ Set(coid)) ++ lc,
              se._1 ++ (Set(coid) ++ lc),
              se._1 ++ (lc ++ Set(coid)),
            )
            SetProperties.equalsTransitivity(
              (se._1 ++ Set(coid)) ++ lc,
              se._1 ++ (lc ++ Set(coid)),
              (se._1 ++ lc) ++ Set(coid),
            )
            SetProperties.equalsTransitivity(
              (se._1 ++ Set(coid)) ++ lc,
              (se._1 ++ lc) ++ Set(coid),
              si._1 ++ Set(coid),
            )
          case _ => Trivial()
        }
      }

    }

  }.ensuring(
    scanLC(tr, Set.empty[ContractId], consumed2, defined)(i)._1._1 ++ lc ===
      scanLC(tr, lc, consumed1, defined)(i)._1._1
  )

  /** The set of locally created contracts obtained at the end of the traversal is independent of the initial set. That is
    * we can extract the set of the initial state, run the traversal with the empty set and afterward computing the union
    * between the result and the inital set.
    *
    * @param tr        The tree that is being traversed.
    * @param lc       The inital set of locally created contracts of the original traversal.
    * @param consumed1 The inital set of consumed contracts of the original traversal.
    * @param consumed2 The inital set of consumed contracts of the traversal with an empty locally created contracts set.
    * @param defined   Indicates whether there is already an error or not.
    */
  def traverseLCExtractInitLC(
      tr: Tree[(NodeId, Node)],
      lc: Set[ContractId],
      consumed1: Set[ContractId],
      consumed2: Set[ContractId],
      defined: Boolean,
  ): Unit = {

    if (tr.size > 0) {

      scanIndexingState(tr, (lc, consumed1, defined), buildLC, (z, t) => z, 0)
      scanIndexingState(tr, (Set.empty[ContractId], consumed2, defined), buildLC, (z, t) => z, 0)

      scanLCExtractInitLC(tr, lc, consumed1, consumed2, defined, 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        (lc, consumed1, defined),
        (Set.empty[ContractId], consumed2, defined),
        buildLC,
        (z, t) => z,
        buildLC,
        (z, t) => z,
        2 * tr.size - 1,
      )

      val (si, n, dir) = scanLC(tr, lc, consumed1, defined)(2 * tr.size - 1)
      val se = scanLC(tr, Set.empty[ContractId], consumed2, defined)(2 * tr.size - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(buildLC(si, n))
        unfold(buildLC(se, n))

        n._2 match {
          case Node.Create(coid, _) =>
            SetProperties.equalsIncl(se._1 ++ lc, si._1, coid)
            unfold((se._1 ++ lc).incl(coid))
            SetProperties.unionAssociativity(se._1, lc, Set(coid))
            SetProperties.unionCommutativity(lc, Set(coid))
            SetProperties.unionAssociativity(se._1, Set(coid), lc)
            unfold(si._1.incl(coid))
            unfold(se._1.incl(coid))
            SetProperties.unionEqualsRight(se._1, Set(coid) ++ lc, lc ++ Set(coid))
            SetProperties.equalsTransitivity(
              (se._1 ++ Set(coid)) ++ lc,
              se._1 ++ (Set(coid) ++ lc),
              se._1 ++ (lc ++ Set(coid)),
            )
            SetProperties.equalsTransitivity(
              (se._1 ++ Set(coid)) ++ lc,
              se._1 ++ (lc ++ Set(coid)),
              (se._1 ++ lc) ++ Set(coid),
            )
            SetProperties.equalsTransitivity(
              (se._1 ++ Set(coid)) ++ lc,
              (se._1 ++ lc) ++ Set(coid),
              si._1 ++ Set(coid),
            )
          case _ => Trivial()
        }
      }

    } else {
      SetProperties.unionEmpty(lc)
    }

  }.ensuring(
    traverseLC(tr, Set.empty[ContractId], consumed2, defined)._1 ++ lc ===
      traverseLC(tr, lc, consumed1, defined)._1
  )

  /** Any intermediate set of consumed contracts in the traversal is independent of the initial set. That is
    * we can extract the set of the initial state, run the traversal with the empty set and afterward computing the union
    * between the result and the inital set.
    *
    * @param tr       The tree that is being traversed.
    * @param lc1      The inital set of locally created contracts of the original traversal.
    * @param lc2      The inital set of locally created contracts of the traversal with an empty consumed contracts set.
    * @param consumed The inital set of consumed contracts of the original traversal.
    * @param defined  Indicates whether there is already an error or not.
    * @param i         The step number of the intermediate set we are looking at.
    */
  def scanLCExtractInitConsumed(
      tr: Tree[(NodeId, Node)],
      lc1: Set[ContractId],
      lc2: Set[ContractId],
      consumed: Set[ContractId],
      defined: Boolean,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(i < 2 * tr.size)
    require(0 <= i)

    scanIndexingState(tr, (lc1, consumed, defined), buildLC, (z, t) => z, i)
    scanIndexingState(tr, (lc2, Set.empty[ContractId], defined), buildLC, (z, t) => z, i)

    if (i == 0) {
      SetProperties.unionEmpty(consumed)
    } else {
      scanLCExtractInitConsumed(tr, lc1, lc2, consumed, defined, i - 1)
      scanIndexingNode(
        tr,
        (lc1, consumed, defined),
        (lc2, Set.empty[ContractId], defined),
        buildLC,
        (z, t) => z,
        buildLC,
        (z, t) => z,
        i - 1,
      )

      val (si, n, dir) = scanLC(tr, lc1, consumed, defined)(i - 1)
      val se = scanLC(tr, lc2, Set.empty[ContractId], defined)(i - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(buildLC(si, n))
        unfold(buildLC(se, n))

        n._2 match {
          case exe: Node.Exercise if exe.consuming =>
            SetProperties.equalsIncl(se._2 ++ consumed, si._2, exe.targetCoid)
            unfold((se._2 ++ consumed).incl(exe.targetCoid))
            SetProperties.unionAssociativity(se._2, consumed, Set(exe.targetCoid))
            SetProperties.unionCommutativity(consumed, Set(exe.targetCoid))
            SetProperties.unionAssociativity(se._2, Set(exe.targetCoid), consumed)
            unfold(si._2.incl(exe.targetCoid))
            unfold(se._2.incl(exe.targetCoid))
            SetProperties.unionEqualsRight(
              se._2,
              Set(exe.targetCoid) ++ consumed,
              consumed ++ Set(exe.targetCoid),
            )
            SetProperties.equalsTransitivity(
              (se._2 ++ Set(exe.targetCoid)) ++ consumed,
              se._2 ++ (Set(exe.targetCoid) ++ consumed),
              se._2 ++ (consumed ++ Set(exe.targetCoid)),
            )
            SetProperties.equalsTransitivity(
              (se._2 ++ Set(exe.targetCoid)) ++ consumed,
              se._2 ++ (consumed ++ Set(exe.targetCoid)),
              (se._2 ++ consumed) ++ Set(exe.targetCoid),
            )
            SetProperties.equalsTransitivity(
              (se._2 ++ Set(exe.targetCoid)) ++ consumed,
              (se._2 ++ consumed) ++ Set(exe.targetCoid),
              si._2 ++ Set(exe.targetCoid),
            )
          case _ => Trivial()
        }
      }

    }

  }.ensuring(
    scanLC(tr, lc2, Set.empty[ContractId], defined)(i)._1._2 ++ consumed ===
      scanLC(tr, lc1, consumed, defined)(i)._1._2
  )

  /** The set of consumed contracts obtained at the end of the traversal is independent of the initial set. That is
    * we can extract the set of the initial state, run the traversal with the empty set and afterward computing the union
    * between the result and the inital set.
    *
    * @param tr        The tree that is being traversed.
    * @param lc1       The inital set of locally created contracts of the original traversal.
    * @param lc2       The inital set of locally created contracts of the traversal with an empty consumed contracts set.
    * @param consumed The inital set of consumed contracts of the original traversal.
    * @param defined   Indicates whether there is already an error or not.
    */
  def traverseLCExtractInitConsumed(
      tr: Tree[(NodeId, Node)],
      lc1: Set[ContractId],
      lc2: Set[ContractId],
      consumed: Set[ContractId],
      defined: Boolean,
  ): Unit = {

    if (tr.size > 0) {

      scanIndexingState(tr, (lc1, consumed, defined), buildLC, (z, t) => z, 0)
      scanIndexingState(tr, (lc2, Set.empty[ContractId], defined), buildLC, (z, t) => z, 0)

      scanLCExtractInitConsumed(tr, lc1, lc2, consumed, defined, 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        (lc1, consumed, defined),
        (lc2, Set.empty[ContractId], defined),
        buildLC,
        (z, t) => z,
        buildLC,
        (z, t) => z,
        2 * tr.size - 1,
      )

      val (si, n, dir) = scanLC(tr, lc1, consumed, defined)(2 * tr.size - 1)
      val se = scanLC(tr, lc2, Set.empty[ContractId], defined)(2 * tr.size - 1)._1

      if (dir == TraversalDirection.Down) {
        unfold(buildLC(si, n))
        unfold(buildLC(se, n))

        n._2 match {
          case exe: Node.Exercise if exe.consuming =>
            SetProperties.equalsIncl(se._2 ++ consumed, si._2, exe.targetCoid)
            unfold((se._2 ++ consumed).incl(exe.targetCoid))
            SetProperties.unionAssociativity(se._2, consumed, Set(exe.targetCoid))
            SetProperties.unionCommutativity(consumed, Set(exe.targetCoid))
            SetProperties.unionAssociativity(se._2, Set(exe.targetCoid), consumed)
            unfold(si._2.incl(exe.targetCoid))
            unfold(se._2.incl(exe.targetCoid))
            SetProperties.unionEqualsRight(
              se._2,
              Set(exe.targetCoid) ++ consumed,
              consumed ++ Set(exe.targetCoid),
            )
            SetProperties.equalsTransitivity(
              (se._2 ++ Set(exe.targetCoid)) ++ consumed,
              se._2 ++ (Set(exe.targetCoid) ++ consumed),
              se._2 ++ (consumed ++ Set(exe.targetCoid)),
            )
            SetProperties.equalsTransitivity(
              (se._2 ++ Set(exe.targetCoid)) ++ consumed,
              se._2 ++ (consumed ++ Set(exe.targetCoid)),
              (se._2 ++ consumed) ++ Set(exe.targetCoid),
            )
            SetProperties.equalsTransitivity(
              (se._2 ++ Set(exe.targetCoid)) ++ consumed,
              (se._2 ++ consumed) ++ Set(exe.targetCoid),
              si._2 ++ Set(exe.targetCoid),
            )
          case _ => Trivial()
        }
      }
    } else {
      SetProperties.unionEmpty(consumed)
    }

  }.ensuring(
    traverseLC(tr, lc2, Set.empty[ContractId], defined)._2 ++ consumed ===
      traverseLC(tr, lc1, consumed, defined)._2
  )

  /** Key theorem making the link between the locally created/consumed traversal and the classical transaction traversal.
    *
    * At any point in both traversals, the set of locally created and consumed contracts in the tree until that point
    * are equal to the locallyCreated and consumed fields of the state obtained while processing the transaction until
    * that point. In that case the starting sets of the former traversal are the fields of the initial state of the latter.
    *
    * @param tr The transaction that is being processed
    * @param init The initial state of the classical transaction traversal
    * @param defined If there is already an error when the checks are performed. Should be true by default.
    * @param i The step of the traversal we are looking at.
    */
  @pure
  @opaque
  def scanTransactionLC(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      defined: Boolean,
      i: BigInt,
  ): Unit = {

    decreases(i)

    require(0 <= i)
    require(i < 2 * tr.size)
    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)

    val initTr = (init.get.locallyCreated, init.get.consumed, defined)

    scanIndexingState(tr, init, traverseInFun, traverseOutFun, i)
    scanIndexingState(tr, initTr, buildLC, (z, t) => z, i)

    if (i == 0) {
      Trivial()
    } else {

      val (si, n, dir) = scanTransaction(tr, init)(i - 1)
      val lci = scanLC(tr, init.get.locallyCreated, init.get.consumed, defined)(i - 1)._1

      scanTransactionProp(tr, init, i - 1, i)
      unfold(propagatesError(si, scanTransaction(tr, init)(i)._1))
      scanTransactionLC(tr, init, defined, i - 1)
      scanIndexingNode(tr, init, initTr, traverseInFun, traverseOutFun, buildLC, (z, t) => z, i - 1)

      if (dir == TraversalDirection.Down) {
        unfold(buildLC(lci, n))
        unfold(traverseInFun(si, n))
        val snext = traverseInFun(si, n)

        n._2 match {
          case a: Node.Action =>
            handleNodeLocallyCreated(si, n._1, a)
            handleNodeConsumed(si, n._1, a)
          case _ =>
            unfold(sameLocallyCreated(si, snext))
            unfold(sameLocallyCreated(si.get, snext))
            unfold(sameConsumed(si, snext))
            unfold(sameConsumed(si.get, snext))
        }
      } else {
        val snext = traverseOutFun(si, n)
        unfold(sameLocallyCreated(si, snext))
        unfold(sameLocallyCreated(si.get, snext))
        unfold(sameConsumed(si, snext))
        unfold(sameConsumed(si.get, snext))
      }
    }
  }.ensuring(
    (scanTransaction(tr, init)(i)._1.get.locallyCreated == scanLC(
      tr,
      init.get.locallyCreated,
      init.get.consumed,
      defined,
    )(i)._1._1) &&
      (scanTransaction(tr, init)(i)._1.get.consumed == scanLC(
        tr,
        init.get.locallyCreated,
        init.get.consumed,
        defined,
      )(i)._1._2)
  )

  /** Key theorem making the link between the locally created/consumed traversal and the classical transaction traversal.
    *
    * The set of locally created and consumed contracts in the tree are equal to the locallyCreated and consumed fields
    * of the state obtained after processing the transaction.
    * In that case the starting sets of the former traversal are the fields of the initial state of the latter.
    *
    * @param tr      The transaction that is being processed
    * @param init    The initial state of the classical transaction traversal
    * @param defined If there is already an error when the checks are performed. Should be true by default.
    */
  @pure
  @opaque
  def traverseTransactionLC(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      defined: Boolean,
  ): Unit = {

    require(init.isRight)
    require(traverseTransaction(tr, init).isRight)

    if (tr.size == 0) {
      Trivial()
    } else {
      val initTr = (init.get.locallyCreated, init.get.consumed, defined)

      scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
      scanIndexingState(tr, initTr, buildLC, (z, t) => z, 0)

      val (si, n, dir) = scanTransaction(tr, init)(2 * tr.size - 1)
      val lci = scanLC(tr, init.get.locallyCreated, init.get.consumed, defined)(2 * tr.size - 1)._1

      traverseTransactionDefined(tr, init, 2 * tr.size - 1)
      unfold(propagatesError(si, traverseTransaction(tr, init)))
      scanTransactionLC(tr, init, defined, 2 * tr.size - 1)
      scanIndexingNode(
        tr,
        init,
        initTr,
        traverseInFun,
        traverseOutFun,
        buildLC,
        (z, t) => z,
        2 * tr.size - 1,
      )

      if (dir == TraversalDirection.Down) {
        unfold(buildLC(lci, n))
        unfold(traverseInFun(si, n))
        val snext = traverseInFun(si, n)

        n._2 match {
          case a: Node.Action =>
            handleNodeLocallyCreated(si, n._1, a)
            handleNodeConsumed(si, n._1, a)
          case _ =>
            unfold(sameLocallyCreated(si, snext))
            unfold(sameLocallyCreated(si.get, snext))
            unfold(sameConsumed(si, snext))
            unfold(sameConsumed(si.get, snext))
        }
      } else {
        val snext = traverseOutFun(si, n)
        unfold(sameLocallyCreated(si, snext))
        unfold(sameLocallyCreated(si.get, snext))
        unfold(sameConsumed(si, snext))
        unfold(sameConsumed(si.get, snext))
      }
    }
  }.ensuring(
    (traverseTransaction(tr, init).get.locallyCreated == traverseLC(
      tr,
      init.get.locallyCreated,
      init.get.consumed,
      defined,
    )._1) &&
      (traverseTransaction(tr, init).get.consumed == traverseLC(
        tr,
        init.get.locallyCreated,
        init.get.consumed,
        defined,
      )._2)
  )

}
