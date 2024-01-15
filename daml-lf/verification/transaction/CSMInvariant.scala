// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package transaction

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
import scala.annotation.targetName
import stainless.collection._
import utils.Value.ContractId
import utils.Transaction.{DuplicateContractKey, InconsistentContractKey, KeyInputError}
import utils._

import ContractStateMachine._
import CSMHelpers._
import CSMKeysPropertiesDef._
import CSMKeysProperties._
import CSMEitherDef._

object CSMInvariantDef {

  /** *
    * List of invariants that hold for well-defined transactions
    *
    * The following relationships tie together the various sets of ContractId.
    * Let
    * globalIds           :=  globalKeys.values.filter(_.isDefined)
    * act.localIds        :=  act.localKeys.values
    *
    * a. unbound is disjoint from globalIds
    *
    * b. for every act in the rollbackStack and for the active state:
    * act.localIds ⊆ locallyCreated ⊆ lc
    *
    * c. lc.map(KeyActive) is disjoint from globalIds
    *
    * d. for every act in the rollbackStack and for the active state:
    * act.localIds is disjoint from unbound
    *
    * e. for every act in the rollbackStack and for the active state:
    * act.consumedBy.values ⊆ consumed
    *
    * f. for every contractId v in globalIds:
    * globalKeys.preimage(v).size <= 1
    *
    * g. for every contractId v in act.localIds
    * act.localKeys.preimage(v).size <= 1
    *
    * Conditions e and f mean that both maps are injective, i.e. that no contract is assigned to two keys
    * at the same time.
    *
    * Regarding the keys the only invariant is that local keys are a subset of global keys
    *
    * @param s
    * The state for which those invariants hold
    *
    * @param lc
    * Set of contracts contracted during the transaction, before executing
    * @param unbound
    * Set of contracts created throughout the transaction but not linked to any key
    */

  @pure
  def invariantWrtActiveState(
      s: State
  )(unbound: Set[ContractId], lc: Set[ContractId]): ActiveLedgerState => Boolean =
    a =>
      // point b.1.
      a.localKeys.values.subsetOf(s.locallyCreated) &&
        // point d.
        unbound.disjoint(a.localKeys.values) &&
        // point e.
        a.consumedBy.keySet.subsetOf(s.consumed) &&
        // point g.
        (a.localKeys.values).forall(v => a.localKeys.preimage(v).size <= BigInt(1)) &&
        // keySets
        a.localKeys.keySet.subsetOf(s.globalKeys.keySet)

  @pure
  def invariantWrtList(s: State)(unbound: Set[ContractId], lc: Set[ContractId])(
      l: List[ActiveLedgerState]
  ): Boolean = l.forall(invariantWrtActiveState(s)(unbound, lc))

  @pure
  def stateInvariant(s: State)(unbound: Set[ContractId], lc: Set[ContractId]): Boolean = {

    val invariant: Boolean = {

      // point a.
      unbound.map[KeyMapping](KeyActive).disjoint(s.globalKeys.values) &&
      // point b.2.
      s.locallyCreated.subsetOf(lc) &&
      // point c.
      lc.map[KeyMapping](KeyActive).disjoint(s.globalKeys.values) &&
      // point f.
      (s.globalKeys.values
        .filter(_.isDefined))
        .forall(v => s.globalKeys.preimage(v).size <= BigInt(1)) &&
      // the invariants hold for the active state
      invariantWrtActiveState(s)(unbound, lc)(s.activeState) &&
      // the invariants hold for all the states in the stack
      invariantWrtList(s)(unbound, lc)(s.rollbackStack)
    }

    /** Properties that can be derived from the invariants:
      * - globalKeys.keySet == activeKeys.keySet
      */
    @opaque
    def invariantProperties: Unit = {
      require(invariant)
      unfold(invariantWrtActiveState)

      // globalKeys.keySet == activeKeys.keySet
      unfold(s.activeKeys)
      activeKeysKeySet(s)
      SetProperties.unionAltDefinition(s.activeState.localKeys.keySet, s.globalKeys.keySet)
      SetProperties.equalsTransitivityStrong(
        s.globalKeys.keySet,
        s.globalKeys.keySet ++ s.activeState.localKeys.keySet,
        s.activeKeys.keySet,
      )

      // localKeys.values.subsetOf(lc)
      SetProperties.subsetOfTransitivity(s.activeState.localKeys.values, s.locallyCreated, lc)

      // localKeys.values.map(KeyActive).disjoint(s.globalKeys.values)
      SetProperties.mapSubsetOf[ContractId, KeyMapping](
        s.activeState.localKeys.values,
        lc,
        KeyActive,
      )
      SetProperties.disjointSubsetOf(
        lc.map[KeyMapping](KeyActive),
        s.activeState.localKeys.values.map[KeyMapping](KeyActive),
        s.globalKeys.values,
      )

    }.ensuring(
      (s.globalKeys.keySet === s.activeKeys.keySet) &&
        s.activeState.localKeys.values.map[KeyMapping](KeyActive).disjoint(s.globalKeys.values)
    )

    if (invariant) {
      invariantProperties
    }
    invariant

  }

  /** Properties that need to be true for any pair intermediate state - node during a transaction so that the invariants
    * are preserved.
    */

  @pure
  @opaque
  def stateNodeCompatibility(
      s: State,
      n: Node,
      unbound: Set[ContractId],
      lc: Set[ContractId],
      dir: TraversalDirection,
  ): Boolean = {
    (dir == TraversalDirection.Down) ==>
      (n match {
        case Node.Create(coid, mbKey) =>
          lc.contains(coid) &&
          (mbKey.isDefined == !unbound.contains(coid)) &&
          !s.locallyCreated.contains(coid) &&
          !s.consumed.contains(coid)
        case exe: Node.Exercise => exe.gkeyOpt.isDefined == !unbound.contains(exe.targetCoid)
        case _ => true
      })
  }

  /** The invariants are considered true for an error state which means that they are preserved even when the transaction
    * leads to an error
    */
  @pure
  def invariantWrtList[T](e: Either[T, State])(unbound: Set[ContractId], lc: Set[ContractId])(
      l: List[ActiveLedgerState]
  ): Boolean =
    e.forall(s => invariantWrtList(s)(unbound, lc)(l))

  @pure
  def stateInvariant[T](
      e: Either[T, State]
  )(unbound: Set[ContractId], lc: Set[ContractId]): Boolean = {
    e.forall(s => stateInvariant(s)(unbound, lc))
  }

}

/** The scope of this object is to prove that the invariants are preserved when traversing a transaction. This includes
  * proving that they still hold:
  * - after handling a node (given some conditions on the node that hold for well defined transactions)
  * - after entering or exiting a rollback node
  * - after adding a global mapping (given some conditions on the mapping)
  */

object CSMInvariant {

  import CSMInvariantDef._

  /** If a state fulfills the invariants then the state obtained after calling beginRollback also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantBeginRollback(s: State, unbound: Set[ContractId], lc: Set[ContractId]): Unit = {
    require(stateInvariant(s)(unbound, lc))
    unfold(s.beginRollback())
  }.ensuring(stateInvariant(s.beginRollback())(unbound, lc))

  /** If a state fulfills the invariants then the state obtained after calling endRollback also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantEndRollback(s: State, unbound: Set[ContractId], lc: Set[ContractId]): Unit = {
    require(stateInvariant(s)(unbound, lc))
    unfold(s.endRollback())
  }.ensuring(stateInvariant(s.endRollback())(unbound, lc))

  /** If a state fulfills the invariants and an other state has its same fields except for the locallyCreatedThisTimeline
    * then it also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantSameFields[T](
      s1: State,
      s2: Either[T, State],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s1)(unbound, lc))
    require(sameLocalKeys(s1, s2))
    require(sameConsumedBy(s1, s2))
    require(sameGlobalKeys(s1, s2))
    require(sameStack(s1, s2))
    require(sameLocallyCreated(s1, s2))
    require(sameConsumed(s1, s2))

    unfold(sameLocalKeys(s1, s2))
    unfold(sameConsumedBy(s1, s2))
    unfold(sameGlobalKeys(s1, s2))
    unfold(sameStack(s1, s2))
    unfold(sameLocallyCreated(s1, s2))
    unfold(sameConsumed(s1, s2))
  }.ensuring(stateInvariant(s2)(unbound, lc))

  /** If a state fulfills the invariants and an other state has its same fields except for the locallyCreatedThisTimeline
    * then it also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantSameFields[U, T](
      s1: Either[U, State],
      s2: Either[T, State],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s1)(unbound, lc))
    require(sameLocalKeys(s1, s2))
    require(sameConsumedBy(s1, s2))
    require(sameGlobalKeys(s1, s2))
    require(sameStack(s1, s2))
    require(sameLocallyCreated(s1, s2))
    require(sameConsumed(s1, s2))
    require(propagatesError(s1, s2)) // we want to avoid the case s1.isLeft and s2.isRight

    unfold(sameLocalKeys(s1, s2))
    unfold(sameConsumedBy(s1, s2))
    unfold(sameGlobalKeys(s1, s2))
    unfold(sameStack(s1, s2))
    unfold(sameLocallyCreated(s1, s2))
    unfold(sameConsumed(s1, s2))
    unfold(propagatesError(s1, s2))

    s1 match {
      case Right(s) => stateInvariantSameFields(s, s2, unbound, lc)
      case Left(_) => Trivial()
    }
  }.ensuring(stateInvariant(s2)(unbound, lc))

  /** If a state fulfills the invariants and an other state is equal to it then it also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantSameState[T](
      s1: State,
      s2: Either[T, State],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s1)(unbound, lc))
    require(sameState(s1, s2))

    stateInvariantSameFields(s1, s2, unbound, lc)
  }.ensuring(stateInvariant(s2)(unbound, lc))

  /** If a state fulfills the invariants and an other state is equal to it then it also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantSameState[U, T](
      s1: Either[U, State],
      s2: Either[T, State],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s1)(unbound, lc))
    require(sameState(s1, s2))
    require(propagatesError(s1, s2)) // we want to avoid the case s1.isLeft and s2.isRight

    stateInvariantSameFields(s1, s2, unbound, lc)
  }.ensuring(stateInvariant(s2)(unbound, lc))

  /** If a state fulfills the invariants then the state obtained after calling assertKeyMapping also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantAssertKeyMapping(
      s: State,
      cid: ContractId,
      mbKey: Option[GlobalKey],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))

    stateInvariantSameState(s, s.assertKeyMapping(cid, mbKey), unbound, lc)
  }.ensuring(stateInvariant(s.assertKeyMapping(cid, mbKey))(unbound, lc))

  /** If a state fulfills the invariants then the state obtained after calling visitLookup also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantVisitLookup(
      s: State,
      gk: GlobalKey,
      keyResolution: Option[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))

    stateInvariantSameState(s, s.visitLookup(gk, keyResolution), unbound, lc)
  }.ensuring(stateInvariant(s.visitLookup(gk, keyResolution))(unbound, lc))

  /** If a state fulfills the invariants then the state obtained after calling visitExercise also fulfills the invariants.
    */
  @pure
  @opaque
  def stateInvariantVisitExercise(
      s: State,
      nodeId: NodeId,
      targetId: ContractId,
      mbKey: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))
    unfold(s.visitExercise(nodeId, targetId, mbKey, byKey, consuming))

    s.assertKeyMapping(targetId, mbKey) match {
      case Left(_) => Trivial()
      case Right(state) =>
        unfold(sameState(s, s.assertKeyMapping(targetId, mbKey)))
        if (consuming) {
          unfold(state.consume(targetId, nodeId))
          unfold(state.activeState.consume(targetId, nodeId))
          SetProperties.subsetOfIncl(s.activeState.consumedBy.keySet, s.consumed, targetId)
          SetProperties.equalsSubsetOfTransitivity(
            s.activeState.consumedBy.updated(targetId, nodeId).keySet,
            s.activeState.consumedBy.keySet + targetId,
            s.consumed + targetId,
          )

          val res = state.consume(targetId, nodeId)

          if (!invariantWrtList(res)(unbound, lc)(s.rollbackStack)) {
            val a = ListProperties.notForallWitness(
              s.rollbackStack,
              invariantWrtActiveState(res)(unbound, lc),
            )
            ListProperties.forallContains(
              s.rollbackStack,
              invariantWrtActiveState(s)(unbound, lc),
              a,
            )
            SetProperties.subsetOfTransitivity(a.consumedBy.keySet, s.consumed, res.consumed)
            Unreachable()
          }
        }
    }

  }.ensuring(
    stateInvariant(s.visitExercise(nodeId, targetId, mbKey, byKey, consuming))(unbound, lc)
  )

  /** Invariants are preserved after a create node, given some conditions on the state and the node:
    * - The key associated to the new contract has to appear in the global keys. This is always the case since
    *   we update all the globalKeys before traversing the transaction
    * - lc has to contain contractId (since it is the set of all newly created contracts in the transaction)
    * - if unbound contains the contract then it means that no key is given
    * - The contract should not have been created previously in the transaction
    */
  @pure
  @opaque
  def stateInvariantVisitCreate(
      s: State,
      contractId: ContractId,
      mbKey: Option[GlobalKey],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))
    require(containsOptionKey(s)(mbKey))
    require(lc.contains(contractId))
    require(mbKey.isDefined ==> !unbound.contains(contractId))
    require(!s.locallyCreated.contains(contractId))

    unfold(s.visitCreate(contractId, mbKey))

    val me =
      s.copy(
        locallyCreated = s.locallyCreated + contractId,
        activeState = s.activeState
          .copy(locallyCreatedThisTimeline = s.activeState.locallyCreatedThisTimeline + contractId),
      )

    /** STEP 1: the invariants still hold for every state in the rollback stack
      *
      * At this stage all the modifications relevant to the rollbackStack has already been done.
      * In fact what remains is changing the active state. Therefore we can already prove that the
      * invariants are preserved for every state in the rollback stack (by contradiction)
      */

    if (!invariantWrtList(me)(unbound, lc)(me.rollbackStack)) {
      val a =
        ListProperties.notForallWitness(me.rollbackStack, invariantWrtActiveState(me)(unbound, lc))
      ListProperties.forallContains(s.rollbackStack, invariantWrtActiveState(s)(unbound, lc), a)
      SetProperties.subsetOfTransitivity(a.localKeys.values, s.locallyCreated, me.locallyCreated)
      Unreachable()
    }

    /** STEP 2 locallyCreated + contractId ⊆ lc
      *
      * Here we need the fact that lc contains contractId
      */

    SetProperties.subsetOfIncl(s.locallyCreated, lc, contractId)

    mbKey match {
      case None() =>
        /** STEP 3.a localKeys.values ⊆ locallyCreated + contractId
          */
        SetProperties.subsetOfTransitivity(
          s.activeState.localKeys.values,
          s.locallyCreated,
          me.locallyCreated,
        )

      case Some(gk) =>
        val ns = me.copy(activeState = me.activeState.createKey(gk, contractId))

        unfold(me.activeState.createKey(gk, contractId))
        MapProperties.updatedValues(s.activeState.localKeys, gk, contractId)

        /** STEP 3.b localKeys.updated(gk, contractId).values ⊆ locallyCreated + contractId
          */
        SetProperties.subsetOfIncl(s.activeState.localKeys.values, s.locallyCreated, contractId)
        SetProperties.subsetOfTransitivity(
          s.activeState.createKey(gk, contractId).localKeys.values,
          s.activeState.localKeys.values + contractId,
          s.locallyCreated + contractId,
        )

        /** STEP 4 unbound is disjoint from localKeys.updated(gk, contractId).values
          *
          * Here we need the fact that contractId is not in unbound if it is bound to some key
          */
        SetProperties.disjointSubsetOf(
          unbound,
          s.activeState.localKeys.values + contractId,
          s.activeState.createKey(gk, contractId).localKeys.values,
        )
        SetProperties.disjointIncl(unbound, s.activeState.localKeys.values, contractId)

        /** STEP 5 (localKeys.updated(gk, contractId).values).forall(v => localKeys.updated(gk, contractId).preimage(v).size <= BigInt(1))
          *
          * We show that the statement is true for contractId and prove by contradiction that it remains true for
          * the previous values. We will need the fact that locallyCreated and therefore localKeys.values do not
          * contain contractId.
          */
        val newF: ContractId => Boolean =
          v => ns.activeState.localKeys.preimage(v).size <= BigInt(1)

        SetProperties.forallIncl(s.activeState.localKeys.values, contractId, newF)

        // STEP 5.1 localKeys.values do not contain contractId
        if (s.activeState.localKeys.values.contains(contractId)) {
          SetProperties.subsetOfContains(
            s.activeState.localKeys.values,
            s.locallyCreated,
            contractId,
          )
        }

        // STEP 5.2 newF(contractId)
        {
          // STEP 5.2.1 localKeys.preimage(contractId).size == 0
          MapProperties.preimageIsEmpty(s.activeState.localKeys, contractId)
          SetProperties.isEmptySize(s.activeState.localKeys.preimage(contractId))

          // STEP 5.2.2 localKeys.updated(gk, contractId).preimage(contractId).size <= 1
          MapProperties.inclPreimage(s.activeState.localKeys, gk, contractId, contractId)
          SetProperties.subsetOfSize(
            s.activeState.localKeys.updated(gk, contractId).preimage(contractId),
            s.activeState.localKeys.preimage(contractId) + gk,
          )
        }

        // STEP 5.3 localKeys.values.forall(v => localKeys.updated(gk, contractId).preimage(v).size <= BigInt(1))
        if (!s.activeState.localKeys.values.forall(newF)) {
          val w = SetProperties.notForallWitness(s.activeState.localKeys.values, newF)
          SetProperties.forallContains(
            s.activeState.localKeys.values,
            v => s.activeState.localKeys.preimage(v).size <= BigInt(1),
            w,
          )
          MapProperties.inclPreimage(s.activeState.localKeys, gk, contractId, w)
          SetProperties.subsetOfSize(
            s.activeState.localKeys.updated(gk, contractId).preimage(w),
            s.activeState.localKeys.preimage(w),
          )
          Unreachable()
        }

        // STEP 5.4 Final calls
        MapProperties.updatedValues(s.activeState.localKeys, gk, contractId)
        SetProperties.forallSubsetOf(
          s.activeState.localKeys.updated(gk, contractId).values,
          s.activeState.localKeys.values + contractId,
          newF,
        )

        /** STEP 6 localKeys.keySet ⊆ globalKeys.keySet
          *
          * For this, we will need the fact that the key is already in the global keys.
          */
        unfold(containsOptionKey(s)(mbKey))
        unfold(containsKey(s)(gk))
        MapProperties.keySetContains(s.globalKeys, gk)
        SetProperties.subsetOfIncl(s.activeState.localKeys.keySet, s.globalKeys.keySet, gk)
        SetProperties.equalsSubsetOfTransitivity(
          s.activeState.localKeys.updated(gk, contractId).keySet,
          s.activeState.localKeys.keySet + gk,
          s.globalKeys.keySet,
        )
    }
  }.ensuring(stateInvariant(s.visitCreate(contractId, mbKey))(unbound, lc))

  /** If a state fulfills the invariants and a node respects the [[CSMInvariantDef.stateNodeCompatibility]] conditions
    * then the state obtained after handling it also fulfills the invariants.
    */
  @pure
  @opaque
  def handleNodeInvariant(
      s: State,
      id: NodeId,
      node: Node.Action,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))
    require(stateNodeCompatibility(s, node, unbound, lc, TraversalDirection.Down))
    require(containsActionKey(s)(node))

    unfold(s.handleNode(id, node))
    unfold(stateNodeCompatibility(s, node, unbound, lc, TraversalDirection.Down))
    unfold(containsActionKey(s)(node))

    node match {
      case create: Node.Create =>
        stateInvariantVisitCreate(s, create.coid, create.gkeyOpt, unbound, lc)
      case fetch: Node.Fetch =>
        stateInvariantAssertKeyMapping(s, fetch.coid, fetch.gkeyOpt, unbound, lc)
      case lookup: Node.LookupByKey =>
        unfold(containsOptionKey(s)(node.gkeyOpt))
        stateInvariantVisitLookup(s, lookup.gkey, lookup.result, unbound, lc)
      case exe: Node.Exercise =>
        stateInvariantVisitExercise(
          s,
          id,
          exe.targetCoid,
          exe.gkeyOpt,
          exe.byKey,
          exe.consuming,
          unbound,
          lc,
        )
    }
  }.ensuring(stateInvariant(s.handleNode(id, node))(unbound, lc))

}

object CSMInvariantDerivedProp {

  import CSMInvariantDef._

  /** If a state fulfills the invariants then the activeKeys contains a key if and only if the globalKeys also do.
    */
  @pure
  @opaque
  def invariantContainsKey(
      s: State,
      gk: GlobalKey,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))
    unfold(containsKey(s)(gk))
    MapProperties.equalsKeySetContains(s.globalKeys, s.activeKeys, gk)
  }.ensuring(containsKey(s)(gk) == s.activeKeys.contains(gk))

  /** If a state fulfills the invariants and a key maps to a contract in the global keys, then no other key maps to the
    * same contract in the global or local keys.
    */
  @pure
  @opaque
  def invariantGetGlobalKeys(
      s: State,
      k: GlobalKey,
      k2: GlobalKey,
      c: ContractId,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))
    require(s.globalKeys.get(k) == Some(KeyActive(c)))
    require(k != k2)

    /** STEP 1: globalKeys.values.contains(KeyActive(c))
      */

    unfold(s.globalKeys.contains)
    MapAxioms.valuesContains(s.globalKeys, k)

    /** STEP 2: localKeys.get(k2) != Some(c)
      */

    if (s.activeState.localKeys.get(k2) == Some(c)) {
      unfold(s.activeState.localKeys.contains)
      MapAxioms.valuesContains(s.activeState.localKeys, k2)
      SetProperties.mapContains[ContractId, KeyMapping](
        s.activeState.localKeys.values,
        KeyActive,
        s.activeState.localKeys(k2),
      )
      SetProperties.disjointContains(
        s.activeState.localKeys.values.map[KeyMapping](KeyActive),
        s.globalKeys.values,
        KeyActive(c),
      )
      Unreachable()
    }

    /** STEP 3: globalKeys.get(k2) != Some(KeyActive(c)
      */

    if (s.globalKeys.get(k2) == Some(KeyActive(c))) {

      // STEP 2.1: s.globalKeys.preimage(KeyActive(c)) contains k and k2
      MapProperties.preimageGet(s.globalKeys, KeyActive(c), k)
      MapProperties.preimageGet(s.globalKeys, KeyActive(c), k2)

      // STEP 2.2: Set(k) ++ Set(k2) subsetOf s.globalKeys.preimage(KeyActive(c))
      SetProperties.singletonSubsetOf(s.globalKeys.preimage(KeyActive(c)), k)
      SetProperties.singletonSubsetOf(s.globalKeys.preimage(KeyActive(c)), k2)
      SetProperties.unionSubsetOf(Set(k), Set(k2), s.globalKeys.preimage(KeyActive(c)))

      // STEP 2.3: Set(k) ++ Set(k2) size == 2
      SetAxioms.singletonSize(k)
      SetAxioms.singletonSize(k2)
      SetProperties.disjointTwoSingleton(k, k2)
      SetAxioms.unionDisjointSize(Set(k), Set(k2))

      // STEP 2.4: Final calls
      SetProperties.subsetOfSize(Set(k) ++ Set(k2), s.globalKeys.preimage(KeyActive(c)))
      SetProperties.filterContains(s.globalKeys.values, _.isDefined, KeyActive(c))
      SetProperties.forallContains(
        s.globalKeys.values.filter(_.isDefined),
        v => s.globalKeys.preimage(v).size <= BigInt(1),
        KeyActive(c),
      )
      Unreachable()
    }
  }.ensuring(
    (s.globalKeys.get(k2) != Some(KeyActive(c))) && (s.activeState.localKeys.get(k2) != Some(c))
  )

  /** If a state fulfills the invariants and a key maps to a contract in the local keys, then no other key maps to the
    * same contract in the global or local keys.
    */
  @pure
  @opaque
  def invariantGetLocalKeys(
      s: State,
      k: GlobalKey,
      k2: GlobalKey,
      c: ContractId,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(s)(unbound, lc))
    require(s.activeState.localKeys.get(k) == Some(c))
    require(k != k2)

    /** STEP 1: localKeys.values.contains(KeyActive(c))
      */

    unfold(s.activeState.localKeys.contains)
    MapAxioms.valuesContains(s.activeState.localKeys, k)

    /** STEP 2: globalKeys.get(k2) != Some(KeyActive(c))
      */

    if (s.globalKeys.get(k2) == Some(KeyActive(c))) {
      unfold(s.globalKeys.contains)
      MapAxioms.valuesContains(s.globalKeys, k2)
      SetProperties.mapContains[ContractId, KeyMapping](
        s.activeState.localKeys.values,
        KeyActive,
        c,
      )
      SetProperties.disjointContains(
        s.activeState.localKeys.values.map[KeyMapping](KeyActive),
        s.globalKeys.values,
        s.globalKeys(k2),
      )
      Unreachable()
    }

    /** STEP 3: localKeys.get(k2) != Some(c)
      */

    if (s.activeState.localKeys.get(k2) == Some(c)) {

      // STEP 2.1: s.activeState.localKeys.preimage(c) contains k and k2
      MapProperties.preimageGet(s.activeState.localKeys, c, k)
      MapProperties.preimageGet(s.activeState.localKeys, c, k2)

      // STEP 2.2: Set(k) ++ Set(k2) subsetOf s.activeState.localKeys.preimage(c)
      SetProperties.singletonSubsetOf(s.activeState.localKeys.preimage(c), k)
      SetProperties.singletonSubsetOf(s.activeState.localKeys.preimage(c), k2)
      SetProperties.unionSubsetOf(Set(k), Set(k2), s.activeState.localKeys.preimage(c))

      // STEP 2.3: Set(k) ++ Set(k2) size == 2
      SetAxioms.singletonSize(k)
      SetAxioms.singletonSize(k2)
      SetProperties.disjointTwoSingleton(k, k2)
      SetAxioms.unionDisjointSize(Set(k), Set(k2))

      // STEP 2.4: Final calls
      SetProperties.subsetOfSize(Set(k) ++ Set(k2), s.activeState.localKeys.preimage(c))
      SetProperties.forallContains(
        s.activeState.localKeys.values,
        v => s.activeState.localKeys.preimage(v).size <= BigInt(1),
        c,
      )
      Unreachable()
    }
  }.ensuring(
    (s.globalKeys.get(k2) != Some(KeyActive(c))) && (s.activeState.localKeys.get(k2) != Some(c))
  )

  /** The invariants are true for any state whose activeState is empty.
    */
  @pure
  @opaque
  def emptyActiveStateInvariant(s: State, unbound: Set[ContractId], lc: Set[ContractId]): Unit = {
    unfold(ActiveLedgerState.empty)
    unfold(invariantWrtActiveState(s)(unbound, lc)(ActiveLedgerState.empty))

    MapProperties.emptyValues[GlobalKey, ContractId]
    SetProperties.isEmptySubsetOf(ActiveLedgerState.empty.localKeys.values, s.locallyCreated)
    SetProperties.disjointIsEmpty(unbound, ActiveLedgerState.empty.localKeys.values)
    SetProperties.forallIsEmpty(
      ActiveLedgerState.empty.localKeys.values,
      v => ActiveLedgerState.empty.localKeys.preimage(v).size <= BigInt(1),
    )

    MapProperties.emptyKeySet[GlobalKey, ContractId]
    MapProperties.emptyKeySet[ContractId, NodeId]
    SetProperties.isEmptySubsetOf(ActiveLedgerState.empty.consumedBy.keySet, s.consumed)
    SetProperties.isEmptySubsetOf(ActiveLedgerState.empty.localKeys.keySet, s.globalKeys.keySet)

  }.ensuring(
    invariantWrtActiveState(s)(unbound, lc)(ActiveLedgerState.empty)
  )

  /** The list invariants are always true for the empty state.
    */
  @pure
  @opaque
  def emptyListInvariant(s: State, unbound: Set[ContractId], lc: Set[ContractId]): Unit = {
    unfold(State.empty)
    unfold(invariantWrtList(State.empty)(unbound, lc)(State.empty.rollbackStack))
  }.ensuring(
    invariantWrtList(State.empty)(unbound, lc)(State.empty.rollbackStack)
  )

}
