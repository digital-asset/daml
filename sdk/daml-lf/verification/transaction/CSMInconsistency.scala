// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import CSMEitherDef._
import CSMKeysPropertiesDef._
import CSMKeysProperties._

/** File stating under which conditions a state is well defined after handling a node and how its active keys behave
  * after that.
  */
object CSMInconsistencyDef {

  /** *
    * Alternative and general definition of an error in a transaction
    *
    * The objective of bringing this definition in are proving that:
    * - s.handleNode(nid, node) leads to an error <=> inconsistencyCheck(s, node.gkeyOpt, node.m) is true
    * - If node.gkeyOpt =/= Some(k2) then
    * inconsistencyCheck(s, k2, m) == inconsistencyCheck(s.handleNode(nid, node), k2, m)
    *
    * @param s
    * The current state
    * @param k
    * The key of the node we are handling. If the key is not defined then the transaction never fails, so
    * the inconsistencyCheck is false
    * @param m
    * The key mapping that would have been added to the globalKeys if there was not an entry already for that key.
    * This mapping (let denote it node.m) is:
    * - KeyInactive for Node.Create
    * - KeyActive(coid) for Node.Fetch
    * - result for Node.Lookup
    * - KeyActive(targetCoid) for Node.Exercise
    */
  @pure
  def inconsistencyCheck(s: State, k: GlobalKey, m: KeyMapping): Boolean = {
    s.activeKeys.get(k).exists(_ != m)
  }

  @pure
  def inconsistencyCheck(s: State, k: Option[GlobalKey], m: KeyMapping): Boolean = {
    k.exists(gk => inconsistencyCheck(s, gk, m))
  }

}

object CSMInconsistency {

  import CSMInconsistencyDef._
  import CSMInvariantDef._
  import CSMInvariantDerivedProp._

  /** The resulting state after handling a Create node is defined if any only if the inconsistencyCondition is not met
    */
  @pure
  @opaque
  def visitCreateUndefined(s: State, contractId: ContractId, mbKey: Option[GlobalKey]): Unit = {
    unfold(inconsistencyCheck(s, mbKey, KeyInactive))
    unfold(s.visitCreate(contractId, mbKey))

    mbKey match {
      case None() => Trivial()
      case Some(gk) => unfold(inconsistencyCheck(s, gk, KeyInactive))
    }

  }.ensuring(
    (s.visitCreate(contractId, mbKey).isLeft == inconsistencyCheck(s, mbKey, KeyInactive))
  )

  /** The resulting state after handling a Fetch node is defined if any only if the inconsistencyCondition is not met
    */
  @pure
  @opaque
  def assertKeyMappingUndefined(s: State, cid: ContractId, gkey: Option[GlobalKey]): Unit = {
    require(containsOptionKey(s)(gkey))

    unfold(containsOptionKey(s)(gkey))
    unfold(inconsistencyCheck(s, gkey, KeyActive(cid)))
    unfold(s.assertKeyMapping(cid, gkey))

    gkey match {
      case None() => Trivial()
      case Some(gk) => visitLookupUndefined(s, gk, KeyActive(cid))
    }

  }.ensuring(s.assertKeyMapping(cid, gkey).isLeft == inconsistencyCheck(s, gkey, KeyActive(cid)))

  /** The resulting state after handling an Exercise node is defined if any only if the inconsistencyCondition is not met
    */
  @pure
  @opaque
  def visitExerciseUndefined(
      s: State,
      nodeId: NodeId,
      targetId: ContractId,
      gk: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
  ): Unit = {
    require(containsOptionKey(s)(gk))
    unfold(s.visitExercise(nodeId, targetId, gk, byKey, consuming))
    assertKeyMappingUndefined(s, targetId, gk)
  }.ensuring(
    s.visitExercise(nodeId, targetId, gk, byKey, consuming).isLeft == inconsistencyCheck(
      s,
      gk,
      KeyActive(targetId),
    )
  )

  /** The resulting state after handling a Lookup node is defined if any only if the inconsistencyCondition is not met
    */
  @pure
  @opaque
  def visitLookupUndefined(s: State, gk: GlobalKey, keyResolution: Option[ContractId]): Unit = {
    require(containsKey(s)(gk))

    unfold(inconsistencyCheck(s, gk, keyResolution))
    unfold(s.visitLookup(gk, keyResolution))
    unfold(containsKey(s)(gk))
    unfold(s.globalKeys.contains)

    activeKeysGetOrElse(s, gk, KeyInactive)
    activeKeysGet(s, gk)

  }.ensuring(s.visitLookup(gk, keyResolution).isLeft == inconsistencyCheck(s, gk, keyResolution))

  /** The resulting state after handling a node is defined if any only if the inconsistencyCondition is not met
    */
  @pure
  @opaque
  def handleNodeUndefined(s: State, id: NodeId, node: Node.Action): Unit = {
    require(containsActionKey(s)(node))

    unfold(s.handleNode(id, node))
    unfold(nodeActionKeyMapping(node))
    unfold(containsActionKey(s)(node))

    node match {
      case create: Node.Create => visitCreateUndefined(s, create.coid, create.gkeyOpt)
      case fetch: Node.Fetch => assertKeyMappingUndefined(s, fetch.coid, fetch.gkeyOpt)
      case lookup: Node.LookupByKey =>
        unfold(containsOptionKey(s)(node.gkeyOpt))
        unfold(inconsistencyCheck(s, node.gkeyOpt, nodeActionKeyMapping(node)))
        visitLookupUndefined(s, lookup.gkey, lookup.result)
      case exe: Node.Exercise =>
        visitExerciseUndefined(s, id, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming)
    }
  }.ensuring(
    s.handleNode(id, node).isLeft == inconsistencyCheck(s, node.gkeyOpt, nodeActionKeyMapping(node))
  )

  /** If two states are defined after handling a node then their activeKeys shared the same entry for the node's key
    * before entering the node.
    */
  @pure
  @opaque
  def handleSameNodeActiveKeys(s1: State, s2: State, id: NodeId, node: Node.Action): Unit = {
    require(containsActionKey(s1)(node))
    require(containsActionKey(s2)(node))
    require(s1.handleNode(id, node).isRight)
    require(s2.handleNode(id, node).isRight)
    activeKeysContainsKey(s1, node)
    activeKeysContainsKey(s2, node)
    handleNodeUndefined(s1, id, node)
    handleNodeUndefined(s2, id, node)
    unfold(s1.activeKeys.contains)
    unfold(s2.activeKeys.contains)
  }.ensuring(node.gkeyOpt.forall(k => s1.activeKeys.get(k) == s2.activeKeys.get(k)))

  /** If a state stay well defined after handling a Create node and if we are given a key different from the node's one,
    * then the entry for that key in the activeKeys of the state did not change
    */
  @opaque
  @pure
  def visitCreateActiveKeysGet(
      s: State,
      contractId: ContractId,
      mbKey: Option[GlobalKey],
      k2: GlobalKey,
  ): Unit = {
    require(s.visitCreate(contractId, mbKey).isRight)
    require(mbKey.forall(k1 => k1 != k2))
    unfold(s.visitCreate(contractId, mbKey))

    activeKeysGet(s, k2)

    val me =
      s.copy(
        locallyCreated = s.locallyCreated + contractId,
        activeState = s.activeState
          .copy(locallyCreatedThisTimeline = s.activeState.locallyCreatedThisTimeline + contractId),
      )

    mbKey match {
      case None() =>
        activeKeysGet(me, k2)
      case Some(gk) =>
        activeKeysGet(me.copy(activeState = me.activeState.createKey(gk, contractId)), k2)
        MapProperties.updatedGet(me.activeState.localKeys, gk, contractId, k2)
    }

  }.ensuring(s.visitCreate(contractId, mbKey).get.activeKeys.get(k2) == s.activeKeys.get(k2))

  /** If a state stay well defined after handling a Lookup node and if we are given a key different from the node's one,
    * then the entry for that key in the activeKeys of the state did not change
    */
  @opaque
  @pure
  def visitLookupActiveKeysGet(
      s: State,
      gk: GlobalKey,
      keyResolution: Option[ContractId],
      k2: GlobalKey,
  ): Unit = {
    require(s.visitLookup(gk, keyResolution).isRight)
    unfold(sameState(s, s.visitLookup(gk, keyResolution)))
  }.ensuring(s.visitLookup(gk, keyResolution).get.activeKeys.get(k2) == s.activeKeys.get(k2))

  /** If a state stay well defined after handling a Fetch node and if we are given a key different from the node's one,
    * then the entry for that key in the activeKeys of the state did not change
    */
  @opaque
  @pure
  def assertKeyMappingActiveKeysGet(
      s: State,
      cid: ContractId,
      mbKey: Option[GlobalKey],
      k2: GlobalKey,
  ): Unit = {
    require(s.assertKeyMapping(cid, mbKey).isRight)
    unfold(sameState(s, s.assertKeyMapping(cid, mbKey)))
  }.ensuring(s.assertKeyMapping(cid, mbKey).get.activeKeys.get(k2) == s.activeKeys.get(k2))

  /** If a state stay well defined after handling an Exercise node and if we are given a key different from the node's one,
    * then the entry for that key in the activeKeys of the state did not change
    */
  @pure
  @opaque
  def visitExerciseActiveKeysGet(
      s: State,
      nodeId: NodeId,
      targetId: ContractId,
      gk: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
      k2: GlobalKey,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(s.visitExercise(nodeId, targetId, gk, byKey, consuming).isRight)
    require(containsOptionKey(s)(gk))
    require(!gk.isDefined ==> unbound.contains(targetId))
    require(gk.forall(k1 => k1 != k2))
    require(stateInvariant(s)(unbound, lc))

    unfold(s.visitExercise(nodeId, targetId, gk, byKey, consuming))

    visitExerciseUndefined(s, nodeId, targetId, gk, byKey, consuming)
    unfold(inconsistencyCheck(s, gk, KeyActive(targetId)))
    unfold(containsOptionKey(s)(gk))

    s.assertKeyMapping(targetId, gk) match {
      case Left(_) => Trivial()
      case Right(state) =>
        keysGet(s, k2)
        unfold(s.globalKeys.contains)
        unfold(s.activeState.localKeys.contains)

        gk match {
          case Some(k) =>
            // (s.globalKeys.get(k) == Some(KeyActive(targetId))) || (s.activeState.localKeys.get(k).map(KeyActive) == Some(KeyActive(targetId)))
            activeKeysGet(s, k)
            unfold(containsKey(s)(k))
            unfold(keyMappingToActiveMapping(s.activeState.consumedBy))
            if (s.globalKeys.get(k) == Some(KeyActive(targetId))) {
              invariantGetGlobalKeys(s, k, k2, targetId, unbound, lc)
            } else {
              invariantGetLocalKeys(s, k, k2, targetId, unbound, lc)
            }
          case None() =>
            SetProperties.mapContains(unbound, KeyActive, targetId)
            SetProperties.disjointContains(
              unbound.map(KeyActive),
              s.globalKeys.values,
              KeyActive(targetId),
            )
            if (s.globalKeys.get(k2) == Some(KeyActive(targetId))) {
              MapAxioms.valuesContains(s.globalKeys, k2)
            }

            SetProperties.disjointContains(unbound, s.activeState.localKeys.values, targetId)
            if (s.activeState.localKeys.get(k2) == Some(targetId)) {
              MapAxioms.valuesContains(s.activeState.localKeys, k2)
            }
        }

        unfold(sameState(s, s.assertKeyMapping(targetId, gk)))
        unfold(state.consume(targetId, nodeId))
        unfold(state.activeState.consume(targetId, nodeId))
        activeKeysGet(state.consume(targetId, nodeId), k2)
        activeKeysGet(state, k2)
        unfold(keyMappingToActiveMapping(state.activeState.consumedBy))
        unfold(keyMappingToActiveMapping(state.activeState.consumedBy.updated(targetId, nodeId)))

        (s.activeState.localKeys.get(k2).map(KeyActive), s.globalKeys.get(k2)) match {
          case (Some(Some(c1)), _) =>
            MapProperties.updatedContains(state.activeState.consumedBy, targetId, nodeId, c1)
          case (_, Some(Some(c2))) =>
            MapProperties.updatedContains(state.activeState.consumedBy, targetId, nodeId, c2)
          case _ => Trivial()
        }
    }

  }.ensuring(
    s.visitExercise(nodeId, targetId, gk, byKey, consuming).get.activeKeys.get(k2) == s.activeKeys
      .get(k2)
  )

  /** If a state stay well defined after handling a node and if we are given a key different from the node's one,
    * then the entry for that key in the activeKeys of the state did not change
    */
  @pure
  @opaque
  def handleNodeActiveKeysGet(
      s: State,
      id: NodeId,
      node: Node.Action,
      k2: GlobalKey,
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(s.handleNode(id, node).isRight)
    require(containsActionKey(s)(node))
    require(node.gkeyOpt.forall(k1 => k1 != k2))
    require(stateInvariant(s)(unbound, lc))
    require(stateNodeCompatibility(s, node, unbound, lc, TraversalDirection.Down))

    unfold(s.handleNode(id, node))
    unfold(containsActionKey(s)(node))

    node match {
      case create: Node.Create => visitCreateActiveKeysGet(s, create.coid, create.gkeyOpt, k2)
      case fetch: Node.Fetch => assertKeyMappingActiveKeysGet(s, fetch.coid, fetch.gkeyOpt, k2)
      case lookup: Node.LookupByKey => visitLookupActiveKeysGet(s, lookup.gkey, lookup.result, k2)
      case exe: Node.Exercise =>
        unfold(stateNodeCompatibility(s, node, unbound, lc, TraversalDirection.Down))
        visitExerciseActiveKeysGet(
          s,
          id,
          exe.targetCoid,
          exe.gkeyOpt,
          exe.byKey,
          exe.consuming,
          k2,
          unbound,
          lc,
        )
    }

  }.ensuring(s.handleNode(id, node).get.activeKeys.get(k2) == s.activeKeys.get(k2))

  /** The entry for a given key in the activeKeys of the state does not change after entering a beginRollback
    */
  @pure
  @opaque
  def beginRollbackActiveKeysGet(s: State, k: GlobalKey): Unit = {
    unfold(s.beginRollback())
    activeKeysGetSameFields(s.beginRollback(), s, k)
  }.ensuring(s.beginRollback().activeKeys.get(k) == s.activeKeys.get(k))

  /** If two states are defined after handling a Create node then they share the same mapping for the node's key entry in
    * the activeKeys.
    */
  @opaque
  @pure
  def visitCreateDifferentStatesActiveKeysGet(
      s1: State,
      s2: State,
      contractId: ContractId,
      mbKey: Option[GlobalKey],
  ): Unit = {
    require(s1.visitCreate(contractId, mbKey).isRight)
    require(s2.visitCreate(contractId, mbKey).isRight)
    require(!s1.activeState.consumedBy.contains(contractId))
    require(!s2.activeState.consumedBy.contains(contractId))

    unfold(s1.visitCreate(contractId, mbKey))
    unfold(s2.visitCreate(contractId, mbKey))

    val me1 =
      s1.copy(
        locallyCreated = s1.locallyCreated + contractId,
        activeState = s1.activeState
          .copy(locallyCreatedThisTimeline = s1.activeState.locallyCreatedThisTimeline + contractId),
      )

    val me2 =
      s2.copy(
        locallyCreated = s2.locallyCreated + contractId,
        activeState = s2.activeState
          .copy(locallyCreatedThisTimeline = s2.activeState.locallyCreatedThisTimeline + contractId),
      )

    mbKey match {
      case None() => Trivial()
      case Some(gk) =>
        val sf1 = me1.copy(activeState = me1.activeState.createKey(gk, contractId))
        val sf2 = me2.copy(activeState = me2.activeState.createKey(gk, contractId))
        activeKeysGet(sf1, gk)
        activeKeysGet(sf2, gk)
        unfold(me1.activeState.createKey(gk, contractId))
        unfold(me2.activeState.createKey(gk, contractId))
        unfold(keyMappingToActiveMapping(s1.activeState.consumedBy))
        unfold(keyMappingToActiveMapping(s2.activeState.consumedBy))
    }
  }.ensuring(
    mbKey.forall(k =>
      s1.visitCreate(contractId, mbKey).get.activeKeys.get(k) == s2
        .visitCreate(contractId, mbKey)
        .get
        .activeKeys
        .get(k)
    )
  )

  /** If two states are defined after handling a Lookup node then they share the same mapping for the node's key entry in
    * the activeKeys.
    */
  @opaque
  @pure
  def visitLookupDifferentStatesActiveKeysGet(
      s1: State,
      s2: State,
      gk: GlobalKey,
      keyResolution: Option[ContractId],
      unbound1: Set[ContractId],
      lc1: Set[ContractId],
      unbound2: Set[ContractId],
      lc2: Set[ContractId],
  ): Unit = {
    require(s1.visitLookup(gk, keyResolution).isRight)
    require(s2.visitLookup(gk, keyResolution).isRight)
    require(
      keyResolution.isDefined || (containsKey(s1)(gk) && containsKey(s2)(gk) && stateInvariant(s1)(
        unbound1,
        lc1,
      ) && stateInvariant(s2)(unbound2, lc2))
    )

    unfold(s1.visitLookup(gk, keyResolution))
    unfold(s2.visitLookup(gk, keyResolution))
    unfold(s1.activeKeys.getOrElse(gk, KeyInactive))
    unfold(s2.activeKeys.getOrElse(gk, KeyInactive))

    keyResolution match {
      case None() =>
        invariantContainsKey(s1, gk, unbound1, lc1)
        invariantContainsKey(s2, gk, unbound2, lc2)
        unfold(s1.activeKeys.contains)
        unfold(s2.activeKeys.contains)
      case Some(_) => Trivial()
    }

  }.ensuring(
    (s1.visitLookup(gk, keyResolution).get.activeKeys.get(gk) == s2
      .visitLookup(gk, keyResolution)
      .get
      .activeKeys
      .get(gk)) &&
      (s1.visitLookup(gk, keyResolution).get.activeKeys.get(gk) == Some(keyResolution))
  )

  /** If two states are defined after handling a Fetch node then they share the same mapping for the node's key entry in
    * the activeKeys.
    */
  @opaque
  @pure
  def assertKeyMappingDifferentStatesActiveKeysGet(
      s1: State,
      s2: State,
      cid: ContractId,
      mbKey: Option[GlobalKey],
  ): Unit = {
    require(s1.assertKeyMapping(cid, mbKey).isRight)
    require(s2.assertKeyMapping(cid, mbKey).isRight)

    unfold(s1.assertKeyMapping(cid, mbKey))
    unfold(s2.assertKeyMapping(cid, mbKey))
    mbKey match {
      case None() => Trivial()
      case Some(k) =>
        visitLookupDifferentStatesActiveKeysGet(
          s1,
          s2,
          k,
          Some(cid),
          Set.empty[ContractId],
          Set.empty[ContractId],
          Set.empty[ContractId],
          Set.empty[ContractId],
        )
    }

  }.ensuring(
    mbKey.forall(gk =>
      (s1.assertKeyMapping(cid, mbKey).get.activeKeys.get(gk) == s2
        .assertKeyMapping(cid, mbKey)
        .get
        .activeKeys
        .get(gk)) &&
        (s1.assertKeyMapping(cid, mbKey).get.activeKeys.get(gk) == Some(KeyActive(cid)))
    )
  )

  /** If two states are defined after handling an Exercise node then they share the same mapping for the node's key entry in
    * the activeKeys.
    */
  @opaque
  @pure
  def visitExerciseDifferentStatesActiveKeysGet(
      s1: State,
      s2: State,
      nodeId: NodeId,
      targetId: ContractId,
      mbKey: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
  ): Unit = {
    require(s1.visitExercise(nodeId, targetId, mbKey, byKey, consuming).isRight)
    require(s2.visitExercise(nodeId, targetId, mbKey, byKey, consuming).isRight)
    unfold(s1.visitExercise(nodeId, targetId, mbKey, byKey, consuming))
    unfold(s2.visitExercise(nodeId, targetId, mbKey, byKey, consuming))

    assertKeyMappingDifferentStatesActiveKeysGet(s1, s2, targetId, mbKey)

    (s1.assertKeyMapping(targetId, mbKey), s2.assertKeyMapping(targetId, mbKey), mbKey) match {
      case (Left(_), _, _) => Unreachable()
      case (_, Left(_), _) => Unreachable()
      case (Right(state1), Right(state2), Some(k)) =>
        unfold(sameState(s1, s1.assertKeyMapping(targetId, mbKey)))
        unfold(sameState(s2, s2.assertKeyMapping(targetId, mbKey)))
        unfold(state1.consume(targetId, nodeId))
        unfold(state1.activeState.consume(targetId, nodeId))
        unfold(state2.consume(targetId, nodeId))
        unfold(state2.activeState.consume(targetId, nodeId))

        activeKeysGet(state1, k)
        activeKeysGet(state2, k)
        unfold(keyMappingToActiveMapping(state1.activeState.consumedBy))
        unfold(keyMappingToActiveMapping(state2.activeState.consumedBy))

        activeKeysGet(state1.consume(targetId, nodeId), k)
        activeKeysGet(state2.consume(targetId, nodeId), k)
        unfold(keyMappingToActiveMapping(state1.activeState.consumedBy.updated(targetId, nodeId)))
        unfold(keyMappingToActiveMapping(state2.activeState.consumedBy.updated(targetId, nodeId)))

      case _ => Trivial()
    }

  }.ensuring(
    mbKey.forall(k =>
      s1.visitExercise(nodeId, targetId, mbKey, byKey, consuming).get.activeKeys.get(k) ==
        s2.visitExercise(nodeId, targetId, mbKey, byKey, consuming).get.activeKeys.get(k)
    )
  )

  /** If two states are defined after handling a node then they share the same mapping for the node's key entry in
    * the activeKeys.
    */
  @opaque
  @pure
  def handleNodeDifferentStatesActiveKeysGet(
      s1: State,
      s2: State,
      nid: NodeId,
      node: Node.Action,
      unbound1: Set[ContractId],
      lc1: Set[ContractId],
      unbound2: Set[ContractId],
      lc2: Set[ContractId],
  ): Unit = {
    require(s1.handleNode(nid, node).isRight)
    require(s2.handleNode(nid, node).isRight)
    require(containsActionKey(s1)(node))
    require(containsActionKey(s2)(node))
    require(stateInvariant(s1)(unbound1, lc1))
    require(stateInvariant(s2)(unbound2, lc2))
    require(stateNodeCompatibility(s1, node, unbound1, lc1, TraversalDirection.Down))
    require(stateNodeCompatibility(s2, node, unbound2, lc2, TraversalDirection.Down))

    unfold(s1.handleNode(nid, node))
    unfold(s2.handleNode(nid, node))
    unfold(containsActionKey(s1)(node))
    unfold(containsActionKey(s2)(node))
    unfold(stateNodeCompatibility(s1, node, unbound1, lc1, TraversalDirection.Down))
    unfold(stateNodeCompatibility(s2, node, unbound2, lc2, TraversalDirection.Down))

    node match {
      case create: Node.Create =>
        if (s1.activeState.consumedBy.contains(create.coid)) {
          MapProperties.keySetContains(s1.activeState.consumedBy, create.coid)
          SetProperties.subsetOfContains(s1.activeState.consumedBy.keySet, s1.consumed, create.coid)
          Unreachable()
        }
        if (s2.activeState.consumedBy.contains(create.coid)) {
          MapProperties.keySetContains(s2.activeState.consumedBy, create.coid)
          SetProperties.subsetOfContains(s2.activeState.consumedBy.keySet, s2.consumed, create.coid)
          Unreachable()
        }
        visitCreateDifferentStatesActiveKeysGet(s1, s2, create.coid, create.gkeyOpt)
      case fetch: Node.Fetch =>
        assertKeyMappingDifferentStatesActiveKeysGet(s1, s2, fetch.coid, fetch.gkeyOpt)
      case lookup: Node.LookupByKey =>
        unfold(containsOptionKey(s1)(node.gkeyOpt))
        unfold(containsOptionKey(s2)(node.gkeyOpt))
        visitLookupDifferentStatesActiveKeysGet(
          s1,
          s2,
          lookup.gkey,
          lookup.result,
          unbound1,
          lc1,
          unbound2,
          lc2,
        )
      case exe: Node.Exercise =>
        visitExerciseDifferentStatesActiveKeysGet(
          s1,
          s2,
          nid,
          exe.targetCoid,
          exe.gkeyOpt,
          exe.byKey,
          exe.consuming,
        )
    }

  }.ensuring(
    node.gkeyOpt.forall(k =>
      s1.handleNode(nid, node).get.activeKeys.get(k) ==
        s2.handleNode(nid, node).get.activeKeys.get(k)
    )
  )

  /** If two states have the same activeState and the same globalKeys then their activeKeys are also the same.
    */
  @pure @opaque
  def activeKeysGetSameFields(s1: State, s2: State, k: GlobalKey): Unit = {
    require(s1.activeState == s2.activeState)
    require(s1.globalKeys == s2.globalKeys)
    unfold(s1.activeKeys)
    unfold(s2.activeKeys)
    unfold(s1.keys)
    unfold(s2.keys)
  }.ensuring(s1.activeKeys.get(k) == s2.activeKeys.get(k))

  /** beginRollback followed by a function that does not modify the rollbackStack and the globalKeys, followed by an
    * endRollback does not change the activeKeys of a state.
    */
  @opaque
  @pure
  def activeKeysGetRollbackScope(s: State, k: GlobalKey, f: State => State): Unit = {
    require(f(s.beginRollback()).endRollback().isRight)
    require(f(s.beginRollback()).rollbackStack == s.beginRollback().rollbackStack)
    require(f(s.beginRollback()).globalKeys == s.beginRollback().globalKeys)

    unfold(s.beginRollback())
    unfold(f(s.beginRollback()).endRollback())
    activeKeysGetSameFields(s, f(s.beginRollback()).endRollback().get, k)
  }.ensuring(
    f(s.beginRollback()).endRollback().get.activeKeys.get(k) ==
      s.activeKeys.get(k)
  )

}
