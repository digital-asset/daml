// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package translation

import stainless.lang._
import stainless.annotation._
import stainless.collection._
import utils.{
  Either,
  Map,
  Set,
  Value,
  GlobalKey,
  Transaction,
  Unreachable,
  Node,
  ContractKeyUniquenessMode,
  MapProperties,
  MapAxioms,
  NodeId,
  SetAxioms,
  SetProperties,
  Trivial,
}
import utils.Value.ContractId
import utils.Transaction.{KeyCreate, NegativeKeyLookup, KeyInput}
import utils.TransactionErrors.{
  KeyInputError,
  InconsistentContractKey,
  InconsistentContractKeyKIError,
  DuplicateContractKey,
}

import transaction.ContractStateMachine._
import transaction.CSMHelpers._
import transaction.CSMKeysPropertiesDef._
import transaction.CSMKeysProperties._
import transaction.CSMInvariantDerivedProp._
import transaction.CSMInvariantDef._
import transaction.State

import ContractStateMachine.{State => StateOriginal, ActiveLedgerState => ActiveLedgerStateOriginal}

object CSMConversion {
  @pure
  def toKeyMapping: KeyInput => KeyMapping = _.toKeyMapping
  @pure
  def toKeyInput: KeyMapping => KeyInput = {
    case None() => NegativeKeyLookup
    case Some(cid) => Transaction.KeyActive(cid)
  }
  @pure @opaque
  def globalKeys(gki: Map[GlobalKey, KeyInput]): Map[GlobalKey, Option[ContractId]] = {
    MapProperties.mapValuesKeySet(gki, toKeyMapping)
    gki.mapValues(toKeyMapping)
  }.ensuring(_.keySet === gki.keySet)

  @pure
  @opaque
  def globalKeyInputs(gki: Map[GlobalKey, KeyMapping]): Map[GlobalKey, KeyInput] = {
    MapProperties.mapValuesKeySet(gki, toKeyInput)
    gki.mapValues(toKeyInput)
  }.ensuring(_.keySet === gki.keySet)

  @pure @opaque
  def globalKeysGlobalKeyInputs(gki: Map[GlobalKey, KeyMapping]): Unit = {
    unfold(globalKeyInputs(gki))
    unfold(globalKeys(globalKeyInputs(gki)))
    MapProperties.mapValuesAndThen(gki, toKeyInput, toKeyMapping)
    if (gki =/= gki.mapValues(toKeyInput andThen toKeyMapping)) {
      val k = MapProperties.notEqualsWitness(gki, gki.mapValues(toKeyInput andThen toKeyMapping))
      MapProperties.mapValuesGet(gki, toKeyInput andThen toKeyMapping, k)
    }
    MapProperties.equalsTransitivity(
      gki,
      gki.mapValues(toKeyInput andThen toKeyMapping),
      globalKeys(globalKeyInputs(gki)),
    )
    MapAxioms.extensionality(gki, globalKeys(globalKeyInputs(gki)))
  }.ensuring(globalKeys(globalKeyInputs(gki)) == gki)

  @pure
  def toOriginal(s: ActiveLedgerState): ActiveLedgerStateOriginal[NodeId] = {
    ActiveLedgerStateOriginal[NodeId](s.locallyCreatedThisTimeline, s.consumedBy, s.localKeys)
  }
  @pure
  def toAlt(s: ActiveLedgerStateOriginal[NodeId]): ActiveLedgerState = {
    ActiveLedgerState(s.locallyCreatedThisTimeline, s.consumedBy, s.localKeys)
  }

  @pure @opaque
  def altOriginalInverse(s: ActiveLedgerState): Unit = {
    unfold(toOriginal(s))
    unfold(toAlt(toOriginal(s)))
  }.ensuring(toAlt(toOriginal(s)) == s)

  @pure
  @opaque
  def originalAltInverse(s: ActiveLedgerStateOriginal[NodeId]): Unit = {
    unfold(toAlt(s))
    unfold(toOriginal(toAlt(s)))
  }.ensuring(toOriginal(toAlt(s)) == s)

  @pure
  def toOriginal(s: State): StateOriginal[NodeId] = {
    StateOriginal[NodeId](
      s.locallyCreated,
      s.inputContractIds,
      globalKeyInputs(s.globalKeys),
      toOriginal(s.activeState),
      s.rollbackStack.map(toOriginal),
      ContractKeyUniquenessMode.Strict,
    )
  }
  @pure
  def toAlt(consumed: Set[ContractId])(s: StateOriginal[NodeId]): State = {
    State(
      s.locallyCreated,
      s.inputContractIds,
      consumed,
      globalKeys(s.globalKeyInputs),
      toAlt(s.activeState),
      s.rollbackStack.map(toAlt),
    )
  }

  @pure
  def toAlt[T](consumed: Set[ContractId])(e: Either[T, StateOriginal[NodeId]]): Either[T, State] = {
    e.map(toAlt(consumed))
  }

  @pure
  def toOriginal[T](e: Either[T, State]): Either[T, StateOriginal[NodeId]] = {
    e.map(toOriginal)
  }

  @pure
  @opaque
  def lookupActiveGlobalKeyInputToAlt(
      s: StateOriginal[NodeId],
      key: GlobalKey,
      consumed: Set[ContractId],
  ) = {
    unfold(toAlt(consumed)(s))
    unfold(toAlt(s.activeState))
    unfold(keyMappingToActiveMapping(toAlt(s.activeState).consumedBy))
    unfold(globalKeys(s.globalKeyInputs))
    MapProperties.mapValuesGet(s.globalKeyInputs, toKeyMapping, key)
    MapProperties.mapValuesGet(
      toAlt(consumed)(s).globalKeys,
      keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy),
      key,
    )

  }.ensuring(
    s.lookupActiveGlobalKeyInput(key) == (toAlt(consumed)(s)).globalKeys
      .mapValues(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy))
      .get(key)
  )

  @pure
  @opaque
  def getLocalActiveKeyToAlt(
      s: ActiveLedgerStateOriginal[NodeId],
      key: GlobalKey,
      consumed: Set[ContractId],
  ): Unit = {
    unfold(toAlt(s))
    unfold(keyMappingToActiveMapping(toAlt(s).consumedBy))
    MapProperties.mapValuesGet(
      s.localKeys,
      (v: ContractId) => if (s.consumedBy.contains(v)) KeyInactive else KeyActive(v),
      key,
    )
    MapProperties.mapValuesGet(
      toAlt(s).localKeys.mapValues(KeyActive),
      keyMappingToActiveMapping(toAlt(s).consumedBy),
      key,
    )
    MapProperties.mapValuesGet(toAlt(s).localKeys, KeyActive, key)
  }.ensuring(
    s.getLocalActiveKey(key) == toAlt(s).localKeys
      .mapValues(KeyActive)
      .mapValues(keyMappingToActiveMapping(toAlt(s).consumedBy))
      .get(key)
  )

  @pure
  @opaque
  def lookupActiveKeyToAlt(
      s: StateOriginal[NodeId],
      key: GlobalKey,
      consumed: Set[ContractId],
  ): Unit = {
    unfold(toAlt(consumed)(s).activeKeys)
    unfold(toAlt(consumed)(s).keys)
    MapProperties.mapValuesConcat(
      toAlt(consumed)(s).globalKeys,
      toAlt(consumed)(s).activeState.localKeys.mapValues(KeyActive),
      keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy),
    )
    MapProperties.equalsGet(
      toAlt(consumed)(s).activeKeys,
      toAlt(consumed)(s).globalKeys
        .mapValues(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy)) ++
        toAlt(consumed)(s).activeState.localKeys
          .mapValues(KeyActive)
          .mapValues(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy)),
      key,
    )
    MapAxioms.concatGet(
      toAlt(consumed)(s).globalKeys
        .mapValues(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy)),
      toAlt(consumed)(s).activeState.localKeys
        .mapValues(KeyActive)
        .mapValues(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy)),
      key,
    )
    getLocalActiveKeyToAlt(s.activeState, key, consumed)
    lookupActiveGlobalKeyInputToAlt(s, key, consumed)
  }.ensuring(
    s.lookupActiveKey(key) == toAlt(consumed)(s).activeKeys.get(key)
  )

  @pure
  @opaque
  def consumeToAlt(
      s: StateOriginal[NodeId],
      cid: ContractId,
      nid: NodeId,
      consumed: Set[ContractId],
  ): Unit = {
    unfold(toAlt(consumed)(s).consume(cid, nid))
  }.ensuring(
    toAlt(consumed + cid)(s.copy(activeState = s.activeState.consume(cid, nid))) == toAlt(consumed)(
      s
    ).consume(cid, nid)
  )

//
  @pure @opaque
  def globalKeyInputsContainsToAlt(
      s: StateOriginal[NodeId],
      gk: GlobalKey,
      consumed: Set[ContractId],
  ): Unit = {
    unfold(containsKey(toAlt(consumed)(s))(gk))
    MapProperties.equalsKeySetContains(s.globalKeyInputs, globalKeys(s.globalKeyInputs), gk)
  }.ensuring(s.globalKeyInputs.contains(gk) == containsKey(toAlt(consumed)(s))(gk))

  @pure
  @opaque
  def globalKeyInputsUpdatedToAlt(
      s: StateOriginal[NodeId],
      gk: GlobalKey,
      keyInput: KeyInput,
      consumed: Set[ContractId],
  ): Unit = {
    unfold(globalKeys(s.globalKeyInputs))
    unfold(globalKeys(s.globalKeyInputs.updated(gk, keyInput)))
    MapProperties.mapValuesUpdated(s.globalKeyInputs, gk, keyInput, toKeyMapping)
    MapAxioms.extensionality(
      s.globalKeyInputs.updated(gk, keyInput).mapValues(toKeyMapping),
      s.globalKeyInputs.mapValues(toKeyMapping).updated(gk, keyInput.toKeyMapping),
    )
  }.ensuring(
    globalKeys(s.globalKeyInputs.updated(gk, keyInput)) == globalKeys(s.globalKeyInputs)
      .updated(gk, keyInput.toKeyMapping)
  )

  @pure
  @opaque
  def visitCreateToAlt(
      s: StateOriginal[NodeId],
      contractId: ContractId,
      mbKey: Option[GlobalKey],
      consumed: Set[ContractId],
  ): Unit = {
    require(s.mode == ContractKeyUniquenessMode.Strict)

    unfold(addKey(toAlt(consumed)(s), mbKey, KeyInactive))
    unfold(addKey(toAlt(consumed)(s), mbKey, KeyInactive).visitCreate(contractId, mbKey))

    mbKey match {
      case None() => Trivial()
      case Some(gk) =>
        // STEP 1: conflicts match
        lookupActiveKeyToAlt(s, gk, consumed)
        activeKeysAddKey(toAlt(consumed)(s), gk, KeyInactive)
        unfold(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy))

        val newKeyInputs =
          if (s.globalKeyInputs.contains(gk)) s.globalKeyInputs
          else s.globalKeyInputs.updated(gk, KeyCreate)

        // STEP 2: globalKeys(newKeyInputs) == toAlt(s).addKey(gk, KeyInactive).globalKeyInputs
        unfold(addKey(toAlt(consumed)(s), gk, KeyInactive))
        globalKeyInputsContainsToAlt(s, gk, consumed)
        globalKeyInputsUpdatedToAlt(s, gk, KeyCreate, consumed)
    }

  }.ensuring(
    addKey(toAlt(consumed)(s), mbKey, KeyInactive).visitCreate(contractId, mbKey) == toAlt(
      consumed
    )(s.visitCreate(contractId, mbKey))
  )

  @pure
  def extractPair(
      e: Either[Option[
        ContractId
      ] => (KeyMapping, StateOriginal[NodeId]), (KeyMapping, StateOriginal[NodeId])],
      result: Option[ContractId],
  ): (KeyMapping, StateOriginal[NodeId]) = {
    e match {
      case Left(handle) => handle(result)
      case Right(p) => p
    }
  }

  @pure
  def extractState(
      e: Either[Option[
        ContractId
      ] => (KeyMapping, StateOriginal[NodeId]), (KeyMapping, StateOriginal[NodeId])],
      result: Option[ContractId],
  ): StateOriginal[NodeId] = {
    extractPair(e, result)._2
  }

  @pure
  @opaque
  def resolveKeyToAlt(
      s: StateOriginal[NodeId],
      result: Option[ContractId],
      gkey: GlobalKey,
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))

    lookupActiveKeyToAlt(s, gkey, consumed)
    invariantContainsKey(toAlt(consumed)(s), gkey, unbound, lc)
//      globalKeyInputsContainsToAlt(s, gkey, consumed)
    activeKeysAddKey(toAlt(consumed)(s), gkey, result)
    unfold(addKey(toAlt(consumed)(s), gkey, result))
    unfold(toAlt(consumed)(s).activeKeys.contains)

    unfold(addKey(toAlt(consumed)(s), gkey, result))
    globalKeyInputsUpdatedToAlt(
      s,
      gkey,
      result match {
        case None() => NegativeKeyLookup
        case Some(cid) => Transaction.KeyActive(cid)
      },
      consumed,
    )
    unfold(
      toAlt(consumed)(s).activeKeys.getOrElse(
        gkey,
        keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy)(result),
      )
    )
    unfold(keyMappingToActiveMapping(toAlt(consumed)(s).activeState.consumedBy))

  }.ensuring(
    (
      addKey(toAlt(consumed)(s), gkey, result).activeKeys(gkey),
      addKey(toAlt(consumed)(s), gkey, result),
    ) ==
      (extractPair(s.resolveKey(gkey), result)._1, toAlt(consumed)(
        extractState(s.resolveKey(gkey), result)
      ))
  )

  @pure
  @opaque
  def visitLookupToAlt(
      s: StateOriginal[NodeId],
      gk: GlobalKey,
      keyInput: Option[ContractId],
      keyResolution: Option[ContractId],
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))

    unfold(addKey(toAlt(consumed)(s), gk, keyInput).visitLookup(gk, keyResolution))
    keyInput match {
      case Some(cid) =>
        witnessContractIdAddKey(s, cid, Some(gk), consumed, unbound, lc)
        witnessContractIdToAlt(s, cid, consumed, unbound, lc)
        resolveKeyToAlt(s.witnessContractId(cid), keyInput, gk, consumed, unbound, lc)
        unfold(
          addKey(toAlt(consumed)(s), gk, keyInput)
            .witnessContractId(cid)
            .activeKeys
            .getOrElse(gk, KeyInactive)
        )
      case None() =>
        resolveKeyToAlt(s, keyInput, gk, consumed, unbound, lc)
        unfold(addKey(toAlt(consumed)(s), gk, keyInput).activeKeys.getOrElse(gk, KeyInactive))
    }

  }.ensuring(
    addKey(toAlt(consumed)(s), gk, keyInput).visitLookup(gk, keyResolution) ==
      toAlt(consumed)(s.visitLookup(gk, keyInput, keyResolution))
  )

  @pure
  @opaque
  def assertKeyMappingToAlt(
      s: StateOriginal[NodeId],
      cid: ContractId,
      gkey: Option[GlobalKey],
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))
    unfold(addKey(toAlt(consumed)(s), gkey, Some(cid)).assertKeyMapping(cid, gkey))
    unfold(addKey(toAlt(consumed)(s), gkey, Some(cid)))
    gkey match {
      case None() => Trivial()
      case Some(k) => visitLookupToAlt(s, k, Some(cid), Some(cid), consumed, unbound, lc)
    }

  }.ensuring(
    addKey(toAlt(consumed)(s), gkey, Some(cid)).assertKeyMapping(cid, gkey) == toAlt(consumed)(
      s.assertKeyMapping(cid, gkey)
    )
  )

  @pure
  @opaque
  def witnessContractIdToAlt(
      s: StateOriginal[NodeId],
      cid: ContractId,
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))
    unfold(toAlt(consumed)(s).witnessContractId(cid))
  }.ensuring(
    toAlt(consumed)(s).witnessContractId(cid) == toAlt(consumed)(s.witnessContractId(cid))
  )

  @pure
  @opaque
  def witnessContractIdAddKey(
      s: StateOriginal[NodeId],
      cid: ContractId,
      gkey: Option[GlobalKey],
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))
    unfold(toAlt(consumed)(s), gkey, Some(cid))
  }.ensuring(
    addKey(toAlt(consumed)(s), gkey, Some(cid)).witnessContractId(cid)
      == addKey(toAlt(consumed)(s).witnessContractId(cid), gkey, Some(cid))
  )

  @pure
  @opaque
  def visitFetchToAlt(
      s: StateOriginal[NodeId],
      cid: ContractId,
      gkey: Option[GlobalKey],
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))
    unfold(addKey(toAlt(consumed)(s), gkey, Some(cid)).visitFetch(cid, gkey))
    witnessContractIdAddKey(s, cid, gkey, consumed, unbound, lc)
    witnessContractIdToAlt(s, cid, consumed, unbound, lc)
    assertKeyMappingToAlt(s.witnessContractId(cid), cid, gkey, consumed, unbound, lc)
  }.ensuring(
    addKey(toAlt(consumed)(s), gkey, Some(cid)).visitFetch(cid, gkey) == toAlt(consumed)(
      s.visitFetch(cid, gkey, true)
    )
  )

  @pure
  @opaque
  def visitExerciseToAlt(
      s: StateOriginal[NodeId],
      nodeId: NodeId,
      targetId: ContractId,
      gk: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))
    require(s.mode == ContractKeyUniquenessMode.Strict)
    unfold(
      addKey(toAlt(consumed)(s), gk, Some(targetId))
        .visitExercise(nodeId, targetId, gk, byKey, consuming)
    )
    unfold(
      addKey(toAlt(consumed)(s), gk, Some(targetId))
        .witnessContractId(targetId)
    )

    assertKeyMappingToAlt(s, targetId, gk, consumed, unbound, lc)
    s.assertKeyMapping(targetId, gk) match {
      case Left(e) => Trivial()
      case Right(state) => consumeToAlt(state, targetId, nodeId, consumed)
    }
  }.ensuring(
    addKey(toAlt(consumed)(s), gk, Some(targetId))
      .visitExercise(nodeId, targetId, gk, byKey, consuming) ==
      toAlt(if (consuming) consumed + targetId else consumed)(
        s.visitExercise(nodeId, targetId, gk, byKey, consuming)
      )
  )

  @pure @opaque
  def nodeConsumed(consumed: Set[ContractId], n: Node): Set[ContractId] = {
    n match {
      case exe: Node.Exercise if exe.consuming => consumed + exe.targetCoid
      case _ => consumed
    }
  }

  @pure
  @opaque
  def handleNodeOriginal(
      e: Either[KeyInputError, StateOriginal[NodeId]],
      nodeId: NodeId,
      node: Node.Action,
      keyInput: Option[ContractId],
  ): Either[KeyInputError, StateOriginal[NodeId]] = {
    e match {
      case Right(s) => s.handleNode(nodeId, node, keyInput)
      case Left(_) => e
    }
  }

  @pure @opaque
  def handleNodeToAlt(
      s: StateOriginal[NodeId],
      nodeId: NodeId,
      node: Node.Action,
      keyInput: Option[ContractId],
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(s))(unbound, lc))
    require(s.mode == ContractKeyUniquenessMode.Strict)

    unfold(nodeConsumed(consumed, node))
    unfold(addKeyBeforeAction(toAlt(consumed)(s), node))
    unfold(addKeyBeforeAction(toAlt(consumed)(s), node).handleNode(nodeId, node))
    unfold(nodeActionKeyMapping(node))

    node match {
      case create: Node.Create => visitCreateToAlt(s, create.coid, create.gkeyOpt, consumed)
      case fetch: Node.Fetch =>
        visitFetchToAlt(s, fetch.coid, fetch.gkeyOpt, consumed, unbound, lc)
      case lookup: Node.LookupByKey =>
        unfold(addKey(toAlt(consumed)(s), node.gkeyOpt, nodeActionKeyMapping(node)))
        visitLookupToAlt(s, lookup.gkey, lookup.result, lookup.result, consumed, unbound, lc)
      case exe: Node.Exercise =>
        visitExerciseToAlt(
          s,
          nodeId,
          exe.targetCoid,
          exe.gkeyOpt,
          exe.byKey,
          exe.consuming,
          consumed,
          unbound,
          lc,
        )
    }
  }.ensuring(
    addKeyBeforeAction(toAlt(consumed)(s), node).handleNode(nodeId, node) ==
      toAlt(nodeConsumed(consumed, node))(s.handleNode(nodeId, node, keyInput))
  )

  @pure
  @opaque
  def handleNodeToAlt(
      e: Either[KeyInputError, StateOriginal[NodeId]],
      nodeId: NodeId,
      node: Node.Action,
      keyInput: Option[ContractId],
      consumed: Set[ContractId],
      unbound: Set[ContractId],
      lc: Set[ContractId],
  ): Unit = {
    require(stateInvariant(toAlt(consumed)(e))(unbound, lc))
    require(e.forall(s => s.mode == ContractKeyUniquenessMode.Strict))

    unfold(addKeyBeforeNode(toAlt(consumed)(e), node))
    unfold(handleNodeOriginal(e, nodeId, node, keyInput))

    e match {
      case Right(s) =>
        unfold(addKeyBeforeNode(toAlt(consumed)(s), node))
        handleNodeToAlt(s, nodeId, node, keyInput, consumed, unbound, lc)
      case Left(_) => Trivial()
    }

  }.ensuring(
    handleNode(addKeyBeforeNode(toAlt(consumed)(e), node), nodeId, node) ==
      toAlt(nodeConsumed(consumed, node))(handleNodeOriginal(e, nodeId, node, keyInput))
  )

  @pure
  @opaque
  def beginRollbackToAlt(s: StateOriginal[NodeId], consumed: Set[ContractId]): Unit = {
    unfold(toAlt(consumed)(s).beginRollback())
  }.ensuring(
    toAlt(consumed)(s).beginRollback() ==
      toAlt(consumed)(s.beginRollback())
  )

  @pure @opaque
  def endRollbackOriginal(
      s: StateOriginal[NodeId]
  ): Either[KeyInputError, StateOriginal[NodeId]] = {
    if (s.withinRollbackScope) {
      Right[KeyInputError, StateOriginal[NodeId]](s.endRollback())
    } else {
      Left[KeyInputError, StateOriginal[NodeId]](
        InconsistentContractKeyKIError(InconsistentContractKey(GlobalKey(BigInt(0))))
      )
    }
  }

  @pure
  @opaque
  def endRollbackToAlt(s: StateOriginal[NodeId], consumed: Set[ContractId]): Unit = {
    unfold(toAlt(consumed)(s).endRollback())
    unfold(endRollbackOriginal(s))
  }.ensuring(
    toAlt(consumed)(s).endRollback() ==
      toAlt(consumed)(endRollbackOriginal(s))
  )

  @pure
  @opaque
  def advanceToAlt(
      s: ActiveLedgerStateOriginal[NodeId],
      substate: ActiveLedgerStateOriginal[NodeId],
  ): Unit = {

    unfold(toAlt(s).advance(toAlt(substate)))
  }.ensuring(toAlt(s).advance(toAlt(substate)) == toAlt(s.advance(substate)))

  /** Proof of only a part of advance equivalence. The other part is not here due to a Stainless bug.
    */
  @pure @opaque
  def advanceToAlt(
      s: StateOriginal[NodeId],
      substate: StateOriginal[NodeId],
      resolver: KeyResolver,
      consumed1: Set[ContractId],
      consumed2: Set[ContractId],
  ): Unit = {
    require(s.mode == ContractKeyUniquenessMode.Strict)
    require(!substate.withinRollbackScope)
    require(!toAlt(consumed2)(substate).withinRollbackScope)
    require(toAlt(consumed1)(s).advance(toAlt(consumed2)(substate)).isRight)
    require(s.advance(resolver, substate).isRight)

    unfold(globalKeys(s.globalKeyInputs))
    unfold(globalKeys(substate.globalKeyInputs))
    unfold(globalKeys(substate.globalKeyInputs ++ s.globalKeyInputs))

    MapProperties.mapValuesConcat(substate.globalKeyInputs, s.globalKeyInputs, toKeyMapping)
    MapAxioms.extensionality(
      globalKeys(substate.globalKeyInputs ++ s.globalKeyInputs),
      globalKeys(substate.globalKeyInputs) ++ globalKeys(s.globalKeyInputs),
    )
    advanceToAlt(s.activeState, substate.activeState)

  }.ensuring(
    (toAlt(consumed1)(s).advance(toAlt(consumed2)(substate)).get ==
      toAlt(consumed1 ++ consumed2)(s.advance(resolver, substate).get))
  )

}
