// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package transaction

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
  NodeId,
  MapProperties,
  SetProperties,
}
import utils.Value.ContractId
import utils.Transaction.{
  KeyCreate,
  KeyInputError,
  NegativeKeyLookup,
  InconsistentContractKey,
  DuplicateContractKey,
  KeyInput,
}
import CSMHelpers._
import CSMEitherDef._
import CSMEither._

/** Simplified version of the contract state machine. All the implementations are simplified, and [[State.globalKeys]] are
  * not updated anymore during [[State.handleNode]]. [[CSMKeysPropertiesDef.addKeyBeforeNode]] has to be called beforehand.
  */
case class State(
    locallyCreated: Set[ContractId],
    consumed: Set[ContractId],
    globalKeys: Map[GlobalKey, ContractStateMachine.KeyMapping],
    activeState: ContractStateMachine.ActiveLedgerState,
    rollbackStack: List[ContractStateMachine.ActiveLedgerState],
) {

  import ContractStateMachine._

  @pure @opaque
  def keys: Map[GlobalKey, KeyMapping] = {
    MapProperties.mapValuesKeySet(activeState.localKeys, KeyActive)
    SetProperties.unionEqualsRight(
      globalKeys.keySet,
      activeState.localKeys.keySet,
      activeState.localKeys.mapValues(KeyActive).keySet,
    )
    MapProperties.concatKeySet(globalKeys, activeState.localKeys.mapValues(KeyActive))
    SetProperties.equalsTransitivity(
      (globalKeys ++ activeState.localKeys.mapValues(KeyActive)).keySet,
      globalKeys.keySet ++ activeState.localKeys.mapValues(KeyActive).keySet,
      globalKeys.keySet ++ activeState.localKeys.keySet,
    )

    globalKeys ++ activeState.localKeys.mapValues(KeyActive)
  }.ensuring(res => res.keySet === globalKeys.keySet ++ activeState.localKeys.keySet)

  @pure @opaque
  def activeKeys: Map[GlobalKey, KeyMapping] = {
    keys.mapValues(keyMappingToActiveMapping(activeState.consumedBy))
  }

  @pure @opaque
  def consume(cid: ContractId, nid: NodeId): State = {
    unfold(activeState.consume(cid, nid))
    this.copy(activeState = activeState.consume(cid, nid), consumed = consumed + cid)
  }.ensuring(res =>
    (globalKeys == res.globalKeys) &&
      (locallyCreated == res.locallyCreated) &&
      (rollbackStack == res.rollbackStack) &&
      (activeState.localKeys == res.activeState.localKeys)
  )

  @pure @opaque
  def visitCreate(
      contractId: ContractId,
      mbKey: Option[GlobalKey],
  ): Either[DuplicateContractKey, State] = {

    val me =
      this.copy(
        locallyCreated = locallyCreated + contractId,
        activeState = this.activeState
          .copy(locallyCreatedThisTimeline =
            this.activeState.locallyCreatedThisTimeline + contractId
          ),
      )

    val res = mbKey match {
      case None() => Right[DuplicateContractKey, State](me)
      case Some(gk) =>
        val conflict = activeKeys.get(gk).exists(_ != KeyInactive)

        Either.cond(
          !conflict,
          me.copy(activeState = me.activeState.createKey(gk, contractId)),
          DuplicateContractKey(gk),
        )
    }

    unfold(sameGlobalKeys(this, res))
    unfold(sameStack(this, res))
    unfold(sameConsumed(this, res))

    res
  }.ensuring(res =>
    sameGlobalKeys(this, res) &&
      sameStack(this, res) &&
      sameConsumed(this, res) &&
      res.forall(r => r.locallyCreated == locallyCreated + contractId)
  )

  @pure @opaque
  def visitExercise(
      nodeId: NodeId,
      targetId: ContractId,
      mbKey: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
  ): Either[InconsistentContractKey, State] = {
    val res =
      for {
        state <- assertKeyMapping(targetId, mbKey)
      } yield {

        if (consuming)
          state.consume(targetId, nodeId)
        else state
      }
    unfold(sameGlobalKeys(this, assertKeyMapping(targetId, mbKey)))
    unfold(sameStack(this, assertKeyMapping(targetId, mbKey)))
    unfold(sameLocalKeys(this, assertKeyMapping(targetId, mbKey)))
    unfold(sameLocallyCreated(this, assertKeyMapping(targetId, mbKey)))
    unfold(sameGlobalKeys(this, res))
    unfold(sameLocalKeys(this, res))
    unfold(sameStack(this, res))
    unfold(sameLocallyCreated(this, res))
    res
  }.ensuring(res =>
    sameGlobalKeys(this, res) &&
      sameStack(this, res) &&
      sameLocalKeys(this, res) &&
      sameLocallyCreated(this, res)
  )

  @pure @opaque
  def visitLookup(
      gk: GlobalKey,
      keyResolution: Option[ContractId],
  ): Either[InconsistentContractKey, State] = {
    val res = Either.cond(
      activeKeys.getOrElse(gk, KeyInactive) == keyResolution,
      this,
      InconsistentContractKey(gk),
    )
    unfold(sameState(this, res))
    res
  }.ensuring(res => sameState(this, res))

  @pure @opaque
  def assertKeyMapping(
      cid: ContractId,
      mbKey: Option[GlobalKey],
  ): Either[InconsistentContractKey, State] = {
    val res = mbKey match {
      case None() => Right[InconsistentContractKey, State](this)
      case Some(gk) => visitLookup(gk, KeyActive(cid))
    }
    unfold(sameState(this, res))
    res
  }.ensuring(res => sameState(this, res))

  @pure @opaque
  def handleNode(id: NodeId, node: Node.Action): Either[KeyInputError, State] = {
    val res = node match {
      case create: Node.Create => toKeyInputError(visitCreate(create.coid, create.gkeyOpt))
      case fetch: Node.Fetch => toKeyInputError(assertKeyMapping(fetch.coid, fetch.gkeyOpt))
      case lookup: Node.LookupByKey => toKeyInputError(visitLookup(lookup.gkey, lookup.result))
      case exe: Node.Exercise =>
        toKeyInputError(visitExercise(id, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming))
    }

    @pure @opaque
    def sameHandleNode: Unit = {
      node match {
        case create: Node.Create =>
          sameGlobalKeysTransitivity(this, visitCreate(create.coid, create.gkeyOpt), res)
          sameStackTransitivity(this, visitCreate(create.coid, create.gkeyOpt), res)
        case fetch: Node.Fetch =>
          sameGlobalKeysTransitivity(this, assertKeyMapping(fetch.coid, fetch.gkeyOpt), res)
          sameStackTransitivity(this, assertKeyMapping(fetch.coid, fetch.gkeyOpt), res)
        case lookup: Node.LookupByKey =>
          sameGlobalKeysTransitivity(this, visitLookup(lookup.gkey, lookup.result), res)
          sameStackTransitivity(this, visitLookup(lookup.gkey, lookup.result), res)
        case exe: Node.Exercise =>
          sameGlobalKeysTransitivity(
            this,
            visitExercise(id, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming),
            res,
          )
          sameStackTransitivity(
            this,
            visitExercise(id, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming),
            res,
          )
      }
    }.ensuring(sameGlobalKeys(this, res) && sameStack(this, res))

    sameHandleNode

    res
  }.ensuring(res =>
    sameGlobalKeys(this, res) &&
      sameStack(this, res)
  )

  @pure @opaque
  def beginRollback(): State = {
    val res = this.copy(rollbackStack = activeState :: rollbackStack)
    unfold(res.withinRollbackScope)
    res
  }.ensuring(res =>
    (globalKeys == res.globalKeys) &&
      (locallyCreated == res.locallyCreated) &&
      (consumed == res.consumed)
  )

  @pure @opaque
  def endRollback(): Either[KeyInputError, State] = {
    val res = rollbackStack match {
      case Nil() =>
        Left[KeyInputError, State](
          Left[InconsistentContractKey, DuplicateContractKey](
            InconsistentContractKey(GlobalKey(BigInt(0)))
          )
        )
      case Cons(headState, tailStack) =>
        Right[KeyInputError, State](this.copy(activeState = headState, rollbackStack = tailStack))
    }
    unfold(sameGlobalKeys(this, res))
    unfold(sameLocallyCreated(this, res))
    unfold(sameConsumed(this, res))
    res
  }.ensuring(res =>
    sameGlobalKeys(this, res) &&
      sameLocallyCreated(this, res) &&
      sameConsumed(this, res)
  )

  @pure @opaque
  def withinRollbackScope: Boolean = !rollbackStack.isEmpty

  @pure
  def advance(substate: State): Either[Unit, State] = {
    require(!substate.withinRollbackScope)
    if (
      substate.globalKeys.keySet
        .forall(k => activeKeys.get(k).forall(m => Some(m) == substate.globalKeys.get(k)))
    ) {
      Right[Unit, State](
        this.copy(
          locallyCreated = locallyCreated ++ substate.locallyCreated,
          consumed = consumed ++ substate.consumed,
          globalKeys = substate.globalKeys ++ globalKeys,
          activeState = activeState.advance(substate.activeState),
        )
      )
    } else {
      Left[Unit, State](())
    }
  }

}

object State {
  def empty: State = new State(
    Set.empty,
    Set.empty,
    Map.empty,
    ContractStateMachine.ActiveLedgerState.empty,
    List.empty,
  )
}

object ContractStateMachine {

  type KeyResolver = Map[GlobalKey, KeyMapping]

  type KeyMapping = Option[ContractId]
  val KeyInactive: KeyMapping = None[ContractId]()
  val KeyActive: ContractId => KeyMapping = Some[ContractId](_)

  final case class ActiveLedgerState(
      locallyCreatedThisTimeline: Set[ContractId],
      consumedBy: Map[ContractId, NodeId],
      localKeys: Map[GlobalKey, ContractId],
  ) {

    @pure @opaque
    def consume(contractId: ContractId, nodeId: NodeId): ActiveLedgerState =
      this.copy(consumedBy = consumedBy.updated(contractId, nodeId))

    def createKey(key: GlobalKey, cid: ContractId): ActiveLedgerState =
      this.copy(localKeys = localKeys.updated(key, cid))

    @pure @opaque
    def advance(substate: ActiveLedgerState): ActiveLedgerState =
      ActiveLedgerState(
        locallyCreatedThisTimeline =
          locallyCreatedThisTimeline ++ substate.locallyCreatedThisTimeline,
        consumedBy = consumedBy ++ substate.consumedBy,
        localKeys = localKeys ++ substate.localKeys,
      )
  }

  object ActiveLedgerState {
    def empty: ActiveLedgerState = ActiveLedgerState(
      Set.empty[ContractId],
      Map.empty[ContractId, NodeId],
      Map.empty[GlobalKey, ContractId],
    )
  }
}
