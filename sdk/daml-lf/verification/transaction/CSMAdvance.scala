// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified.transaction

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
import CSMEitherDef._

/** This file expresses the change of active state after handling a node as a call to [[ActiveLedgerState.advance]]
  * with another state representing what is added.
  *
  * It also contains necessary properties of the advance methods that are needed in order to prove the above claim.
  */
object CSMAdvanceDef {

  /** Express what is added to an active state after handling a node.
    *
    * @see [[CSMAdvance.handleNodeActiveState]] for an use case of this definition.
    */
  @pure
  @opaque
  def actionActiveStateAddition(id: NodeId, n: Node.Action): ActiveLedgerState = {
    n match {
      case create: Node.Create =>
        ActiveLedgerState(
          Set(create.coid),
          Map.empty[ContractId, NodeId],
          (create.gkeyOpt match {
            case None() => Map.empty[GlobalKey, ContractId]
            case Some(k) => Map[GlobalKey, ContractId](k -> create.coid)
          }),
        )
      case exe: Node.Exercise =>
        ActiveLedgerState(
          Set.empty[ContractId],
          (if (exe.consuming) Map(exe.targetCoid -> id) else Map.empty[ContractId, NodeId]),
          Map.empty[GlobalKey, ContractId],
        )
      case _ => ActiveLedgerState.empty
    }
  }

}

object CSMAdvance {

  import CSMAdvanceDef._

  /** [[ActiveLedgerState.empty]] is a neutral element wrt [[ActiveLedgerState.advance]].
    *
    * The set extensionality axiom is used here for the sake of simplicity.
    */
  @pure
  @opaque
  def emptyAdvance(s: ActiveLedgerState): Unit = {
    unfold(s.advance(ActiveLedgerState.empty))
    unfold(ActiveLedgerState.empty.advance(s))

    SetProperties.unionEmpty(s.locallyCreatedThisTimeline)
    SetAxioms.extensionality(
      s.locallyCreatedThisTimeline ++ Set.empty[ContractId],
      s.locallyCreatedThisTimeline,
    )
    SetAxioms.extensionality(
      Set.empty[ContractId] ++ s.locallyCreatedThisTimeline,
      s.locallyCreatedThisTimeline,
    )
    MapProperties.concatEmpty(s.consumedBy)
    MapAxioms.extensionality(s.consumedBy ++ Map.empty[ContractId, NodeId], s.consumedBy)
    MapAxioms.extensionality(Map.empty[ContractId, NodeId] ++ s.consumedBy, s.consumedBy)
    MapProperties.concatEmpty(s.localKeys)
    MapAxioms.extensionality(s.localKeys ++ Map.empty[GlobalKey, ContractId], s.localKeys)
    MapAxioms.extensionality(Map.empty[GlobalKey, ContractId] ++ s.localKeys, s.localKeys)

  }.ensuring(
    (s.advance(ActiveLedgerState.empty) == s) &&
      (ActiveLedgerState.empty.advance(s) == s)
  )

  /** [[ActiveLedgerState.advance]] is an associative operation.
    *
    * The set extensionality axiom is used here for the sake of simplicity.
    */
  @pure
  @opaque
  def advanceAssociativity(
      s1: ActiveLedgerState,
      s2: ActiveLedgerState,
      s3: ActiveLedgerState,
  ): Unit = {
    unfold(s1.advance(s2).advance(s3))
    unfold(s1.advance(s2))
    unfold(s1.advance(s2.advance(s3)))
    unfold(s2.advance(s3))

    SetProperties.unionAssociativity(
      s1.locallyCreatedThisTimeline,
      s2.locallyCreatedThisTimeline,
      s3.locallyCreatedThisTimeline,
    )
    SetAxioms.extensionality(
      (s1.locallyCreatedThisTimeline ++ s2.locallyCreatedThisTimeline) ++ s3.locallyCreatedThisTimeline,
      s1.locallyCreatedThisTimeline ++ (s2.locallyCreatedThisTimeline ++ s3.locallyCreatedThisTimeline),
    )
    MapProperties.concatAssociativity(s1.consumedBy, s2.consumedBy, s3.consumedBy)
    MapAxioms.extensionality(
      (s1.consumedBy ++ s2.consumedBy) ++ s3.consumedBy,
      s1.consumedBy ++ (s2.consumedBy ++ s3.consumedBy),
    )
    MapProperties.concatAssociativity(s1.localKeys, s2.localKeys, s3.localKeys)
    MapAxioms.extensionality(
      (s1.localKeys ++ s2.localKeys) ++ s3.localKeys,
      s1.localKeys ++ (s2.localKeys ++ s3.localKeys),
    )
  }.ensuring(
    s1.advance(s2).advance(s3) == s1.advance(s2.advance(s3))
  )

  /** Two states are equal if and only if one is the other after calling [[ActiveLedgerState.advance]] with
    * [[ActiveLedgerState.empty]].
    */
  @pure
  @opaque
  def sameActiveStateActiveState[T](s1: State, e2: Either[T, State]): Unit = {
    require(sameActiveState(s1, e2))

    unfold(sameActiveState(s1, e2))

    e2 match {
      case Right(s2) => emptyAdvance(s1.activeState)
      case _ => Trivial()
    }

  }.ensuring(
    sameActiveState(s1, e2) ==
      e2.forall(
        _.activeState ==
          s1.activeState.advance(ActiveLedgerState.empty)
      )
  )

  /** Express the [[State.activeState]] of a [[State]], after handling a [[Node.Create]], as a call to
    * [[ActiveLedgerState.advance]].
    *
    * The set extensionality axiom is used here for the sake of simplicity.
    */
  @pure
  @opaque
  def visitCreateActiveState(s: State, contractId: ContractId, mbKey: Option[GlobalKey]): Unit = {
    unfold(s.visitCreate(contractId, mbKey))
    unfold(
      s.activeState.advance(
        ActiveLedgerState(
          Set(contractId),
          Map.empty[ContractId, NodeId],
          (mbKey match {
            case None() => Map.empty[GlobalKey, ContractId]
            case Some(k) => Map[GlobalKey, ContractId](k -> contractId)
          }),
        )
      )
    )

    unfold(s.activeState.locallyCreatedThisTimeline.incl(contractId))
    MapProperties.concatEmpty(s.activeState.consumedBy)
    MapAxioms.extensionality(
      s.activeState.consumedBy ++ Map.empty[ContractId, NodeId],
      s.activeState.consumedBy,
    )

    mbKey match {
      case None() =>
        MapProperties.concatEmpty(s.activeState.localKeys)
        MapAxioms.extensionality(
          s.activeState.localKeys ++ Map.empty[GlobalKey, ContractId],
          s.activeState.localKeys,
        )
      case Some(k) =>
        unfold(s.activeState.localKeys.updated(k, contractId))
    }

  }.ensuring(
    s.visitCreate(contractId, mbKey)
      .forall(
        _.activeState == s.activeState.advance(
          ActiveLedgerState(
            Set(contractId),
            Map.empty[ContractId, NodeId],
            (mbKey match {
              case None() => Map.empty[GlobalKey, ContractId]
              case Some(k) => Map[GlobalKey, ContractId](k -> contractId)
            }),
          )
        )
      )
  )

  /** Express the [[State.activeState]] of a [[State]], after handling a [[Node.Exercise]], as a call to
    * [[ActiveLedgerState.advance]].
    *
    * The set extensionality axiom is used here for the sake of simplicity.
    */
  @pure
  @opaque
  def visitExerciseActiveState(
      s: State,
      nodeId: NodeId,
      targetId: ContractId,
      mbKey: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
  ): Unit = {

    unfold(s.visitExercise(nodeId, targetId, mbKey, byKey, consuming))
    unfold(
      s.activeState.advance(
        ActiveLedgerState(
          Set.empty[ContractId],
          (if (consuming) Map(targetId -> nodeId) else Map.empty[ContractId, NodeId]),
          Map.empty[GlobalKey, ContractId],
        )
      )
    )
    unfold(sameActiveState(s, s.assertKeyMapping(targetId, mbKey)))

    s.assertKeyMapping(targetId, mbKey) match {
      case Right(state) =>
        unfold(state.consume(targetId, nodeId))
        unfold(state.activeState.consume(targetId, nodeId))
        unfold(state.activeState.consumedBy.updated(targetId, nodeId))
        MapProperties.concatEmpty(state.activeState.consumedBy)
        MapAxioms.extensionality(
          state.activeState.consumedBy ++ Map.empty[ContractId, NodeId],
          state.activeState.consumedBy,
        )

        SetProperties.unionEmpty(state.activeState.locallyCreatedThisTimeline)
        SetAxioms.extensionality(
          state.activeState.locallyCreatedThisTimeline ++ Set.empty[ContractId],
          state.activeState.locallyCreatedThisTimeline,
        )
        MapProperties.concatEmpty(state.activeState.localKeys)
        MapAxioms.extensionality(
          state.activeState.localKeys ++ Map.empty[GlobalKey, ContractId],
          state.activeState.localKeys,
        )
      case _ => Trivial()
    }

  }.ensuring(
    s.visitExercise(nodeId, targetId, mbKey, byKey, consuming)
      .forall(
        _.activeState == s.activeState.advance(
          ActiveLedgerState(
            Set.empty[ContractId],
            (if (consuming) Map(targetId -> nodeId) else Map.empty[ContractId, NodeId]),
            Map.empty[GlobalKey, ContractId],
          )
        )
      )
  )

  /** Express the [[State.activeState]] of a [[State]], after handling a [[Node.Action]], as a call to
    * [[ActiveLedgerState.advance]].
    */
  @pure
  @opaque
  def handleNodeActiveState(s: State, nodeId: NodeId, n: Node.Action): Unit = {

    unfold(s.handleNode(nodeId, n))
    unfold(actionActiveStateAddition(nodeId, n))

    n match {
      case create: Node.Create => visitCreateActiveState(s, create.coid, create.gkeyOpt)
      case fetch: Node.Fetch =>
        sameActiveStateActiveState(s, s.assertKeyMapping(fetch.coid, fetch.gkeyOpt))
      case lookup: Node.LookupByKey =>
        sameActiveStateActiveState(s, s.visitLookup(lookup.gkey, lookup.result))
      case exe: Node.Exercise =>
        visitExerciseActiveState(s, nodeId, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming)
    }

  }.ensuring(
    s.handleNode(nodeId, n)
      .forall(sf => sf.activeState == s.activeState.advance(actionActiveStateAddition(nodeId, n)))
  )

}
