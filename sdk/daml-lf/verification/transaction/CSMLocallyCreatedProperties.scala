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
import CSMEither._
import CSMEitherDef._

/** This file shows how the set of locallyCreated and consumed contracts of a state behave after handling a node.
  */
object CSMLocallyCreatedProperties {

  /** The set of locally created contracts has the new contract added to it when the node is an instance of Create and
    * otherwise remains the same
    */
  @pure
  @opaque
  def handleNodeLocallyCreated(s: State, id: NodeId, node: Node.Action): Unit = {
    unfold(s.handleNode(id, node))

    val res = s.handleNode(id, node)

    node match {
      case create: Node.Create => Trivial()
      case fetch: Node.Fetch =>
        sameLocallyCreatedTransitivity(s, s.assertKeyMapping(fetch.coid, fetch.gkeyOpt), res)
        unfold(sameLocallyCreated(s, res))
      case lookup: Node.LookupByKey =>
        sameLocallyCreatedTransitivity(s, s.visitLookup(lookup.gkey, lookup.result), res)
        unfold(sameLocallyCreated(s, res))
      case exe: Node.Exercise =>
        sameLocallyCreatedTransitivity(
          s,
          s.visitExercise(id, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming),
          res,
        )
        unfold(sameLocallyCreated(s, res))
    }
  }.ensuring(
    s.handleNode(id, node)
      .forall(r =>
        node match {
          case Node.Create(coid, _) => r.locallyCreated == s.locallyCreated + coid
          case _ => s.locallyCreated == r.locallyCreated
        }
      )
  )

  /** The set of locally created contracts has the new contract added to it when the node is an instance of Create and
    * otherwise remains the same
    */
  @pure
  @opaque
  def handleNodeLocallyCreated(
      e: Either[KeyInputError, State],
      id: NodeId,
      node: Node.Action,
  ): Unit = {
    unfold(handleNode(e, id, node))
    e match {
      case Left(_) => Trivial()
      case Right(s) => handleNodeLocallyCreated(s, id, node)
    }
  }.ensuring(
    handleNode(e, id, node).forall(r =>
      e.forall(s =>
        node match {
          case Node.Create(coid, _) => r.locallyCreated == s.locallyCreated + coid
          case _ => s.locallyCreated == r.locallyCreated
        }
      )
    )
  )

  /** If two states propagates the error have the same set of locally created contracts then if the first is a subset of
    * a third set then the second one also is.
    */
  def sameLocallyCreatedSubsetOfTransivity[T, U](
      e1: Either[T, State],
      e2: Either[U, State],
      lc: Set[ContractId],
  ): Unit = {
    require(sameLocallyCreated(e1, e2))
    require(propagatesError(e1, e2))
    require(e1.forall(s1 => s1.locallyCreated.subsetOf(lc)))

    unfold(propagatesError(e1, e2))
    unfold(sameLocallyCreated(e1, e2))
    e1 match {
      case Left(_) => Trivial()
      case Right(s1) => unfold(sameLocallyCreated(s1, e2))
    }
  }.ensuring(e2.forall(s2 => s2.locallyCreated.subsetOf(lc)))

  /** The set of consumed contracts has the consumed contract added to it when the node is an instance of a consuming
    * Exercise and otherwise remains the same
    */
  @pure
  @opaque
  def handleNodeConsumed(s: State, id: NodeId, node: Node.Action): Unit = {
    unfold(s.handleNode(id, node))

    val res = s.handleNode(id, node)

    node match {
      case create: Node.Create =>
        sameConsumedTransitivity(s, s.visitCreate(create.coid, create.gkeyOpt), res)
        unfold(sameConsumed(s, res))
      case fetch: Node.Fetch =>
        sameConsumedTransitivity(s, s.assertKeyMapping(fetch.coid, fetch.gkeyOpt), res)
        unfold(sameConsumed(s, res))
      case lookup: Node.LookupByKey =>
        sameConsumedTransitivity(s, s.visitLookup(lookup.gkey, lookup.result), res)
        unfold(sameConsumed(s, res))
      case exe: Node.Exercise =>
        unfold(s.visitExercise(id, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming))
        for {
          state <- s.assertKeyMapping(exe.targetCoid, exe.gkeyOpt)
        } yield {
          unfold(sameConsumed(s, s.assertKeyMapping(exe.targetCoid, exe.gkeyOpt)))
          unfold(state.consume(exe.targetCoid, id))
        }
        ()
    }
  }.ensuring(
    s.handleNode(id, node)
      .forall(r =>
        node match {
          case Node.Exercise(targetCoid, true, _, _, _) => r.consumed == s.consumed + targetCoid
          case _ => s.consumed == r.consumed
        }
      )
  )

  /** The set of consumed contracts has the consumed contract added to it when the node is an instance of a consuming
    * Exercise and otherwise remains the same
    */
  @pure
  @opaque
  def handleNodeConsumed(e: Either[KeyInputError, State], id: NodeId, node: Node.Action): Unit = {
    unfold(handleNode(e, id, node))
    e match {
      case Left(_) => Trivial()
      case Right(s) => handleNodeConsumed(s, id, node)
    }
  }.ensuring(
    handleNode(e, id, node).forall(r =>
      e.forall(s =>
        node match {
          case Node.Exercise(targetCoid, true, _, _, _) => r.consumed == s.consumed + targetCoid
          case _ => s.consumed == r.consumed
        }
      )
    )
  )

  /** If two states propagates the error have the same set of consumed contracts then if the first is a subset of
    * a third set then the second one also is.
    */
  def sameConsumedSubsetOfTransivity[T, U](
      e1: Either[T, State],
      e2: Either[U, State],
      lc: Set[ContractId],
  ): Unit = {
    require(sameConsumed(e1, e2))
    require(propagatesError(e1, e2))
    require(e1.forall(s1 => s1.consumed.subsetOf(lc)))

    unfold(propagatesError(e1, e2))
    unfold(sameConsumed(e1, e2))
    e1 match {
      case Left(_) => Trivial()
      case Right(s1) => unfold(sameConsumed(s1, e2))
    }
  }.ensuring(e2.forall(s2 => s2.consumed.subsetOf(lc)))

}
