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
import utils.TransactionErrors.{CreateError, InconsistentContractKey, KeyInputError}
import utils._

import ContractStateMachine._
import CSMEitherDef._
import CSMEither._

/** Helpers definitions
  *
  * Definitions tightly related to ContractStateMachineAlt content. Most of them are extensions of already existing
  * methods with Either arguments type instead of State.
  */
object CSMHelpers {

  /** Extension of [[State.handleNode]] method to a union type argument. If e is defined calls handleNode otherwise returns
    * the same error.
    */
  @pure
  def handleNode(
      e: Either[KeyInputError, State],
      id: NodeId,
      node: Node.Action,
  ): Either[KeyInputError, State] = {
    val res = e match {
      case Left(e) => Left[KeyInputError, State](e)
      case Right(s) => s.handleNode(id, node)
    }
    unfold(propagatesError(e, res))
    unfold(sameGlobalKeys(e, res))
    res
  }.ensuring(res =>
    propagatesError(e, res) &&
      sameGlobalKeys(e, res)
  )

  /** Converts consumed active key mappings into an inactive key mappings.
    */
  @pure
  @opaque
  def keyMappingToActiveMapping(consumedBy: Map[ContractId, NodeId]): KeyMapping => KeyMapping = {
    case Some(cid) if !consumedBy.contains(cid) => KeyActive(cid)
    case _ => KeyInactive
  }

  /** Converts the error of an union type argument (InconsistentContractKey or DuplicateContractkey) into the more
    * generic error type KeyInputError.
    */
  @pure
  def toKeyInputError(e: Either[InconsistentContractKey, State]): Either[KeyInputError, State] = {
    sameStateLeftProj(e, KeyInputError.inject(_: InconsistentContractKey))
    e.left.map(KeyInputError.inject)
  }.ensuring(res =>
    propagatesBothError(e, res) &&
      sameState(e, res)
  )

  @pure
  @targetName("toKeyInputErrorCreateError")
  def toKeyInputError(e: Either[CreateError, State]): Either[KeyInputError, State] = {
    sameStateLeftProj(e, KeyInputError.from(_: CreateError))
    e.left.map(KeyInputError.from)
  }.ensuring(res =>
    propagatesBothError(e, res) &&
      sameState(e, res)
  )

  /** Mapping that is added in the global keys when the n is handled.
    *
    * In the original ContractStateMachine, handleNode first adds the node's key with this mapping to the global keys
    * and then process the node in the same way as ContractStateMachineAlt. In the simplified version, both operations
    * are separated which brings the need for this function.
    *
    * @see mapping n, in the latex document
    */
  @pure
  @opaque
  def nodeActionKeyMapping(n: Node.Action): KeyMapping = {
    n match {
      case create: Node.Create => KeyInactive
      case fetch: Node.Fetch => KeyActive(fetch.coid)
      case lookup: Node.LookupByKey => lookup.result
      case exe: Node.Exercise => KeyActive(exe.targetCoid)
    }
  }

  /** Extension of State.beginRollback() method to a union type argument. If e is defined calls beginRollback otherwise
    * returns the same error.
    */
  @pure
  @opaque
  def beginRollback[T](e: Either[T, State]): Either[T, State] = {
    sameErrorMap(e, s => s.beginRollback())
    val res = e.map(s => s.beginRollback())
    unfold(sameGlobalKeys(e, res))
    unfold(sameLocallyCreated(e, res))
    unfold(sameConsumed(e, res))
    e match {
      case Left(t) => Trivial()
      case Right(s) =>
        unfold(s.beginRollback())
        unfold(sameGlobalKeys(s, res))
        unfold(sameLocallyCreated(s, res))
        unfold(sameConsumed(s, res))
    }
    res
  }.ensuring((res: Either[T, State]) =>
    sameGlobalKeys[T, T](e, res) &&
      sameLocallyCreated(e, res) &&
      sameConsumed(e, res) &&
      sameError(e, res)
  )

  /** Extension of State.endRollback() method to a union type argument. If e is defined calls endRollback otherwise
    * returns the same error.
    *
    * Due to a bug in Stainless, writing the properties in the enduring clause of endRollback makes the JVM crash.
    * Therefore, all of these are grouped in a different function endRollbackProp that often needs to be called
    * separately when using endRollback with a union type argument.
    */
  @pure
  @opaque
  def endRollback(e: Either[KeyInputError, State]): Either[KeyInputError, State] = {
    e match {
      case Left(t) => Left[KeyInputError, State](t)
      case Right(s) => s.endRollback()
    }
  }

  @pure @opaque
  def endRollbackProp(e: Either[KeyInputError, State]): Unit = {
    unfold(endRollback(e))
    unfold(sameLocallyCreated(e, endRollback(e)))
    unfold(sameConsumed(e, endRollback(e)))
    unfold(sameGlobalKeys(e, endRollback(e)))
    unfold(propagatesError(e, endRollback(e)))
  }.ensuring(
    sameLocallyCreated(e, endRollback(e)) &&
      sameConsumed(e, endRollback(e)) &&
      sameGlobalKeys(e, endRollback(e)) &&
      propagatesError(e, endRollback(e))
  )

  @pure
  @opaque
  def advanceIsDefined(init: State, substate: State): Unit = {
    require(!substate.withinRollbackScope)
  }.ensuring(
    init.advance(substate).isRight == (
      substate.globalKeys.keySet.forall(k =>
        init.activeKeys.get(k).forall(m => Some(m) == substate.globalKeys.get(k))
      )
        &&
          !substate.locallyCreated.exists(init.locallyCreated.union(init.inputContractIds).contains)
    )
  )

}
