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
import utils.TransactionErrors.{DuplicateContractKey, InconsistentContractKey, KeyInputError}
import utils._

import ContractStateMachine._

/** Having pattern matching inside ensuring or require clauses generates a lot of verification conditions
  * and considerably slows down stainless. In order to avoid that, every pattern match in a postcondition
  * or precondition should, if possible, be enclosed in an opaque function.
  *
  * This files gather all field equalities between a [[State]] and a Either[T, State], or between an Either[T, State]
  * and an Either[U, State], where T and U are generic type.
  * In practice T will be either [[KeyInputError]], [[InconsistentContractKey]] or [[DuplicateContractKey]]. Having a
  * generic type avoids Stainless to spend time on what kind of type that is, making it sort of 'opaque'.
  *
  * It also describes error propagation, which happens when processing a transaction. The exact definitions of error
  * propagation are described lower in the file.
  */
object CSMEitherDef {

  /** Checks state equality. Equivalent to checking equality for every field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1 is equal to s2 if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameState[T](s1: State, s2: Either[T, State]): Boolean = {
    unfold(sameStack(s1, s2))
    unfold(sameGlobalKeys(s1, s2))
    unfold(sameActiveState(s1, s2))
    unfold(sameLocallyCreated(s1, s2))
    unfold(sameInputContractIds(s1, s2))
    unfold(sameConsumed(s1, s2))

    s2.forall((s: State) => s1 == s)
  }.ensuring(
    _ == (sameStack(s1, s2) && sameGlobalKeys(s1, s2) && sameActiveState(s1, s2) &&
      sameLocallyCreated(s1, s2) && sameInputContractIds(s1, s2) && sameConsumed(s1, s2))
  )

  /** Checks state equality. Equivalent to checking equality for every field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1 is equal to s2 when both are well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameState[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    unfold(sameStack(s1, s2))
    unfold(sameGlobalKeys(s1, s2))
    unfold(sameActiveState(s1, s2))
    unfold(sameLocallyCreated(s1, s2))
    unfold(sameInputContractIds(s1, s2))
    unfold(sameConsumed(s1, s2))

    s1.forall((s: State) => sameState(s, s2))
  }.ensuring(
    _ == (sameStack(s1, s2) && sameGlobalKeys(s1, s2) && sameActiveState(
      s1,
      s2,
    ) && sameLocallyCreated(s1, s2) && sameInputContractIds(s1, s2) && sameConsumed(s1, s2))
  )

  /** Checks that two states have the same [[State.locallyCreated]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.locallyCreated is equal to s2.locallyCreated if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameLocallyCreated[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.locallyCreated == s.locallyCreated)
  }

  /** Checks that two states have the same [[State.locallyCreated]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.locallyCreated is equal to s2.locallyCreated when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameLocallyCreated[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameLocallyCreated(s, s2))
  }

  /** Checks that two states have the same [[State.inputContractIds]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.inputContractIds is equal to s2.inputContractIds if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameInputContractIds[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.inputContractIds == s.inputContractIds)
  }

  /** Checks that two states have the same [[State.inputContractIds]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.inputContractIds is equal to s2.inputContractIds when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameInputContractIds[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameInputContractIds(s, s2))
  }

  /** Checks that two states have the same [[State.consumed]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.consumed is equal to s2.consumed if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameConsumed[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.consumed == s.consumed)
  }

  /** Checks that two states have the same [[State.consumed]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.consumed is equal to s2.locallyCreated when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameConsumed[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameConsumed(s, s2))
  }

  /** Checks that two states have the same [[State.activeState]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState is equal to s2.activeState if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameActiveState[T](s1: State, s2: Either[T, State]): Boolean = {
    unfold(sameLocallyCreatedThisTimeline(s1, s2))
    unfold(sameConsumedBy(s1, s2))
    unfold(sameLocalKeys(s1, s2))
    s2.forall((s: State) => s1.activeState == s.activeState)
  }.ensuring(
    _ == (sameLocallyCreatedThisTimeline(s1, s2) && sameConsumedBy(s1, s2) && sameLocalKeys(s1, s2))
  )

  /** Checks that two states have the same [[State.activeState]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState is equal to s2.activeState when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameActiveState[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    unfold(sameLocallyCreatedThisTimeline(s1, s2))
    unfold(sameConsumedBy(s1, s2))
    unfold(sameLocalKeys(s1, s2))
    s1.forall((s: State) => sameActiveState(s, s2))
  }.ensuring(
    _ == (sameLocallyCreatedThisTimeline(s1, s2) && sameConsumedBy(s1, s2) && sameLocalKeys(s1, s2))
  )

  /** Checks that two states have the same [[State.activeState.localKeys]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.localKeys is equal to s2.activeState.localKeys if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameLocalKeys[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.activeState.localKeys == s.activeState.localKeys)
  }

  /** Checks that two states have the same [[State.activeState.localKeys]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.localKeys is equal to s2.activeState.localKeys when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameLocalKeys[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameLocalKeys(s, s2))
  }

  /** Checks that two states have the same [[State.activeState.locallyCreatedThisTimeline]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.locallyCreatedThisTimeline is equal to s2.activeState.locallyCreatedThisTimeline
    *         if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameLocallyCreatedThisTimeline[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) =>
      s1.activeState.locallyCreatedThisTimeline == s.activeState.locallyCreatedThisTimeline
    )
  }

  /** Checks that two states have the same [[State.activeState.locallyCreatedThisTimeline]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.locallyCreatedThisTimeline is equal to s2.activeState.locallyCreatedThisTimeline
    *         when both are well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameLocallyCreatedThisTimeline[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameLocallyCreatedThisTimeline(s, s2))
  }

  /** Checks that two states have the same [[State.activeState.consumedBy]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.consumedBy is equal to s2.activeState.consumedBy if s2 is well-defined, otherwise
    *         returns true
    */
  @pure
  @opaque
  def sameConsumedBy[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.activeState.consumedBy == s.activeState.consumedBy)
  }

  /** Checks that two states have the same [[State.activeState.consumedBy]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.consumedBy is equal to s2.activeState.consumedBy when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameConsumedBy[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameConsumedBy(s, s2))
  }

  /** Checks that two states have the same [[State.globalKeys]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.globalKeys is equal to s2.globalKeys if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameGlobalKeys[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.globalKeys == s.globalKeys)
  }

  /** Checks that two states have the same [[State.globalKeys]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.globalKeys is equal to s2.globalKeys when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameGlobalKeys[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameGlobalKeys(s, s2))
  }

  /** Checks that two states have the same [[State.rollbackStack]] field.
    *
    * @param s1 A well-defined state
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.rollbackStack is equal to s2.rollbackStack if s2 is well-defined, otherwise returns true
    */
  @pure
  @opaque
  def sameStack[T](s1: State, s2: Either[T, State]): Boolean = {
    s2.forall((s: State) => s1.rollbackStack == s.rollbackStack)
  }

  /** Checks that two states have the same [[State.rollbackStack]] field.
    *
    * @param s1 A state that can be either well-defined or erroneous
    * @param s2 A state that can be either well-defined or erroneous
    * @return Whether s1.activeState.rollbackStack is equal to s2.rollbackStack when both are well-defined,
    *         otherwise returns true
    */
  @pure
  @opaque
  def sameStack[U, T](s1: Either[U, State], s2: Either[T, State]): Boolean = {
    s1.forall((s: State) => sameStack(s, s2))
  }

  /** Various definitions of error propagation
    *
    * @param s1
    * The state or error before the operation
    *
    * @param s2
    * The state or error after the operation
    *
    * 1. PropagatesError: if s1 is an error state then s2 will be an error as well. Both errors
    * do not necessarily need to be the same.
    *
    * 2. PropagatesBothError: s1 is an error state if and only if s2 is an error state as well,
    * although both errors do not necessarily need to be the same
    *
    * 3. PropagatesSameError: if s1 is an error state then s1 == s2
    *
    * 4. SameError: if s1 or s2 is an error state then s1 == s2
    *
    * Implications:
    *
    *  SameError ==> PropagatesSameError ==> PropagatesError
    *  SameError ==> PropagatesBothError ==> PropagatesError
    */

  /** Checks that if a state is erroneous than the other one is as well.
    *
    * @see above for further details
    */
  @pure
  @opaque
  def propagatesError[U, T, V](s1: Either[U, V], s2: Either[T, V]): Boolean = {
    s1.isLeft ==> s2.isLeft
  }

  /** Checks that a state is erroneous if and only if the other one is as well.
    *
    * @see above for further details
    */
  @pure
  def propagatesBothError[U, T, V](s1: Either[U, V], s2: Either[T, V]): Boolean = {
    propagatesError(s1, s2) && propagatesError(s2, s1)
  }

  /** Checks that if a state is erroneous then the other one is erroneous has well and outputs the same result.
    *
    * @see above for further details
    */
  @pure
  @opaque
  def propagatesSameError[T, V](s1: Either[T, V], s2: Either[T, V]): Boolean = {
    unfold(propagatesError(s1, s2))
    s1.isLeft ==> (s1 == s2)
  }.ensuring(_ ==> propagatesError(s1, s2))

  /** Checks that a state is erroneous if and only if the other one is erroneous. Moreover when this happens,
    * both errors are the same.
    *
    * @see above for further details
    */
  @pure
  def sameError[T, V](s1: Either[T, V], s2: Either[T, V]): Boolean = {
    propagatesSameError(s1, s2) && propagatesSameError(s2, s1)
  }.ensuring(_ ==> propagatesBothError(s1, s2))

}

object CSMEither {

  import CSMEitherDef._

  /** Field equality extension is always reflexive. In order to be transitivity, we need error propagation
    * on at least the second operation. In fact transitivity does not hold if we have s1 -> error -> s2
    * with s1.field =/= s2.field.
    */

  /** Reflexivity of [[State.rollbackStack]] field equality.
    */
  @pure
  @opaque
  def sameStackReflexivity[T](s: Either[T, State]): Unit = {
    unfold(sameStack(s, s))
    s match {
      case Left(_) => Trivial()
      case Right(s2) => unfold(sameStack(s2, s))
    }
  }.ensuring(sameStack(s, s))

  /** Transitivity of [[State.rollbackStack]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameStackTransitivity[V, W](s1: State, s2: Either[V, State], s3: Either[W, State]): Unit = {
    require(sameStack(s1, s2))
    require(sameStack(s2, s3))
    require(propagatesError(s2, s3))

    unfold(sameStack(s1, s2))
    unfold(sameStack(s2, s3))
    unfold(sameStack(s1, s3))
    unfold(propagatesError(s2, s3))

    s2 match {
      case Left(_) => Trivial()
      case Right(s) => unfold(sameStack(s, s3))
    }

  }.ensuring(sameStack(s1, s3))

  /** Transitivity of [[State.rollbackStack]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameStackTransitivity[U, V, W](
      s1: Either[U, State],
      s2: Either[V, State],
      s3: Either[W, State],
  ): Unit = {
    require(sameStack(s1, s2))
    require(sameStack(s2, s3))
    require(propagatesError(s2, s3))

    unfold(sameStack(s1, s2))
    unfold(sameStack(s1, s3))
    s1 match {
      case Left(_) => Trivial()
      case Right(s) => sameStackTransitivity(s, s2, s3)
    }
  }.ensuring(sameStack(s1, s3))

  /** Reflexivity of [[State.globaKeys]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameGlobalKeysReflexivity[T](s: Either[T, State]): Unit = {
    unfold(sameGlobalKeys(s, s))
    s match {
      case Left(_) => Trivial()
      case Right(s2) => unfold(sameGlobalKeys(s2, s))
    }
  }.ensuring(sameGlobalKeys(s, s))

  /** Transitivity of [[State.globalKeys]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameGlobalKeysTransitivity[V, W](
      s1: State,
      s2: Either[V, State],
      s3: Either[W, State],
  ): Unit = {
    require(sameGlobalKeys(s1, s2))
    require(sameGlobalKeys(s2, s3))
    require(propagatesError(s2, s3))

    unfold(sameGlobalKeys(s1, s2))
    unfold(sameGlobalKeys(s2, s3))
    unfold(sameGlobalKeys(s1, s3))
    unfold(propagatesError(s2, s3))

    s2 match {
      case Left(_) => Trivial()
      case Right(s) => unfold(sameGlobalKeys(s, s3))
    }

  }.ensuring(sameGlobalKeys(s1, s3))

  /** Transitivity of [[State.globalKeys]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameGlobalKeysTransitivity[U, V, W](
      s1: Either[U, State],
      s2: Either[V, State],
      s3: Either[W, State],
  ): Unit = {
    require(sameGlobalKeys(s1, s2))
    require(sameGlobalKeys(s2, s3))
    require(propagatesError(s2, s3))

    unfold(sameGlobalKeys(s1, s2))
    unfold(sameGlobalKeys(s1, s3))
    s1 match {
      case Left(_) => Trivial()
      case Right(s) => sameGlobalKeysTransitivity(s, s2, s3)
    }
  }.ensuring(sameGlobalKeys(s1, s3))

  /** Reflexivity of [[State.locallyCreated]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @opaque
  def sameLocallyCreatedReflexivity[T](s: Either[T, State]): Unit = {
    unfold(sameLocallyCreated(s, s))
    s match {
      case Left(_) => Trivial()
      case Right(s2) => unfold(sameLocallyCreated(s2, s))
    }
  }.ensuring(sameLocallyCreated(s, s))

  /** Transitivity of [[State.locallyCreated]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameLocallyCreatedTransitivity[V, W](
      s1: State,
      s2: Either[V, State],
      s3: Either[W, State],
  ): Unit = {
    require(sameLocallyCreated(s1, s2))
    require(sameLocallyCreated(s2, s3))
    require(propagatesError(s2, s3))

    unfold(sameLocallyCreated(s1, s2))
    unfold(sameLocallyCreated(s2, s3))
    unfold(sameLocallyCreated(s1, s3))
    unfold(propagatesError(s2, s3))

    s2 match {
      case Left(_) => Trivial()
      case Right(s) => unfold(sameLocallyCreated(s, s3))
    }

  }.ensuring(sameLocallyCreated(s1, s3))

  /** Reflexivity of [[State.consumed]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameConsumedReflexivity[T](s: Either[T, State]): Unit = {
    unfold(sameConsumed(s, s))
    s match {
      case Left(_) => Trivial()
      case Right(s2) => unfold(sameConsumed(s2, s))
    }
  }.ensuring(sameConsumed(s, s))

  /** Transitivity of [[State.consumed]] field equality. Holds only if error propagates between the
    * second and the third state.
    */
  @pure
  @opaque
  def sameConsumedTransitivity[V, W](
      s1: State,
      s2: Either[V, State],
      s3: Either[W, State],
  ): Unit = {
    require(sameConsumed(s1, s2))
    require(sameConsumed(s2, s3))
    require(propagatesError(s2, s3))

    unfold(sameConsumed(s1, s2))
    unfold(sameConsumed(s2, s3))
    unfold(sameConsumed(s1, s3))
    unfold(propagatesError(s2, s3))

    s2 match {
      case Left(_) => Trivial()
      case Right(s) => unfold(sameConsumed(s, s3))
    }

  }.ensuring(sameConsumed(s1, s3))

  /** Reflexivity of error propagation.
    */
  @pure
  @opaque
  def propagatesErrorReflexivity[T, V](s: Either[T, V]): Unit = {
    unfold(propagatesError(s, s))
  }.ensuring(propagatesError(s, s))

  /** Transitivity of error propagation.
    */
  @pure
  @opaque
  def propagatesErrorTransitivity[T, V](
      s1: Either[T, V],
      s2: Either[T, V],
      s3: Either[T, V],
  ): Unit = {
    require(propagatesError(s1, s2))
    require(propagatesError(s2, s3))
    unfold(propagatesError(s1, s2))
    unfold(propagatesError(s2, s3))
    unfold(propagatesError(s1, s3))
  }.ensuring(propagatesError(s1, s3))

  /** Reflexivity of error propagation.
    */
  @pure
  @opaque
  def propagatesSameErrorReflexivity[T, V](s: Either[T, V]): Unit = {
    unfold(propagatesSameError(s, s))
  }.ensuring(propagatesSameError(s, s))

  /** Transitivity of error propagation.
    */
  @pure
  @opaque
  def propagatesSameErrorTransitivity[T, V](
      s1: Either[T, V],
      s2: Either[T, V],
      s3: Either[T, V],
  ): Unit = {
    require(propagatesSameError(s1, s2))
    require(propagatesSameError(s2, s3))
    unfold(propagatesSameError(s1, s2))
    unfold(propagatesSameError(s2, s3))
    unfold(propagatesSameError(s1, s3))
  }.ensuring(propagatesSameError(s1, s3))

  /** Reflexivity of error propagation.
    */
  @pure
  @opaque
  def sameErrorReflexivity[T, V](s: Either[T, V]): Unit = {
    propagatesSameErrorReflexivity(s)
  }.ensuring(sameError(s, s))

  /** Transitivity of error propagation.
    */
  @pure
  @opaque
  def sameErrorTransitivity[T, V](s1: Either[T, V], s2: Either[T, V], s3: Either[T, V]): Unit = {
    require(sameError(s1, s2))
    require(sameError(s2, s3))
    propagatesSameErrorTransitivity(s1, s2, s3)
    propagatesSameErrorTransitivity(s3, s2, s1)
  }.ensuring(sameError(s1, s3))

  /** Right map propagates error in both directions.
    */
  @pure
  @opaque
  def propagatesSameErrorMap[T, V](e: Either[T, V], f: V => V): Unit = {
    unfold(propagatesSameError(e, e.map(f)))
    unfold(propagatesSameError(e.map(f), e))
  }.ensuring(propagatesSameError(e, e.map(f)) && propagatesSameError(e.map(f), e))

  /** Right map propagates error in both direction. Furthermore they are the same.
    */
  @pure
  @opaque
  def sameErrorMap[T, V](e: Either[T, V], f: V => V): Unit = {
    propagatesSameErrorMap(e, f)
  }.ensuring(sameError(e, e.map(f)))

  /** Left map propagates error in both directions. Furthermore the resulting state is equal to the input.
    */
  @pure
  @opaque
  def sameStateLeftProj[U, T](s: Either[U, State], f: U => T): Unit = {

    val lMap = s.left.map(f)

    unfold(sameState(s, lMap))
    unfold(propagatesError(s, lMap))
    unfold(propagatesError(lMap, s))
    s match {
      case Left(_) => Trivial()
      case Right(state) => unfold(sameState(state, lMap))
    }
  }.ensuring(
    sameState(s, s.left.map(f)) &&
      propagatesBothError(s, s.left.map(f))
  )

}
