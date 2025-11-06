// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.either.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.reflect.ClassTag

trait StateMachine[S] { self: NamedLogging =>

  protected val stateRef: AtomicReference[S]

  // Like `transition` but supports 2 expected states instead of one
  // The use of Either here is not right-biased and is just a wrapper for 2 possible states
  protected def transitionEither[E, T <: S, U <: S](
      getExpectedState: S => Option[Either[T, U]],
      newState: S,
      unexpectedStateFn: S => E,
  ): Either[E, Either[T, U]] = {
    val prevState = stateRef.getAndUpdate { currentState =>
      if (getExpectedState(currentState).isDefined)
        newState
      else currentState
    }

    getExpectedState(prevState).toRight(unexpectedStateFn(prevState))
  }

  protected def transition[E, T <: S](
      getExpectedState: S => Option[T],
      newState: S,
      unexpectedStateFn: S => E,
  ): Either[E, T] = {
    val prevState = stateRef.getAndUpdate { currentState =>
      if (getExpectedState(currentState).isDefined)
        newState
      else currentState
    }

    getExpectedState(prevState).toRight(unexpectedStateFn(prevState))
  }

  protected def transition[E](
      expectedState: S,
      newState: S,
      error: S => E,
  ): Either[E, Unit] =
    transition(s => Some(s).filter(_ == expectedState), newState, error).map(_ => ())

  protected def transitionOrFail[T <: S](
      expectedState: String,
      getExpectedState: S => Option[T],
      newState: S,
  )(implicit traceContext: TraceContext): T =
    transition(
      getExpectedState,
      newState,
      errorState =>
        s"Failed to transition from $expectedState to $newState: current state is $errorState",
    ).tapRight(_ => logger.debug(s"Transitioned from $expectedState to $newState"))
      .valueOr(ErrorUtil.invalidState(_))

  protected def transitionOrFail(expectedState: S, newState: S)(implicit
      traceContext: TraceContext
  ): Unit =
    transition(
      expectedState,
      newState,
      errorState =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Failed to transition from $expectedState to $newState: current state is $errorState"
          )
        ),
    ).discard

  // Match on the expected state type but not the value
  protected def transitionOrFail[S1 <: S: ClassTag](newState: S)(implicit
      traceContext: TraceContext
  ): Unit = {
    val prevState = stateRef.getAndUpdate {
      case _: S1 => newState
      case currentState => currentState
    }

    prevState match {
      case _: S1 => ()
      case unexpectedState =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Failed to transition to $newState: current state is $unexpectedState"
          )
        )
    }
  }

}
