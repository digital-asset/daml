// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}

import scala.annotation.implicitNotFound
import scala.util.Try

@implicitNotFound(
  "Cannot find an implicit retry.Success for the given type of Future, either require one yourself or import retry.Success._"
)
class Success[-T](val predicate: T => Boolean) {
  def or[TT <: T](that: Success[TT]): Success[TT] =
    Success[TT](v => predicate(v) || that.predicate(v))
  def or[TT <: T](that: => Boolean): Success[TT] = or(Success[TT](_ => that))
  def and[TT <: T](that: Success[TT]): Success[TT] =
    Success[TT](v => predicate(v) && that.predicate(v))
  def and[TT <: T](that: => Boolean): Success[TT] = and(Success[TT](_ => that))
}

object Success {
  implicit def either[A, B]: Success[Either[A, B]] =
    Success(_.isRight)
  implicit def option[A]: Success[Option[A]] =
    Success(!_.isEmpty)
  implicit def tried[A]: Success[Try[A]] =
    Success(_.isSuccess)
  implicit def boolean: Success[Boolean] = Success(identity)

  val always = Success(Function.const(true))
  val never = Success(Function.const(false))

  def onShutdown[T](implicit success: Success[T]): Success[UnlessShutdown[T]] =
    Success[UnlessShutdown[T]] {
      case Outcome(result) => success.predicate(result)
      case AbortedDueToShutdown => true
    }

  def apply[T](pred: T => Boolean) = new Success(pred)
}
