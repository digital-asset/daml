// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.Monoid
import com.digitalasset.canton.DoNotDiscardLikeFuture
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}

import scala.concurrent.ExecutionContext
import scala.util.Try

/** The asynchronous part of processing an event (or of a stage of its processing). */
@DoNotDiscardLikeFuture
final case class AsyncResult(unwrap: FutureUnlessShutdown[Unit]) {
  def andThenF(
      f: Unit => FutureUnlessShutdown[Unit]
  )(implicit ec: ExecutionContext): AsyncResult = {
    AsyncResult(unwrap.flatMap(f))
  }

  def transform(f: Try[UnlessShutdown[Unit]] => Try[UnlessShutdown[Unit]])(implicit
      ec: ExecutionContext
  ): AsyncResult =
    AsyncResult(unwrap.transform(f))

  /** Analog to [[com.digitalasset.canton.util.Thereafter.thereafter]]
    * We do not provide a [[com.digitalasset.canton.util.Thereafter.thereafter]] instance
    * because [[AsyncResult]] doesn't take a type argument.
    */
  def thereafter(f: Try[UnlessShutdown[Unit]] => Unit)(implicit ec: ExecutionContext): AsyncResult =
    transform { res =>
      f(res)
      res
    }
}

object AsyncResult {

  /** No asynchronous processing. */
  val immediate: AsyncResult = AsyncResult(FutureUnlessShutdown.unit)

  implicit def monoidAsyncResult(implicit ec: ExecutionContext): Monoid[AsyncResult] =
    new Monoid[AsyncResult] {
      override def empty: AsyncResult = immediate
      override def combine(x: AsyncResult, y: AsyncResult): AsyncResult = {
        AsyncResult(Monoid[FutureUnlessShutdown[Unit]].combine(x.unwrap, y.unwrap))
      }
    }
}
