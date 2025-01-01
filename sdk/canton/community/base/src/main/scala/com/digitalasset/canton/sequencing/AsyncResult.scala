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
final case class AsyncResult[T](unwrap: FutureUnlessShutdown[T]) {
  def andThenF(f: Unit => FutureUnlessShutdown[Unit])(implicit
      ec: ExecutionContext
  ): AsyncResult[T] =
    AsyncResult(unwrap.flatMap(res => f(()).map(_ => res)))

  def transform(f: Try[UnlessShutdown[T]] => Try[UnlessShutdown[T]])(implicit
      ec: ExecutionContext
  ): AsyncResult[T] =
    AsyncResult(unwrap.transform(f))

  /** Analog to [[com.digitalasset.canton.util.Thereafter.thereafter]]
    * We do not provide a [[com.digitalasset.canton.util.Thereafter.thereafter]] instance
    * because [[AsyncResult]] doesn't take a type argument.
    */
  def thereafter(f: Try[UnlessShutdown[T]] => Unit)(implicit
      ec: ExecutionContext
  ): AsyncResult[T] =
    transform { res =>
      f(res)
      res
    }

  def map[U](f: T => U)(implicit
      ec: ExecutionContext
  ): AsyncResult[U] = AsyncResult(unwrap.map(f))

  def flatMapFUS[U](f: T => FutureUnlessShutdown[U])(implicit
      ec: ExecutionContext
  ): AsyncResult[U] = AsyncResult(unwrap.flatMap(f))
}

object AsyncResult {

  /** No asynchronous processing. */
  val immediate: AsyncResult[Unit] = AsyncResult(FutureUnlessShutdown.unit)

  def pure[T](t: T): AsyncResult[T] = AsyncResult(FutureUnlessShutdown.pure(t))

  implicit def monoidAsyncResult(implicit ec: ExecutionContext): Monoid[AsyncResult[Unit]] =
    new Monoid[AsyncResult[Unit]] {
      override def empty: AsyncResult[Unit] = immediate
      override def combine(x: AsyncResult[Unit], y: AsyncResult[Unit]): AsyncResult[Unit] =
        AsyncResult(Monoid[FutureUnlessShutdown[Unit]].combine(x.unwrap, y.unwrap))
    }
}
