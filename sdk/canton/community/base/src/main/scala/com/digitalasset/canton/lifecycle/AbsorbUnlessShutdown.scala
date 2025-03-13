// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.traverse.*

import scala.concurrent.ExecutionContext

/** Type class for functors that embed the [[UnlessShutdown]] effect. Allows to merge an explicit
  * [[UnlessShutdown]] layer into the functor itself.
  */
trait AbsorbUnlessShutdown[F[_]] {

  /** Absorbs an inner [[UnlessShutdown]] into the functor.
    *
    * A special case of [[mapUS]] that does not compose.
    */
  def absorbInner[A](fa: F[UnlessShutdown[A]]): F[A] = mapUS(fa)(identity)

  /** Absorbs an outer [[UnlessShutdown]] into the functor.
    */
  def absorbOuter[A](a: UnlessShutdown[F[A]]): F[A] = a.onShutdown(pureAborted)

  /** Maps the given function over the functor, embedding the [[UnlessShutdown]] effect.
    *
    * This operation composes.
    */
  def mapUS[A, B](fa: F[A])(f: A => UnlessShutdown[B]): F[B]

  /** Returns a computation that always returns [[UnlessShutdown.AbortedDueToShutdown]].
    */
  def pureAborted[A]: F[A]

}

object AbsorbUnlessShutdown {
  def apply[F[_]](implicit instance: AbsorbUnlessShutdown[F]): AbsorbUnlessShutdown[F] = instance

  implicit val unlessShutdownAbsorbUnlessShutdown: AbsorbUnlessShutdown[UnlessShutdown] =
    new AbsorbUnlessShutdown[UnlessShutdown] {
      override def mapUS[A, B](fa: UnlessShutdown[A])(
          f: A => UnlessShutdown[B]
      ): UnlessShutdown[B] = fa.flatMap(f)

      override def absorbInner[A](
          fa: UnlessShutdown[UnlessShutdown[A]]
      ): UnlessShutdown[A] = fa.flatten

      override def pureAborted[A]: UnlessShutdown[A] = UnlessShutdown.AbortedDueToShutdown
    }

  implicit def futureUnlessShutdownAbsorbUnlessShutdown(implicit
      ec: ExecutionContext
  ): AbsorbUnlessShutdown[FutureUnlessShutdown] =
    new AbsorbUnlessShutdown[FutureUnlessShutdown] {
      override def mapUS[A, B](fa: FutureUnlessShutdown[A])(
          f: A => UnlessShutdown[B]
      ): FutureUnlessShutdown[B] = fa.subflatMap(f)

      override def pureAborted[A]: FutureUnlessShutdown[A] =
        FutureUnlessShutdown.abortedDueToShutdown
    }

  implicit def eitherTAbsorbUnlessShutdown[F[_], L](implicit
      absorb: AbsorbUnlessShutdown[F]
  ): AbsorbUnlessShutdown[EitherT[F, L, *]] = new AbsorbUnlessShutdown[EitherT[F, L, *]] {
    override def mapUS[A, B](fa: EitherT[F, L, A])(f: A => UnlessShutdown[B]): EitherT[F, L, B] =
      EitherT(absorb.mapUS(fa.value)(_.traverse(f)))

    override def pureAborted[A]: EitherT[F, L, A] = EitherT(absorb.pureAborted)
  }

  implicit def optionTAbsorbUnlessShutdown[F[_], L](implicit
      absorb: AbsorbUnlessShutdown[F]
  ): AbsorbUnlessShutdown[OptionT[F, *]] = new AbsorbUnlessShutdown[OptionT[F, *]] {
    override def mapUS[A, B](fa: OptionT[F, A])(f: A => UnlessShutdown[B]): OptionT[F, B] =
      OptionT(absorb.mapUS(fa.value)(_.traverse(f)))

    override def pureAborted[A]: OptionT[F, A] = OptionT(absorb.pureAborted)
  }
}
