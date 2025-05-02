// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.Applicative
import cats.data.{EitherT, Nested, OptionT}
import com.digitalasset.canton.util.CheckedT

/** Type class for effect types that embed the [[UnlessShutdown]] effect. Allows to merge an
  * explicit [[UnlessShutdown]] wrapping into the effect type itself.
  */
trait CanAbortDueToShutdown[F[_]] {

  /** Absorbs an outer [[UnlessShutdown]] into the effect type.
    */
  def absorbOuter[A](a: UnlessShutdown[F[A]]): F[A] = a.onShutdown(abort)

  /** Returns a computation that always returns [[UnlessShutdown.AbortedDueToShutdown]].
    */
  def abort[A]: F[A]
}

object CanAbortDueToShutdown extends CanAbortDueToShutdown0 {
  def apply[F[_]](implicit instance: CanAbortDueToShutdown[F]): CanAbortDueToShutdown[F] = instance

  implicit val unlessShutdownAbsorbUnlessShutdown: CanAbortDueToShutdown[UnlessShutdown] =
    new CanAbortDueToShutdown[UnlessShutdown] {
      override def abort[A]: UnlessShutdown[A] = UnlessShutdown.AbortedDueToShutdown
    }

  implicit val futureUnlessShutdownAbsorbUnlessShutdown
      : CanAbortDueToShutdown[FutureUnlessShutdown] =
    new CanAbortDueToShutdown[FutureUnlessShutdown] {
      override def abort[A]: FutureUnlessShutdown[A] =
        FutureUnlessShutdown.abortedDueToShutdown
    }

  implicit def eitherTAbsorbUnlessShutdown[F[_], L](implicit
      F: CanAbortDueToShutdown[F]
  ): CanAbortDueToShutdown[EitherT[F, L, *]] = new CanAbortDueToShutdown[EitherT[F, L, *]] {
    override def abort[A]: EitherT[F, L, A] = EitherT(F.abort)
  }

  implicit def optionTAbsorbUnlessShutdown[F[_], L](implicit
      F: CanAbortDueToShutdown[F]
  ): CanAbortDueToShutdown[OptionT[F, *]] = new CanAbortDueToShutdown[OptionT[F, *]] {
    override def abort[A]: OptionT[F, A] = OptionT(F.abort)
  }

  implicit def checkedTAbsorbUnlessShutdown[F[_], A, N](implicit
      F: CanAbortDueToShutdown[F]
  ): CanAbortDueToShutdown[CheckedT[F, A, N, *]] = new CanAbortDueToShutdown[CheckedT[F, A, N, *]] {
    override def abort[X]: CheckedT[F, A, N, X] = CheckedT(F.abort)
  }

  implicit def nestedAbsorbUnlessShutdownOuter[F[_], G[_]](implicit
      F: CanAbortDueToShutdown[F]
  ): CanAbortDueToShutdown[Nested[F, G, *]] = new CanAbortDueToShutdown[Nested[F, G, *]] {
    override def abort[A]: Nested[F, G, A] = Nested(F.abort)
  }
}

trait CanAbortDueToShutdown0 {
  implicit def nestedAbsorbUnlessShutdownInner[F[_], G[_]](implicit
      F: Applicative[F],
      G: CanAbortDueToShutdown[G],
  ): CanAbortDueToShutdown[Nested[F, G, *]] = new CanAbortDueToShutdown[Nested[F, G, *]] {
    override def abort[A]: Nested[F, G, A] = Nested(F.pure(G.abort))
  }
}
