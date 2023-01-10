// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty
package catsinstances.impl

import com.daml.scalautil.ImplicitPreference
import cats.{Eval, Foldable, Reducible}

abstract class CatsInstancesLow extends CatsInstancesLow1 {
  implicit def `cats nonempty foldable`[F[_]](implicit
      F: Foldable[F]
  ): Foldable[NonEmptyF[F, *]] with ImplicitPreference[Nothing] =
    ImplicitPreference[Foldable[NonEmptyF[F, *]], Nothing](NonEmptyColl.Instance substF F)
}

abstract class CatsInstancesLow1 {
  implicit def `cats nonempty reducible`[F[_]](implicit
      F: Foldable[F]
  ): Reducible[NonEmptyF[F, *]] =
    new Reducible[NonEmptyF[F, *]] {
      private def errOnEmpty[A](x: NonEmptyF[F, A]): Nothing =
        throw new IllegalArgumentException(
          s"empty structure coerced to non-empty: $x: ${x.getClass.getSimpleName}"
        )

      override def reduceLeftTo[A, B](fa: NonEmptyF[F, A])(f: A => B)(g: (B, A) => B): B =
        F.reduceLeftToOption(fa)(f)(g).getOrElse(errOnEmpty(fa))

      override def reduceRightTo[A, B](fa: NonEmptyF[F, A])(f: A => B)(
          g: (A, Eval[B]) => Eval[B]
      ): Eval[B] =
        F.reduceRightToOption(fa)(f)(g).map(_.getOrElse(errOnEmpty(fa)))

      override def foldLeft[A, B](fa: NonEmptyF[F, A], b: B)(f: (B, A) => B): B =
        F.foldLeft(fa, b)(f)

      override def foldRight[A, B](fa: NonEmptyF[F, A], lb: Eval[B])(
          f: (A, Eval[B]) => Eval[B]
      ): Eval[B] =
        F.foldRight(fa, lb)(f)
    }
}
