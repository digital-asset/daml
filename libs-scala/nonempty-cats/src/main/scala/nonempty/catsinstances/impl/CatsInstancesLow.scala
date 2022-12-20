// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty
package catsinstances.impl

import com.daml.scalautil.ImplicitPreference
import cats.{Applicative, Eval, Foldable, Reducible, Traverse}

abstract class CatsInstancesLow extends CatsInstancesLow1 {
  implicit def `cats nonempty foldable`[F[_]](implicit
      F: Foldable[F]
  ): Foldable[NonEmptyF[F, *]] with ImplicitPreference[Nothing] =
    ImplicitPreference[Foldable[NonEmptyF[F, *]], Nothing](NonEmptyColl.Instance substF F)
}

abstract class CatsInstancesLow1 {
  implicit def `cats nonempty reducible`[F[_]](implicit
      F0: Foldable[F]
  ): Reducible[NonEmptyF[F, *]] =
    new NonEmptyReducibleFromEmpty[F] {
      override val F = F0
    }
}

abstract class CatsInstancesLow2 {
  implicit def `cats nonempty traverse`[F[_]](implicit
      F0: Foldable[F]
  ): Traverse[NonEmptyF[F, *]] =
    new Traverse[NonEmptyF[F, *]] with NonEmptyReducibleFromEmpty[F] {
      override val F = F0

      override def traverse[G[_]: Applicative, A, B](fa: NonEmptyF[F, A])(f: A => G[B]) =
        sys.error("TODO SC #11155")
    }
}

private[impl] sealed trait NonEmptyReducibleFromEmpty[F[_]] extends Reducible[NonEmptyF[F, *]] {
  protected[this] val F: Foldable[F]

  private[impl] def errOnEmpty[A](x: NonEmptyF[F, A]): Nothing =
    throw new IllegalArgumentException(
      s"empty structure coerced to non-empty: $x: ${x.getClass.getSimpleName}"
    )

  override final def reduceLeftTo[A, B](fa: NonEmptyF[F, A])(f: A => B)(g: (B, A) => B): B =
    F.reduceLeftToOption(fa)(f)(g).getOrElse(errOnEmpty(fa))

  override final def reduceRightTo[A, B](fa: NonEmptyF[F, A])(f: A => B)(
      g: (A, Eval[B]) => Eval[B]
  ): Eval[B] =
    F.reduceRightToOption(fa)(f)(g).map(_.getOrElse(errOnEmpty(fa)))

  override final def foldLeft[A, B](fa: NonEmptyF[F, A], b: B)(f: (B, A) => B): B =
    F.foldLeft(fa, b)(f)

  override final def foldRight[A, B](fa: NonEmptyF[F, A], lb: Eval[B])(
      f: (A, Eval[B]) => Eval[B]
  ): Eval[B] =
    F.foldRight(fa, lb)(f)
}
