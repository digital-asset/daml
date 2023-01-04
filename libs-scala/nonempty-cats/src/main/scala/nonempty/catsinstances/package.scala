// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import cats.{Functor, Monoid, Semigroup, Traverse}

package object catsinstances extends catsinstances.impl.CatsInstancesLow {
  implicit def `cats nonempty traverse`[F[_]](implicit F: Traverse[F]): Traverse[NonEmptyF[F, *]] =
    NonEmptyColl.Instance substF F

  implicit def `cats nonempty semigroup`[A](implicit A: Monoid[A]): Semigroup[NonEmpty[A]] =
    NonEmptyColl.Instance.subst[Lambda[k[_] => Semigroup[k[A]]]](A)

  implicit def `cats nonempty functor`[F[_]](implicit F: Functor[F]): Functor[NonEmptyF[F, *]] =
    NonEmptyColl.Instance substF F
}
