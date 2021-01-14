// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.collection.immutable.Map
import scalaz.Liskov, Liskov.<~<

sealed abstract class NonEmptyColl {
  type NonEmpty[+A]
  type NonEmptyF[F[_], A] <: NonEmpty[F[A]]
  private[scalautil] def substF[T[_[_]], F[_]](tf: T[F]): T[NonEmptyF[F, *]]
  def subtype[A]: NonEmpty[A] <~< A
  def equiv[F[_], A]: NonEmpty[A] === NonEmptyF[F[_], A]
}

object NonEmptyColl extends NonEmptyCollInstances {
  private[daml] object Instance extends NonEmptyColl {
    type NonEmpty[A] = A
    type NonEmptyF[F[_], A] = F[A]
    override def subtype[A] = Liskov.refl
  }

  implicit final class ReshapeOps[F[_], A](private val nfa: NonEmpty[F[A]]) extends AnyVal {
    def toF: NonEmptyF[F, A] = NonEmpty.equiv[F, A](nfa)
  }

  implicit final class MapOps[K, V](private val self: NonEmpty[Map[K, V]]) extends AnyVal {}

  implicit def traverse[F[_]](implicit F: Traverse[F]): Traverse[NonEmptyF[F, *]] =
    NonEmpty.subst(F)
}

sealed abstract class NonEmptyCollInstances {
  implicit def widen[A](na: NonEmpty[A]): A = NonEmpty.subtype[A](na)
  implicit def widenF[F[+_], A](na: F[NonEmpty[A]]): F[A] = Liskov.co(NonEmpty.subtype)(na)
}
