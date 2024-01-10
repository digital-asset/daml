// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.laws.{IsEq, IsEqArrow, TraverseLaws}
import cats.{Applicative, Monoid}

trait SingletonTraverseLaws[F[_], C] extends TraverseLaws[F] {
  implicit override def F: SingletonTraverse.Aux[F, C]

  def sizeAtMost1[A](fa: F[A]): IsEq[Boolean] =
    (F.size(fa) <= 1) <-> true

  def traverseSingletonConsistency[G[_], A, B](fa: F[A], f: A => G[B])(implicit
      G: Applicative[G]
  ): IsEq[G[F[B]]] = {
    val traverseSingleton = F.traverseSingleton(fa)((_, a) => f(a))
    val traverse = F.traverse(fa)(f)
    traverseSingleton <-> traverse
  }

  def contextConsistency[A](fa: F[A]): IsEq[Option[C]] = {
    implicit val keepFirst: Monoid[Option[C]] = new Monoid[Option[C]] {
      override def empty: Option[C] = None
      override def combine(x: Option[C], y: Option[C]): Option[C] = x.orElse(y)
    }

    val (firstContext, fa2) = F.traverseSingleton(fa)((c, a) => (Option(c), (a, a)))
    val (secondContext, _) = F.traverseSingleton(fa2)((c, aa) => (Option(c), (aa, aa)))

    firstContext <-> secondContext
  }
}

object SingletonTraverseLaws {
  def apply[F[_]](implicit FF: SingletonTraverse[F]): SingletonTraverseLaws[F, FF.Context] =
    new SingletonTraverseLaws[F, FF.Context] { def F: SingletonTraverse.Aux[F, FF.Context] = FF }
}
