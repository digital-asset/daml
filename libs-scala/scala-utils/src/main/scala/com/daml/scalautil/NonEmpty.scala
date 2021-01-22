// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.collection.{immutable => sci}, sci.Map, sci.Set
import scalaz.{Foldable, Traverse}
import scalaz.Leibniz, Leibniz.===
import scalaz.Liskov, Liskov.<~<

sealed abstract class NonEmptyColl {
  type NonEmpty[+A]
  type NonEmptyF[F[_], A] <: NonEmpty[F[A]]
  private[scalautil] def substF[T[_[_]], F[_]](tf: T[F]): T[NonEmptyF[F, *]]
  def subtype[A]: NonEmpty[A] <~< A
  def equiv[F[_], A]: NonEmpty[F[A]] === NonEmptyF[F, A]

  def apply[Self](self: Self with sci.Iterable[_]): Option[NonEmpty[Self]]
  private[scalautil] def unsafeNarrow[Self](self: Self with sci.Iterable[_]): NonEmpty[Self]
}

object NonEmptyColl extends NonEmptyCollInstances {
  private[scalautil] object Instance extends NonEmptyColl {
    type NonEmpty[+A] = A
    type NonEmptyF[F[_], A] = F[A]
    private[scalautil] override def substF[T[_[_]], F[_]](tf: T[F]) = tf
    override def subtype[A] = Liskov.refl[A]
    override def equiv[F[_], A] = Leibniz.refl

    override def apply[Self](self: Self with sci.Iterable[_]) =
      if (self.nonEmpty) Some(self) else None
    private[scalautil] override def unsafeNarrow[Self](self: Self with sci.Iterable[_]) = self
  }

  implicit final class ReshapeOps[F[_], A](private val nfa: NonEmpty[F[A]]) extends AnyVal {
    def toF: NonEmptyF[F, A] = NonEmpty.equiv[F, A](nfa)
  }

  // many of these Map and Set operations can return more specific map and set types;
  // however, the way to do that is incompatible between Scala 2.12 and 2.13.
  // So we won't do it at least until 2.12 support is removed

  /** Operations that can ''return'' new maps.  There is no reason to include any other
    * kind of operation here, because they are covered by `#widen`.
    */
  implicit final class MapOps[Self, K, V](private val self: Self with NonEmpty[Map[K, V]])
      extends AnyVal {
    private type ESelf = Map[K, V]
    import NonEmpty.{unsafeNarrow => un}
    def +(kv: (K, V)): NonEmpty[Map[K, V]] = un((self: ESelf) + kv)
    def ++(xs: Iterable[(K, V)]): NonEmpty[Map[K, V]] = un((self: ESelf) ++ xs)
    def keySet: NonEmpty[Set[K]] = un((self: ESelf).keySet)
    def transform[W](f: (K, V) => W): NonEmpty[Map[K, W]] = un((self: ESelf) transform f)
  }

  /** Operations that can ''return'' new sets.  There is no reason to include any other
    * kind of operation here, because they are covered by `#widen`.
    */
  implicit final class SetOps[Self, A](private val self: Self with NonEmpty[Set[A]])
      extends AnyVal {
    private type ESelf = Set[A]
    import NonEmpty.{unsafeNarrow => un}
    def +(elem: A): NonEmpty[Set[A]] = un((self: ESelf) + elem)
    def ++(that: Iterable[A]): NonEmpty[Set[A]] = un((self: ESelf) ++ that)
  }

  implicit def traverse[F[_]](implicit F: Traverse[F]): Traverse[NonEmptyF[F, *]] =
    NonEmpty.substF(F)
}

sealed abstract class NonEmptyCollInstances {
  implicit def foldable[F[_]](implicit F: Foldable[F]): Foldable[NonEmptyF[F, *]] =
    NonEmpty.substF(F)

  import scala.language.implicitConversions
  implicit def widen[A](na: NonEmpty[A]): A = NonEmpty.subtype[A](na)
  implicit def widenF[F[+_], A](na: F[NonEmpty[A]]): F[A] =
    Liskov.co[F, NonEmpty[A], A](NonEmpty.subtype)(na)
}
