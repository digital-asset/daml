// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import scala.collection.{immutable => imm}, imm.Map, imm.Set
import scalaz.Id.Id
import scalaz.{Foldable, Traverse}
import scalaz.Leibniz, Leibniz.===
import scalaz.Liskov, Liskov.<~<
import NonEmptyCollCompat._

/** The visible interface of [[NonEmpty]]; use that value to access
  * these members.
  */
sealed abstract class NonEmptyColl {

  /** Use its alias [[com.daml.scalautil.nonempty.NonEmpty]]. */
  type NonEmpty[+A]

  /** Use its alias [[com.daml.scalautil.nonempty.NonEmptyF]]. */
  type NonEmptyF[F[_], A] <: NonEmpty[F[A]]

  private[nonempty] def substF[T[_[_]], F[_]](tf: T[F]): T[NonEmptyF[F, *]]
  private[nonempty] def subst[F[_[_]]](tf: F[Id]): F[NonEmpty]
  private[nonempty] def unsafeNarrow[Self](self: Self with imm.Iterable[_]): NonEmpty[Self]

  /** Usable proof that [[NonEmpty]] is a subtype of its argument.  (We cannot put
    * this in an upper-bound, because that would prevent us from adding implicit
    * methods that replace the stdlib ones.)
    */
  def subtype[A]: NonEmpty[A] <~< A

  /** Usable proof that [[NonEmptyF]] is actually equivalent to its [[NonEmpty]]
    * parent type, not a strict subtype.  (We want Scala to treat it as a strict
    * subtype, usually, so that the type will be deconstructed by
    * partial-unification correctly.)
    */
  def equiv[F[_], A]: NonEmpty[F[A]] === NonEmptyF[F, A]

  /** Check whether `self` is non-empty; if so, return it as the non-empty subtype. */
  def apply[Self](self: Self with imm.Iterable[_]): Option[NonEmpty[Self]]

  /** In pattern matching, think of [[NonEmpty]] as a sub-case-class of every
    * [[imm.Iterable]]; matching `case NonEmpty(ne)` ''adds'' the non-empty type
    * to `ne` if the pattern matches.
    *
    * You will get an unchecked warning if the selector is not statically of an
    * immutable type.  So [[scala.collection.Seq]] will not work.
    *
    * The type-checker will not permit you to apply this to a value that already
    * has the [[NonEmpty]] type, so don't worry about redundant checks here.
    */
  def unapply[Self](self: Self with imm.Iterable[_]): Option[NonEmpty[Self]] = apply(self)
}

/** If you ever have to import [[NonEmptyColl]] or anything from it, your Scala
  * settings are probably wrong.
  */
object NonEmptyColl extends NonEmptyCollInstances {
  private[nonempty] object Instance extends NonEmptyColl {
    type NonEmpty[+A] = A
    type NonEmptyF[F[_], A] = F[A]
    private[nonempty] override def substF[T[_[_]], F[_]](tf: T[F]) = tf
    private[nonempty] override def subst[F[_[_]]](tf: F[Id]) = tf
    override def subtype[A] = Liskov.refl[A]
    override def equiv[F[_], A] = Leibniz.refl

    override def apply[Self](self: Self with imm.Iterable[_]) =
      if (self.nonEmpty) Some(self) else None
    private[nonempty] override def unsafeNarrow[Self](self: Self with imm.Iterable[_]) = self
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

  implicit final class NEPreservingOps[A, C](
      private val self: NonEmpty[IterableOps[A, imm.Iterable, C with imm.Iterable[A]]]
  ) {
    import NonEmpty.{unsafeNarrow => un}
    private type ESelf = IterableOps[A, imm.Iterable, C with imm.Iterable[A]]
    def toList: NonEmpty[List[A]] = un((self: ESelf).toList)
    def toVector: NonEmpty[Vector[A]] = un((self: ESelf).toVector)
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
