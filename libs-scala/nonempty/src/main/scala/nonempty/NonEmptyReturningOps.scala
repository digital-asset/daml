// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import scala.collection.{immutable => imm}, imm.Map, imm.Set
import scalaz.Foldable1
import NonEmptyCollCompat._

/** Functions where ''the receiver'' is non-empty can be found implicitly with
  * no further imports; they "just work".  However, if we wish to refine a
  * method on a built-in type that merely ''returns'' a nonempty-annotated type, we
  * must import the contents of this object.
  */
object NonEmptyReturningOps {
  implicit final class `NE Iterable Ops`[A, CC[_], C](
      private val self: IterableOps[A, CC, C with imm.Iterable[A]]
  ) extends AnyVal {
    def groupBy1[K](f: A => K): Map[K, NonEmpty[C]] =
      NonEmptyColl.Instance.subst[Lambda[f[_] => Map[K, f[C]]]](self groupBy f)

    def groupMap1[K, B](key: A => K)(f: A => B): Map[K, NonEmpty[CC[B]]] =
      NonEmptyColl.Instance.subst[Lambda[f[_] => Map[K, f[CC[B]]]]](self.groupMap(key)(f))
  }

  implicit final class `NE Seq Ops`[A, CC[X] <: imm.Seq[X], C](
      private val self: SeqOps[A, CC, C with imm.Seq[A]]
  ) extends AnyVal {
    import NonEmptyColl.Instance.{unsafeNarrow => un}

    def +-:(elem: A): NonEmpty[CC[A]] = un(elem +: self)
    def :-+(elem: A): NonEmpty[CC[A]] = un(self :+ elem)
  }

  implicit final class `NE Set Ops`[A](private val self: Set[A]) extends AnyVal {
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    def incl1(elem: A): NonEmpty[Set[A]] = un(self + elem)
  }

  implicit final class `NE Foldable1 Ops`[F[_], A](self: F[A])(implicit F: Foldable1[F]) {
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    def toSet1: NonEmpty[Set[A]] = un(F toSet self)
    def toVector1: NonEmpty[Vector[A]] = un(F toVector self)
  }
}
