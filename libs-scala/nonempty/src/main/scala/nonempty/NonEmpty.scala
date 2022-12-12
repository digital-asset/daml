// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import scala.collection.{Factory, IterableOnce, immutable => imm}, imm.Iterable, imm.Map, imm.Set
import scalaz.Id.Id
import scalaz.{Foldable, Foldable1, Monoid, OneAnd, Semigroup, Traverse}
import scalaz.Leibniz, Leibniz.===
import scalaz.Liskov, Liskov.<~<
import scalaz.syntax.std.option._

import com.daml.scalautil.FoldableContravariant
import com.daml.scalautil.Statement.discard
import NonEmptyCollCompat._

/** The visible interface of [[NonEmpty]]; use that value to access
  * these members.
  */
sealed abstract class NonEmptyColl {

  /** Use its alias [[com.daml.nonempty.NonEmpty]]. */
  type NonEmpty[+A]

  /** Use its alias [[com.daml.nonempty.NonEmptyF]]. */
  type NonEmptyF[F[_], A] <: NonEmpty[F[A]]

  private[nonempty] def substF[T[_[_]], F[_]](tf: T[F]): T[NonEmptyF[F, *]]
  private[nonempty] def subst[F[_[_]]](tf: F[Id]): F[NonEmpty]
  private[nonempty] def unsafeNarrow[Self <: imm.Iterable[Any]](self: Self): NonEmpty[Self]
}

object NonEmpty {

  /** Usable proof that [[NonEmpty]] is a subtype of its argument.  (We cannot put
    * this in an upper-bound, because that would prevent us from adding implicit
    * methods that replace the stdlib ones.)
    */
  def subtype[A]: NonEmpty[A] <~< A = {
    type K[F[_]] = F[A] <~< A
    NonEmptyColl.Instance.subst[K](Liskov.refl[A])
  }

  /** Usable proof that [[NonEmptyF]] is actually equivalent to its [[NonEmpty]]
    * parent type, not a strict subtype.  (We want Scala to treat it as a strict
    * subtype, usually, so that the type will be deconstructed by
    * partial-unification correctly.)
    */
  def equiv[F[_], A]: NonEmpty[F[A]] === NonEmptyF[F, A] = {
    type K[T[_]] = T[F[A]] === F[A]
    type KR[T[_]] = NonEmpty[F[A]] === T[A]
    import NonEmptyColl.Instance.{subst, substF}
    substF[KR, F](subst[K](Leibniz.refl[F[A]]))
  }

  /** {{{
    *  NonEmpty(List, 1, 2, 3) : NonEmpty[List[Int]] // with (1, 2, 3) as elements
    * }}}
    */
  final def apply[Fct, A, C <: imm.Iterable[A]](into: Fct, hd: A, tl: A*)(implicit
      fct: Fct => Factory[A, C]
  ): NonEmpty[C] = {
    val bb = into.newBuilder
    discard { (bb += hd) ++= tl }
    NonEmptyColl.Instance.unsafeNarrow(bb.result())
  }

  /** Like `apply`, but because `A` occurs directly in the result type, if the
    * call to `mk` has an expected `A` type, `hd` and `tl` will have that
    * expected type as well.  Particularly useful for `Set`s, as expected type
    * can widen the element type of the produced set, and `_` as an argument,
    * because the lambda argument type can be inferred from the expected result
    * type.
    *
    * However, strictly speaking, it is less general than `apply`; for example,
    * it doesn't work on `Map`s at all.  So it should only be used when you
    * need its particular type inference behavior.  See `"mk" should` tests
    * in `NonEmptySpec.scala` for examples where `mk` works but `apply` doesn't.
    */
  final def mk[Fct, A, C[X] <: imm.Iterable[X]](into: Fct, hd: A, tl: A*)(implicit
      fct: Fct => Factory[A, C[A]]
  ): NonEmpty[C[A]] =
    apply(into, hd, tl: _*)

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
  def unapply[Self](self: Self with imm.Iterable[_]): Option[NonEmpty[Self]] = {
    type K[F[_]] = F[Self]
    if (self.nonEmpty) Some(NonEmptyColl.Instance.subst[K](self)) else None
  }

  /** Like `unapply`, but when you don't want pattern matching. */
  final def from[Self](self: Self with imm.Iterable[_]): Option[NonEmpty[Self]] = unapply(self)
}

/** If you ever have to import [[NonEmptyColl]] or anything from it, your Scala
  * settings are probably wrong.
  */
object NonEmptyColl extends NonEmptyCollInstances {
  val Instance: NonEmptyColl = new NonEmptyColl {
    type NonEmpty[+A] = A
    type NonEmptyF[F[_], A] = F[A]
    private[nonempty] override def substF[T[_[_]], F[_]](tf: T[F]): T[NonEmptyF[F, *]] = tf
    private[nonempty] override def subst[F[_[_]]](tf: F[Id]): F[NonEmpty] = tf
    private[nonempty] override def unsafeNarrow[Self <: imm.Iterable[Any]](
        self: Self
    ): NonEmpty[Self] = self
  }

  implicit final class ReshapeOps[F[_], A](private val nfa: NonEmpty[F[A]]) extends AnyVal {

    @deprecated("use toNEF instead", since = "2.0.1")
    def toF: NonEmptyF[F, A] = nfa.toNEF

    /** See [[NonEmptyF]] for further explanation. */
    def toNEF: NonEmptyF[F, A] = NonEmpty.equiv[F, A](nfa)
  }

  implicit final class UnReshapeOps[F[_], A](private val nfa: NonEmptyF[F, A]) extends AnyVal {

    /** `x.fromNEF` is `(x: NonEmpty[F[A]])` but possibly shorter.  If code
      * compiles without the call to `fromNEF`, you don't need the call.
      */
    def fromNEF: NonEmpty[F[A]] = nfa
  }

  implicit final class UnwrapOps[A](private val self: NonEmpty[A]) extends AnyVal {

    /** `x.forgetNE` is `(x: A)` but possibly shorter. If code compiles without
      * the call to `forgetNE`, you don't need the call.
      */
    def forgetNE: A = self
  }

  /** Operations that can ''return'' new maps or sets.  There is no reason to include any other
    * kind of operation here, because they are covered by `#widen`.
    */
  implicit final class `SortedMap Ops`[
      K,
      V,
      CC[X, +Y] <: imm.SortedMap[X, Y] with imm.SortedMapOps[X, Y, CC, _],
  ](private val self: NonEmpty[imm.SortedMapOps[K, V, CC, _]])
      extends AnyVal {
    private type ESelf = imm.SortedMapOps[K, V, CC, _]
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    def unsorted: NonEmpty[Map[K, V]] = un((self: ESelf).unsorted)
    def updated(key: K, value: V): NonEmpty[CC[K, V]] = un((self: ESelf).updated(key, value))
    def transform[W](f: (K, V) => W): NonEmpty[CC[K, W]] = un((self: ESelf).transform(f))
    def keySet: NonEmpty[imm.SortedSet[K]] = un((self: ESelf).keySet)
  }

  /** Operations that can ''return'' new maps.  There is no reason to include any other
    * kind of operation here, because they are covered by `#widen`.
    */
  implicit final class `Map Ops`[K, V, CC[X, +Y] <: imm.Map[X, Y] with imm.MapOps[X, Y, CC, _]](
      private val self: NonEmpty[imm.MapOps[K, V, CC, _]]
  ) extends AnyVal {
    private type ESelf = imm.MapOps[K, V, CC, _]
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    // You can't have + because of the dumb string-converting thing in stdlib
    def updated(key: K, value: V): NonEmpty[CC[K, V]] = un((self: ESelf).updated(key, value))
    def ++(xs: IterableOnce[(K, V)]): NonEmpty[CC[K, V]] = un((self: ESelf) ++ xs)
    def keySet: NonEmpty[Set[K]] = un((self: ESelf).keySet)
    def transform[W](f: (K, V) => W): NonEmpty[CC[K, W]] = un((self: ESelf) transform f)
  }

  /** Operations that can ''return'' new sets.  There is no reason to include any other
    * kind of operation here, because they are covered by `#widen`.
    */
  implicit final class `Set Ops`[A, CC[_], C <: imm.SetOps[A, CC, C] with Iterable[A]](
      private val self: NonEmpty[imm.SetOps[A, CC, C]]
  ) extends AnyVal {
    private type ESelf = imm.SetOps[A, CC, C]
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    // You can't have + because of the dumb string-converting thing in stdlib
    def incl(elem: A): NonEmpty[C] = un((self: ESelf) + elem)
  }

  implicit final class NEPreservingSeqOps[A, CC[X] <: imm.Seq[X], C](
      private val self: NonEmpty[SeqOps[A, CC, C with imm.Seq[A]]]
  ) extends AnyVal {
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    private type ESelf = SeqOps[A, CC, C with imm.Iterable[A]]
    // the +: :+ set here is so you don't needlessly "lose" your NE-ness
    def +:[B >: A](elem: B): NonEmpty[CC[B]] = un(elem +: (self: ESelf))
    def :+[B >: A](elem: B): NonEmpty[CC[B]] = un((self: ESelf) :+ elem)
    // the +-: :-+ set here is to mirror the patterns, as the ones in
    // `NE Seq Ops` are unreachable in normal usage, since implicit conversions
    // do not compose
    @`inline` def +-:[B >: A](elem: B): NonEmpty[CC[B]] = elem +: self
    @`inline` def :-+[B >: A](elem: B): NonEmpty[CC[B]] = self :+ elem

    def distinct: NonEmpty[C] = un((self: ESelf).distinct)
    def sortBy[B](f: A => B)(implicit ord: Ordering[B]): NonEmpty[C] =
      un((self: ESelf).sortBy(f))
    def sorted[B >: A](implicit ord: Ordering[B]): NonEmpty[C] =
      un((self: ESelf).sorted[B])
  }

  implicit final class `Seq Ops`[A, CC[_], C](
      private val self: NonEmpty[SeqOps[A, CC, C with CC[A]]]
  ) extends AnyVal {
    def toOneAnd: OneAnd[CC, A] = {
      val h +-: t = self
      OneAnd(h, t)
    }
  }

  implicit final class NEPseudofunctorOps[A, CC[X] <: imm.Iterable[X], C](
      private val self: NonEmpty[IterableOps[A, CC, C with imm.Iterable[A]]]
  ) extends AnyVal {
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    private type ESelf = IterableOps[A, CC, C with imm.Iterable[A]]

    def map[B](f: A => B): NonEmpty[CC[B]] = un((self: ESelf) map f)
    def flatMap[B](f: A => NonEmpty[IterableOnce[B]]): NonEmpty[CC[B]] = {
      type K[F[_]] = (F[ESelf], A => F[IterableOnce[B]]) => F[CC[B]]
      NonEmptyColl.Instance.subst[K](_ flatMap _)(self, f)
    }
  }

  implicit def traverse[F[_]](implicit F: Traverse[F]): Traverse[NonEmptyF[F, *]] =
    NonEmptyColl.Instance.substF(F)

  // by requiring Monoid instead of Semigroup, we exclude intersection semigroups,
  // which are not legitimate for lifting into NonEmpty
  implicit def semigroup[A](implicit A: Monoid[A]): Semigroup[NonEmpty[A]] =
    NonEmptyColl.Instance.subst[Lambda[k[_] => Semigroup[k[A]]]](A)
}

sealed abstract class NonEmptyCollInstances extends NonEmptyCollInstances0 {

  implicit def foldable[F[_]](implicit F: Foldable[F]): Foldable[NonEmptyF[F, *]] =
    NonEmptyColl.Instance.substF(F)

  // Manually expand the implicit class into an implicit conversion
  // so that it gets a lower priority during implicit resolution than `Map Ops`,
  // which also defines `++`.
  import scala.language.implicitConversions
  implicit def NEPreservingOps[A, CC[X] <: imm.Iterable[X], C](
      self: NonEmpty[IterableOps[A, CC, C with imm.Iterable[A]]]
  ): NonEmptyCollInstances.NEPreservingOps[A, CC, C] =
    new NonEmptyCollInstances.NEPreservingOps[A, CC, C](self)

}

object NonEmptyCollInstances {
  final class NEPreservingOps[A, CC[X] <: imm.Iterable[X], C](
      private val self: NonEmpty[IterableOps[A, CC, C with imm.Iterable[A]]]
  ) extends AnyVal {
    import NonEmptyColl.Instance.{unsafeNarrow => un}
    private type ESelf = IterableOps[A, CC, C with imm.Iterable[A]]
    def groupBy[K](f: A => K): NonEmpty[Map[K, NonEmpty[C]]] =
      NonEmptyColl.Instance.subst[Lambda[f[_] => f[Map[K, f[C]]]]]((self: ESelf) groupBy f)
    def groupBy1[K](f: A => K): NonEmpty[Map[K, NonEmpty[C]]] = self groupBy f
    def toList: NonEmpty[List[A]] = un((self: ESelf).toList)
    def toVector: NonEmpty[Vector[A]] = un((self: ESelf).toVector)
    def toSeq: NonEmpty[imm.Seq[A]] = un((self: ESelf).toSeq)
    def toSet: NonEmpty[Set[A]] = un((self: ESelf).toSet)
    def toMap[K, V](implicit isPair: A <:< (K, V)): NonEmpty[Map[K, V]] = un((self: ESelf).toMap)
    def to[C1 <: imm.Iterable[A]](factory: Factory[A, C1]): NonEmpty[C1] = un(
      (self: ESelf) to factory
    )
    def zipWithIndex: NonEmpty[CC[(A, Int)]] = un((self: ESelf).zipWithIndex)
    def ++[B >: A](suffix: IterableOnce[B]): NonEmpty[CC[B]] = un((self: ESelf) ++ suffix)

    // (not so valuable unless also using wartremover to disable partial Seq ops)
    @`inline` def head1: A = self.head
    @`inline` def tail1: C = self.tail
    def reduceLeft[B >: A](op: (B, A) => B): B = (self: ESelf).reduceLeft(op)
    @`inline` def last1: A = self.last
    def min1(implicit ev: Ordering[A]): A = (self: ESelf).min
    def max1(implicit ev: Ordering[A]): A = (self: ESelf).max

    def minBy1[B](f: A => B)(implicit ev: Ordering[B]): A = (self: ESelf).minBy(f)
    def maxBy1[B](f: A => B)(implicit ev: Ordering[B]): A = (self: ESelf).maxBy(f)
  }
}

sealed abstract class NonEmptyCollInstances0 {
  implicit def foldable1[F[_]](implicit F: Foldable[F]): Foldable1[NonEmptyF[F, *]] =
    NonEmptyColl.Instance.substF(new Foldable1[F] with FoldableContravariant[F, F] {
      private[this] def errEmpty(fa: F[_]) =
        throw new IllegalArgumentException(
          s"empty structure coerced to non-empty: $fa: ${fa.getClass.getSimpleName}"
        )

      private[this] def assertNE[Z](original: F[_], fa: Option[Z]) =
        fa.cata(identity, errEmpty(original))

      override def foldMap1[A, B: Semigroup](fa: F[A])(f: A => B) =
        assertNE(fa, F.foldMap1Opt(fa)(f))

      override def foldMapRight1[A, B](fa: F[A])(z: A => B)(f: (A, => B) => B) =
        assertNE(fa, F.foldMapRight1Opt(fa)(z)(f))

      override def foldMapLeft1[A, B](fa: F[A])(z: A => B)(f: (B, A) => B) =
        assertNE(fa, F.foldMapLeft1Opt(fa)(z)(f))

      protected[this] override def Y = F

      protected[this] override def ctmap[A](xa: F[A]) = xa
    })

  import scala.language.implicitConversions

  /** This adds the rest of the `A` API to `na`.  However, because it is in the
    * most distant parent class from `object NonEmptyColl`, it is tried last
    * during method resolution.  That's why our custom `+`, `transform`,
    * `toList`, and other such methods win; their implicit conversions are
    * defined '''in a subclass''' of this one.
    */
  implicit def widen[A](na: NonEmpty[A]): A = NonEmpty.subtype[A](na)
  implicit def widenF[F[+_], A](na: F[NonEmpty[A]]): F[A] =
    Liskov.co[F, NonEmpty[A], A](NonEmpty.subtype)(na)
}
