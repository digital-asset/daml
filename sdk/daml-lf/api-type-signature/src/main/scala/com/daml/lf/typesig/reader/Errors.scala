// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.typesig
package reader

import com.digitalasset.daml.lf.data.Ref.{DottedName, Name}

import scala.language.implicitConversions
import scala.collection.immutable.Map
import scalaz.{-\/, ==>>, @@, Applicative, Cord, Monoid, Order, Semigroup, Tag, Traverse, \/, \/-}
import scalaz.std.map._
import scalaz.std.string._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._

// Free[K ==>> ?, A] with an incompatible semigroup and strict representation
final case class Errors[K, A](run: A \/ (K ==>> Errors[K, A])) {
  def fold[Z](pure: A => Z, roll: (K ==>> Z) => Z): Z = {
    def go(self: Errors[K, A]): Z =
      self.run.fold(pure, r => roll(r map go))
    go(this)
  }

  def collectAndPrune[B](f: A PartialFunction B)(implicit K: Order[K]): Errors[K, B] = {
    def go(self: Errors[K, A]): Option[Errors[K, B]] =
      self.run.fold(
        a => f.lift(a) map Errors.point,
        { m =>
          val m2 = m mapOption go
          if (m2.isEmpty) None else Some(Errors(\/-(m2)))
        },
      )
    go(this) getOrElse Errors.zeroErrors
  }

  def locate(loc: K): Errors[K, A] =
    Errors(\/-(==>>.singleton(loc, this)))
}

object Errors {
  // property of a LF structure, or name of a program element
  type ErrorLoc = (Symbol \/ String) @@ Errors.type

  implicit final class `ErrorLoc syntax`(private val self: ErrorLoc) extends AnyVal {
    def fold[Z](property: Symbol => Z, field: String => Z): Z =
      Tag.unwrap(self).fold(property, field)
  }

  implicit val `ErrorLoc order`: Order[ErrorLoc] = {
    implicit val symorder: Order[Symbol] = Order[String] contramap (_.name)
    Tag.subst(Order[Symbol \/ String])
  }

  def point[K, A](a: A): Errors[K, A] = Errors(-\/(a))

  def zeroErrors[K, A]: Errors[K, A] = Errors(\/-(==>>.empty))

  implicit def `Errors covariant`[K]: Traverse[({ type l[a] = Errors[K, a] })#l] =
    new Traverse[({ type l[a] = Errors[K, a] })#l] {
      override def map[A, B](fa: Errors[K, A])(f: A => B): Errors[K, B] =
        Errors(fa.run.bimap(f, (_ map (map(_)(f)))))

      // inorder fold/traversal
      override def traverseImpl[G[_]: Applicative, A, B](fa: Errors[K, A])(
          f: A => G[B]
      ): G[Errors[K, B]] =
        fa.run.bitraverse(f, (_ traverse (traverseImpl(_)(f)))).map(Errors(_))
    }

  implicit def `Errors monoid`[K: Order, A: Semigroup]: Monoid[Errors[K, A]] = {
    object Rec { // inst recurs on inst
      implicit val inst: Monoid[Errors[K, A]] = Monoid.instance(
        (l, r) => Errors(l.run |+| r.run),
        zeroErrors,
      )
    }
    Rec.inst
  }

  implicit def propertyErr(s: Symbol): ErrorLoc = Tag(-\/(s))
  implicit def keyedErr(s: String): ErrorLoc = Tag(\/-(s))
  implicit def indexErr(i: Int): ErrorLoc = Tag(\/-(i.toString))
  implicit def identifierKeyedErr(s: Name): ErrorLoc = Tag(\/-(s))
  implicit def definitionErr(s: DottedName): ErrorLoc = Tag(\/-(s.toString))

  def locate[K, Loc, E, A](loc: K, fa: Errors[Loc, E] \/ A)(implicit
      kloc: K => Loc
  ): Errors[Loc, E] \/ A =
    fa leftMap (_ locate loc)

  def rootErr[E, A, Loc](fa: E \/ A): Errors[Loc, E] \/ A = fa leftMap point

  def rootErrOf[Loc]: RootErrOf[Loc] = new RootErrOf()

  final class RootErrOf[Loc](private val ignore: Boolean = false) extends AnyVal {
    def apply[E, A](fa: E \/ A): Errors[Loc, E] \/ A = rootErr(fa)
  }

  /** Meant for "discard and continue" style errors; errors end up in the _1,
    * successes in the _2.
    */
  private[reader] def partitionIndexedErrs[K, A, Loc: Order, E: Semigroup, B](
      map: Iterable[(K, A)]
  )(f: A => (Errors[Loc, E] \/ B))(implicit kloc: K => Loc): (Errors[Loc, E], Map[K, B]) = {
    import scalaz.std.iterable._
    val (errs, successes) = map.partitionMap { case (k, a) =>
      locate(kloc(k), f(a)).map((k, _)).toEither
    }
    (errs.suml, successes.toMap)
  }

  /** Meant for "fail if any fail" style errors; failures end up indexed by `kloc`
    * in the left if any (`kloc` is not required to be injective, but that's
    * probably what you want).
    */
  private[reader] def traverseIndexedErrs[F[_]: Traverse, K, A, Loc: Order, E: Semigroup, B](
      map: F[(K, A)]
  )(f: A => (Errors[Loc, E] \/ B))(implicit kloc: K => Loc): Errors[Loc, E] \/ F[B] =
    map.traverse { case (k, a) => locate(kloc(k), f(a)).validation }.disjunction

  /** Like `traverseIndexedErrs` for maps specifically. If result is
    * right, the keyset is guaranteed to be the same.
    */
  private[reader] def traverseIndexedErrsMap[K, A, Loc: Order, E: Semigroup, B](map: Map[K, A])(
      f: A => (Errors[Loc, E] \/ B)
  )(implicit kloc: K => Loc): Errors[Loc, E] \/ Map[K, B] =
    map.transform((k, a) => locate(kloc(k), f(a)).validation).sequence.disjunction

  private[reader] def stringReport[Loc, E](
      errors: Errors[Loc, E]
  )(loc: Loc => Cord, msg: E => Cord): Cord = {
    import scalaz.std.vector._
    errors
      .fold[Vector[Cord]](
        e => Vector(msg(e)),
        _ foldMapWithKey ((l, cs) => loc(l) +: cs.map("  " +: _)),
      )
      .suml
  }

  private[this] implicit final class `scalaz ==>> future`[A, B](private val self: A ==>> B)
      extends AnyVal {
    // added (more efficiently) in scalaz 7.3
    def foldMapWithKey[C: Monoid](f: (A, B) => C): C =
      self.foldlWithKey(mzero[C])((c, a, b) => c |+| f(a, b))
  }
}
