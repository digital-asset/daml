// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import scalaz.{-\/, Apply, Divide, InvariantFunctor, \/, \/-}

/** A variant of [[Apply]] that generalizes with [[Divide]].  Instead of lifting
  * functions, it lifts an isomorphism:
  *
  * {{{
  *   (A, B, ...) <~> Z  =>  (F[A], F[B], ...) => F[Z]
  * }}}
  *
  * Its laws are exactly those for [[Apply]], but restated for isomorphisms.
  */
trait InvariantApply[F[_]] extends InvariantFunctor[F] {

  /** alias for `xmap` */
  @inline final def xmapN[A, Z](fa: F[A])(f: A => Z)(g: Z => A): F[Z] =
    xmap(fa, f, g)

  /** The sole primitive combinator of [[InvariantApply]]. */
  def xmapN[A, B, Z](fa: F[A], fb: F[B])(f: (A, B) => Z)(g: Z => (A, B)): F[Z]

  // longer combinators; override for performance gains, smaller arities are
  // more important
  def xmapN[A, B, C, Z](fa: F[A], fb: F[B], fc: F[C])(f: (A, B, C) => Z)(g: Z => (A, B, C)): F[Z] =
    xmapN(fa, tupleN(fb, fc)) { (a, bc) =>
      val (b, c) = bc
      f(a, b, c)
    } { z =>
      val (a, b, c) = g(z)
      (a, (b, c))
    }

  def xmapN[A, B, C, D, Z](fa: F[A], fb: F[B], fc: F[C], fd: F[D])(
      f: (A, B, C, D) => Z
  )(g: Z => (A, B, C, D)): F[Z] =
    xmapN(tupleN(fa, fb), tupleN(fc, fd)) { (ab, cd) =>
      val (a, b) = ab
      val (c, d) = cd
      f(a, b, c, d)
    } { z =>
      val (a, b, c, d) = g(z)
      ((a, b), (c, d))
    }

  def xmapN[A, B, C, D, E, Z](fa: F[A], fb: F[B], fc: F[C], fd: F[D], fe: F[E])(
      f: (A, B, C, D, E) => Z
  )(g: Z => (A, B, C, D, E)): F[Z] =
    xmapN(fa, tupleN(fb, fc), tupleN(fd, fe)) { (a, bc, de) =>
      val (b, c) = bc
      val (d, e) = de
      f(a, b, c, d, e)
    } { z =>
      val (a, b, c, d, e) = g(z)
      (a, (b, c), (d, e))
    }

  // Likewise, as these functions don't entail...functions, you may have a
  // more efficient implementation
  def tupleN[A, B](fa: F[A], fb: F[B]): F[(A, B)] = xmapN(fa, fb)((_, _))(identity)
  def tupleN[A, B, C](fa: F[A], fb: F[B], fc: F[C]): F[(A, B, C)] =
    xmapN(fa, fb, fc)((_, _, _))(identity)
  def tupleN[A, B, C, D](fa: F[A], fb: F[B], fc: F[C], fd: F[D]): F[(A, B, C, D)] =
    xmapN(fa, fb, fc, fd)((_, _, _, _))(identity)
  def tupleN[A, B, C, D, E](fa: F[A], fb: F[B], fc: F[C], fd: F[D], fe: F[E]): F[(A, B, C, D, E)] =
    xmapN(fa, fb, fc, fd, fe)((_, _, _, _, _))(identity)

  /** Two [[InvariantApply]]s can be run at the same time. */
  def product[G[_]](implicit G: InvariantApply[G]): InvariantApply[Lambda[a => (F[a], G[a])]] =
    new InvariantApply.Product()(this, G)

  /** An identity can be added to [[InvariantApply]]. */
  def applyApplicative: InvariantApply[Lambda[a => F[a] \/ a]] = new InvariantApply.OneOr()(this)
}

object InvariantApply {
  def fromApply[F[_]](implicit F: Apply[F]): InvariantApply[F] = new InvariantApply[F] {
    override def xmap[A, B](fa: F[A], f: A => B, g: B => A) = F.xmap(fa, f, g)

    override def xmapN[A, B, Z](fa: F[A], fb: F[B])(f: (A, B) => Z)(g: Z => (A, B)) =
      F.apply2(fa, fb)(f)
  }

  def fromDivide[F[_]](implicit F: Divide[F]): InvariantApply[F] = new InvariantApply[F] {
    override def xmap[A, B](fa: F[A], f: A => B, g: B => A) = F.xmap(fa, f, g)

    override def xmapN[A, B, Z](fa: F[A], fb: F[B])(f: (A, B) => Z)(g: Z => (A, B)) =
      F.divide(fa, fb)(g)
  }

  private final class Product[F[_], G[_]](implicit F: InvariantApply[F], G: InvariantApply[G])
      extends InvariantApply[Lambda[a => (F[a], G[a])]] {
    type Pair[A] = (F[A], G[A])

    override def xmap[A, B](fa: Pair[A], f: A => B, g: B => A) =
      (F.xmap(fa._1, f, g), G.xmap(fa._2, f, g))

    override def xmapN[A, B, Z](fa: Pair[A], fb: Pair[B])(f: (A, B) => Z)(g: Z => (A, B)) =
      (F.xmapN(fa._1, fb._1)(f)(g), G.xmapN(fa._2, fb._2)(f)(g))
  }

  private final class OneOr[F[_]](implicit F: InvariantApply[F])
      extends InvariantApply[Lambda[a => F[a] \/ a]] {
    override def xmap[A, B](fa: F[A] \/ A, f: A => B, g: B => A) =
      fa.bimap(F.xmap(_, f, g), f)

    override def xmapN[A, B, Z](faa: F[A] \/ A, fbb: F[B] \/ B)(f: (A, B) => Z)(g: Z => (A, B)) =
      (faa, fbb) match {
        case (-\/(fa), -\/(fb)) => -\/(F.xmapN(fa, fb)(f)(g))
        case (-\/(fa), \/-(b)) => -\/(F.xmapN(fa)(f(_, b))(g(_)._1))
        case (\/-(a), -\/(fb)) => -\/(F.xmapN(fb)(f(a, _))(g(_)._2))
        case (\/-(a), \/-(b)) => \/-(f(a, b))
      }
  }
}
