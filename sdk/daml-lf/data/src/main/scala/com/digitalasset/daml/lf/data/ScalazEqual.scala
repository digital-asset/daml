// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scalaz.{@@, Equal, Order, Tag}
import scalaz.syntax.order._

private[daml] object ScalazEqual {
  def withNatural[A](isNatural: Boolean)(c: (A, A) => Boolean): Equal[A] =
    if (isNatural) Equal.equalA else Equal.equal(c)

  /** Curry the typical pattern of matching equals by pairs, preserving exhaustiveness
    * checking while reducing the boilerplate in each case.
    *
    * For example, this is unsafe:
    *
    * {{{
    *  (l, r) match {
    *    case (Left(l1), Left(l2)) => l1 == l2
    *    case (Right(r1), Right(r2)) => r1 == r2
    *    case _ => false
    *  }
    * }}}
    *
    * because the third case disables exhaustiveness checking. And the easier
    * it is to make this mistake, the stronger impulse to create the situation
    * where it can occur, because the cost of writing out the false cases is
    * quadratic.
    *
    * With this function, the above would be written
    *
    * {{{
    *  match2(fallback = false) {
    *    case Left(l1) => {case Left(l2) => l1 == l2}
    *    case Right(r1) => {case Right(r2) => r1 == r2}
    *  }
    * }}}
    *
    * which preserves exhaustiveness checking for the left argument, which is
    * perfectly sufficient for writing equals functions.
    */
  def match2[A, B, C](fallback: => C)(f: A => (B PartialFunction C))(a: A, b: B): C =
    f(a).applyOrElse(b, (_: B) => fallback)

  /** The Equal and Order instances only use `iterator` directly, so
    * this is perfectly sufficient.  If you want a public version, you
    * need something more along the lines of [[ImmArray.ImmArraySeq]],
    * customized for the specific type.
    */
  private[data] def toIterableForScalazInstances[A](iter: => Iterator[A]): Iterable[A] =
    new Iterable[A] {
      override final def iterator = iter
    }

  implicit final class `Match2 syntax`[+A, +B](private val self: (A, B)) extends AnyVal {
    def match2[C](f: A => (B PartialFunction C))(fallback: => C): C =
      ScalazEqual.match2(fallback)(f)(self._1, self._2)
  }

  /** Like Order#contramap, but with support for equalIsNatural. */
  private[data] def orderBy[A, I: Order](f: A => I, inductiveNaturalEqual: Boolean): Order[A] =
    new OrderBy(f, inductiveNaturalEqual)

  private[data] def equalBy[A, I: Equal](f: A => I, inductiveNaturalEqual: Boolean): Equal[A] = {
    val eqn = inductiveNaturalEqual
    new EqualBy[A, I] {
      override val I = Equal[I]
      override val k = f
      override val inductiveNaturalEqual = eqn
    }
  }

  private[this] final class OrderBy[A, I](
      val k: A => I,
      override val inductiveNaturalEqual: Boolean,
  )(implicit val I: Order[I])
      extends Order[A]
      with EqualBy[A, I] {
    override def order(a: A, b: A) =
      k(a) ?|? k(b)
  }

  private[this] sealed trait EqualBy[A, I] extends Equal[A] {
    def k: A => I
    implicit def I: Equal[I]
    def inductiveNaturalEqual: Boolean
    override final def equalIsNatural = inductiveNaturalEqual && I.equalIsNatural
    override final def equal(a: A, b: A) =
      k(a) === k(b)
  }

  private[lf] sealed trait _2
  private[lf] object _2 {
    val T = Tag.of[_2]
    implicit def `_2 Order`[L, R: Order]: Order[(L, R) @@ _2] =
      Tag subst Order[R].contramap(_._2)
  }
}
