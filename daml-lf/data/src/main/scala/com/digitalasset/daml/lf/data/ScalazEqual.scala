// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scalaz.Equal

private[lf] object ScalazEqual {
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
  def match2[A, B, C](fallback: C)(f: A => (B PartialFunction C))(a: A, b: B): C =
    f(a).applyOrElse(b, (_: B) => fallback)
}
