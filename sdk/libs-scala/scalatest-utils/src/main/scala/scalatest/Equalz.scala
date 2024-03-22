// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalatest.matchers.dsl.{MatcherFactory1}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.matchers.should.Matchers
import scalaz.Equal

/** Provides the `equalz` [[Matcher]].
  *
  * {{{
  *   Some(42) should equalz (some(42))
  *   some(42) should equalz (Some(42))
  *   none[Int] shouldNot equalz (Some(1))
  * }}}
  *
  * Why not simply provide [[org.scalactic.Equality]] instances and use
  * the existing `equal` matcher?  As Scalatest doc mentions,
  *
  * > By default, an implicit `Equality[T]` instance is available for
  * > any type `T`...
  *
  * In other words `Equality` instances are incoherent, and you get no
  * notification from the compiler if implicit resolution failed to
  * assemble the instance you meant.  (NB: never design your own
  * typeclasses this way in Scala.)
  *
  * Due to scala/bug#5075, you should enable `-Ypartial-unification` and
  * use `shouldx` instead of `should` where required.
  */
trait Equalz extends Matchers {
  import Equalz.{LubEqual, XMatcherFactory1, EqualFactory1}

  final def equalz[Ex](expected: Ex): EqualFactory1[Ex] =
    new MatcherFactory1[Ex, LubEqual[Ex, *]] with XMatcherFactory1[Ex] {
      type TC[A] = LubEqual[Ex, A]
      override def matcher[T <: Ex](implicit ev: TC[T]): Matcher[T] =
        actual =>
          MatchResult(
            ev.equal(expected, actual),
            s"$actual did not equal $expected",
            s"$actual equalled $expected",
          )
    }

  /** An improved design for [[MatcherFactory1]]; see [[XMatcherFactory1]]. */
  implicit final class XAnyShouldWrapper[L](private val leftHandSide: L) {
    def shouldx(mf: XMatcherFactory1[L])(implicit ev: mf.TC[L]) =
      (leftHandSide: AnyShouldWrapper[L]) should mf.matcher[L]
    def shouldNotx(mf: XMatcherFactory1[L])(implicit ev: mf.TC[L]) =
      (leftHandSide: AnyShouldWrapper[L]) shouldNot mf.matcher[L]
  }
}

object Equalz extends Equalz {
  sealed abstract class LubEqual[-A, -B] {
    def equal(l: A, r: B): Boolean
  }
  object LubEqual {
    implicit def onlyInstance[C: Equal]: LubEqual[C, C] = new LubEqual[C, C] {
      def equal(l: C, r: C) = Equal[C].equal(l, r)
    }
  }

  type EqualFactory1[SC] = XMatcherFactory1[SC] with MatcherFactory1[SC, LubEqual[SC, *]] {
    type TC[A] = LubEqual[SC, A]
  }

  /** scala/bug#5075 triggers in Scala 2.12 with `should equalz` &c, where
    * function = should.  This could also be fixed by making
    * `MatcherFactory1` use a HK type member instead of a parameter,
    * whereupon `should` wouldn't need the TYPECLASS1 parameter that
    * triggers the 5075 problem, but that's a tall order.
    */
  trait XMatcherFactory1[-SC] {
    type TC[A]
    def matcher[T <: SC: TC]: Matcher[T]
  }
}
