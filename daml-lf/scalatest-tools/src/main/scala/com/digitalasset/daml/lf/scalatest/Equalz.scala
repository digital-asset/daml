// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import org.scalatest.Matchers
import org.scalatest.matchers.{MatchResult, Matcher}
import scala.language.higherKinds
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
  */
trait Equalz extends Matchers {
  import Equalz.{LubEqual, XMatcherFactory1}

  final def equalz[Ex](expected: Ex): XMatcherFactory1[Ex] { type TC[A] = LubEqual[Ex, A] } =
    new XMatcherFactory1[Ex] {
      type TC[A] = LubEqual[Ex, A]
      override def matcher[T <: Ex](implicit ev: TC[T]): Matcher[T] =
        actual =>
          MatchResult(
            ev.equal(expected, actual),
            s"$actual did not equal $expected",
            s"$actual equalled $expected"
        )
    }

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

  abstract class XMatcherFactory1[-SC] {
    type TC[A]
    def matcher[T <: SC: TC]: Matcher[T]
  }
}
