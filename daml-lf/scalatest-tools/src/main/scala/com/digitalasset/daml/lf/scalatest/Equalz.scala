// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import org.scalatest.Matchers
import org.scalatest.matchers.{MatchResult, Matcher, MatcherFactory1}
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
  import Equalz.LubEqual

  final def equalz[Ex](expected: Ex): MatcherFactory1[Ex, LubEqual.Gt[Ex, ?]] =
    new MatcherFactory1[Ex, LubEqual.Gt[Ex, ?]] {
      override def matcher[T <: Ex](implicit ev: LubEqual.Gt[Ex, T]): Matcher[T] =
        actual =>
          MatchResult(
            ev.equal(expected, actual),
            s"$actual did not equal $expected",
            s"$actual equalled $expected"
        )
    }
}

object Equalz extends Equalz {
  sealed abstract class LubEqual[-A, C, -B] {
    def equal(l: A, r: B): Boolean
  }
  object LubEqual {
    type Gt[-A, -B] = LubEqual[A, _, B]
    implicit def onlyInstance[C: Equal]: LubEqual[C, C, C] = new LubEqual[C, C, C] {
      def equal(l: C, r: C) = Equal[C].equal(l, r)
    }
  }
}
