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
  import Equalz.{EqualzInvocation, Lub}

  final def equalz[Ex](expected: Ex): MatcherFactory1[Ex, Equal] =
    new MatcherFactory1[Ex, Equal] {
      override def matcher[T <: Ex](implicit ev: Equal[T]): Matcher[T] =
        actual =>
          MatchResult(
            ev.equal(expected, actual),
            s"$actual did not equal $expected",
            s"$actual equalled $expected"
        )
    }
}

object Equalz extends Equalz {
  sealed abstract class Lub[-A, -B, C] {
    def left(l: A): C
    def right(r: B): C
  }
  object Lub {
    final class OnlyInstance[C] extends Lub[C, C, C] {
      override def left(l: C) = l
      override def right(r: C) = r
    }
    implicit def onlyInstance[C]: Lub[C, C, C] = new OnlyInstance
  }
}
