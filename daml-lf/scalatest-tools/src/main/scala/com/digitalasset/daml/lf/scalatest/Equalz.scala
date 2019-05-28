// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import org.scalatest.matchers.{MatchResult, Matcher}
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
trait Equalz {
  import Equalz.LtEqual
  final def equalz[A, B](expected: A)(implicit B: LtEqual[A]): Matcher[B.B] =
    actual =>
      MatchResult(
        B.evB.equal(expected, actual),
        s"$actual did not equal $expected",
        s"$actual equalled $expected"
    )
}

object Equalz extends Equalz {
  sealed abstract class LtEqual[-A] {
    type B >: A
    val evB: Equal[B]
  }

  object LtEqual {
    implicit def onlyInstance[A](implicit ev: Equal[A]): LtEqual[A] {type B = A} =
      new LtEqual[A] {
        type B = A
        val evB = ev
      }
  }
}
