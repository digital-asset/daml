// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import org.scalatest.matchers.Matcher
import scalaz.Equal

/** Provides the `equalz` [[Matcher]].
  *
  * {{{
  *   Some(42) should equalz (some(42))
  *   some(42) should equalz (Some(42))
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
  def equalz[A, B >: A](expected: A)(implicit B: Equal[B]): Matcher[B] =
    actual =>
      MatchResult(
        B.equal(expected, actual),
        s"$expected does not equal $actual",
        s"$expected equals $actual"
    )
}

object Equalz extends Equalz
