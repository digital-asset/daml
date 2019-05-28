// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import org.scalatest.Matchers
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
trait Equalz extends Matchers {
  import Equalz.{EqualzInvocation, Lub}

  private[this] final def equalzImpl[A](expected: A)(implicit A: Equal[A]): Matcher[A] =
    actual =>
      MatchResult(
        A.equal(expected, actual),
        s"$actual did not equal $expected",
        s"$actual equalled $expected"
    )

  def equalz[Ac](ac: Ac): EqualzInvocation[Ac] = new EqualzInvocation(ac)

  implicit final class AnyShouldzWrapper[Ex](private val expected: Ex) {
    def should[Ac, T](actual: EqualzInvocation[Ac])(implicit lub: Lub[Ex, Ac, T], eq: Equal[T]) =
      (lub.left(expected): AnyShouldWrapper[T]) should equalzImpl(lub.right(actual.actual))
    def shouldNot[Ac, T](actual: EqualzInvocation[Ac])(implicit lub: Lub[Ex, Ac, T], eq: Equal[T]) =
      (lub.left(expected): AnyShouldWrapper[T]) shouldNot equalzImpl(lub.right(actual.actual))
  }
}

object Equalz extends Equalz {
  final class EqualzInvocation[+Ac](private[Equalz] val actual: Ac) extends AnyVal

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
