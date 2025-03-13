// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ScalaFuturesWithPatience, lifecycle}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, OptionValues}

trait AbsorbUnlessShutdownTest extends AnyWordSpec with BaseTest {

  def absorbUnlessShutdown[F[_]](
      absorb: AbsorbUnlessShutdown[F],
      fixture: AbsorbUnlessShutdownTest.Fixture[F],
  ): Unit = {
    "mapUS" should {
      "preserve the identity" in {
        val a = fixture.pure(42)
        val mapped = absorb.mapUS(a)(Outcome.apply)
        fixture.value(mapped) shouldBe Outcome(42)
      }

      "absorb AbortedDueToShutdown" in {
        val a = fixture.pure(1)
        val aborted = absorb.mapUS(a)(_ => AbortedDueToShutdown)
        fixture.value(aborted) shouldBe AbortedDueToShutdown
      }

      "compose" in {
        val a = fixture.pure(23)
        val mapped1 = absorb.mapUS(a)(x => Outcome(x + 1))
        val mapped2 = absorb.mapUS(mapped1)(x => Outcome(x * 2))
        val mapped = absorb.mapUS(a)(x => Outcome((x + 1) * 2))
        fixture.value(mapped) shouldBe fixture.value(mapped2)
      }
    }

    "pureAborted" should {
      "return AbortedDueToShutdown" in {
        fixture.value(absorb.pureAborted) shouldBe AbortedDueToShutdown
      }

      "propagate through maps" in {
        val mapped = absorb.mapUS(absorb.pureAborted[Int])(_ => Outcome(42))
        fixture.value(mapped) shouldBe AbortedDueToShutdown
      }
    }
  }
}

object AbsorbUnlessShutdownTest {
  trait Fixture[F[_]] {
    def pure[A](a: A): F[A]
    def value[A](fa: F[A]): UnlessShutdown[A]
  }
}

class UnlessShutdownAbsorbUnlessShutdownTest extends AbsorbUnlessShutdownTest {
  "UnlessShutdown" should {

    behave like absorbUnlessShutdown(
      AbsorbUnlessShutdown[UnlessShutdown],
      UnlessShutdownAbsorbUnlessShutdownTest.Fixture,
    )
  }
}

object UnlessShutdownAbsorbUnlessShutdownTest {
  private[lifecycle] object Fixture extends AbsorbUnlessShutdownTest.Fixture[UnlessShutdown] {
    override def pure[A](a: A): UnlessShutdown[A] = Outcome(a)

    override def value[A](fa: UnlessShutdown[A]): UnlessShutdown[A] = fa
  }
}

class FutureUnlessShutdownAbsorbUnlessShutdownTest
    extends AbsorbUnlessShutdownTest
    with HasExecutionContext {

  "FutureUnlessShutdown" should {

    FutureUnlessShutdown.unit.unwrap.futureValue

    behave like absorbUnlessShutdown(
      AbsorbUnlessShutdown[FutureUnlessShutdown],
      FutureUnlessShutdownAbsorbUnlessShutdownTest.Fixture,
    )
  }
}

object FutureUnlessShutdownAbsorbUnlessShutdownTest {
  private[lifecycle] object Fixture
      extends AbsorbUnlessShutdownTest.Fixture[FutureUnlessShutdown]
      with ScalaFuturesWithPatience {
    override def pure[A](a: A): FutureUnlessShutdown[A] = FutureUnlessShutdown.pure(a)

    override def value[A](fa: FutureUnlessShutdown[A]): UnlessShutdown[A] = fa.unwrap.futureValue
  }
}

class EitherTAbsorbUnlessShutdownTest extends AbsorbUnlessShutdownTest with HasExecutionContext {
  "EitherT" when {
    "applied to FutureUnlessShutdown" should {
      val fixture = new EitherTAbsorbUnlessShutdownTest.Fixture[FutureUnlessShutdown, String](
        FutureUnlessShutdownAbsorbUnlessShutdownTest.Fixture
      )

      behave like absorbUnlessShutdown(
        AbsorbUnlessShutdown[EitherT[FutureUnlessShutdown, String, *]],
        fixture,
      )
    }

    "applied to UnlessShutdown" should {
      val fixture = new EitherTAbsorbUnlessShutdownTest.Fixture[UnlessShutdown, String](
        UnlessShutdownAbsorbUnlessShutdownTest.Fixture
      )
      behave like absorbUnlessShutdown(
        AbsorbUnlessShutdown[EitherT[UnlessShutdown, String, *]],
        fixture,
      )
    }
  }
}

object EitherTAbsorbUnlessShutdownTest {
  private[lifecycle] class Fixture[F[_], L](fixture: AbsorbUnlessShutdownTest.Fixture[F])
      extends AbsorbUnlessShutdownTest.Fixture[EitherT[F, L, *]]
      with EitherValues {
    override def pure[A](a: A): EitherT[F, L, A] = EitherT(fixture.pure(Right(a)))

    override def value[A](fa: EitherT[F, L, A]): UnlessShutdown[A] =
      fixture.value(fa.value).map(_.value)
  }
}

class OptionTAbsorbUnlessShutdownTest extends AbsorbUnlessShutdownTest with HasExecutionContext {
  "OptionT" when {
    "applied to FutureUnlessShutdown" should {
      val fixture = new OptionTAbsorbUnlessShutdownTest.Fixture(
        FutureUnlessShutdownAbsorbUnlessShutdownTest.Fixture
      )

      behave like absorbUnlessShutdown(
        AbsorbUnlessShutdown[OptionT[FutureUnlessShutdown, *]],
        fixture,
      )
    }

    "applied to UnlessShutdown" should {
      val fixture =
        new OptionTAbsorbUnlessShutdownTest.Fixture(UnlessShutdownAbsorbUnlessShutdownTest.Fixture)
      behave like absorbUnlessShutdown(
        AbsorbUnlessShutdown[OptionT[UnlessShutdown, *]],
        fixture,
      )
    }
  }
}

object OptionTAbsorbUnlessShutdownTest {
  private[lifecycle] class Fixture[F[_]](fixture: AbsorbUnlessShutdownTest.Fixture[F])
      extends lifecycle.AbsorbUnlessShutdownTest.Fixture[OptionT[F, *]]
      with OptionValues {
    override def pure[A](a: A): OptionT[F, A] = OptionT(fixture.pure(Some(a)))

    override def value[A](fa: OptionT[F, A]): UnlessShutdown[A] =
      fixture.value(fa.value).map(_.value)
  }
}
