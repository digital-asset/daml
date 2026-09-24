// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.{EitherT, Nested, OptionT}
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ScalaFuturesWithPatience, lifecycle}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, OptionValues}

import scala.concurrent.Future

trait CanAbortDueToShutdownTest extends AnyWordSpec with BaseTest {

  def canAbortDueToShutdown[F[_]](
      F: CanAbortDueToShutdown[F],
      fixture: CanAbortDueToShutdownTest.Fixture[F],
  ): Unit = {
    "abort" should {
      "return AbortedDueToShutdown" in {
        fixture.value(F.abort) shouldBe AbortedDueToShutdown
      }
    }

    "absorbOuter" should {
      "embed AbortedDueToShutdown" in {
        fixture.value(F.absorbOuter(AbortedDueToShutdown)) shouldBe AbortedDueToShutdown
      }

      "embed Outcomes" in {
        val fa = fixture.pure(42)
        fixture.value(F.absorbOuter(Outcome(fa))) shouldBe Outcome(42)
      }
    }
  }
}

object CanAbortDueToShutdownTest {
  trait Fixture[F[_]] {
    def pure[A](a: A): F[A]
    def value[A](fa: F[A]): UnlessShutdown[A]
  }

  private[lifecycle] object FixtureFuture
      extends CanAbortDueToShutdownTest.Fixture[Future]
      with ScalaFuturesWithPatience {
    override def pure[A](a: A): Future[A] = Future.successful(a)

    override def value[A](fa: Future[A]): UnlessShutdown[A] = Outcome(fa.futureValue)
  }
}

class UnlessShutdownCanAbortDueToShutdownTest extends CanAbortDueToShutdownTest {
  "UnlessShutdown" should {

    behave like canAbortDueToShutdown(
      CanAbortDueToShutdown[UnlessShutdown],
      UnlessShutdownCanAbortDueToShutdownTest.Fixture,
    )
  }
}

object UnlessShutdownCanAbortDueToShutdownTest {
  private[lifecycle] object Fixture extends CanAbortDueToShutdownTest.Fixture[UnlessShutdown] {
    override def pure[A](a: A): UnlessShutdown[A] = Outcome(a)

    override def value[A](fa: UnlessShutdown[A]): UnlessShutdown[A] = fa
  }
}

class FutureUnlessShutdownCanAbortDueToShutdownTest
    extends CanAbortDueToShutdownTest
    with HasExecutionContext {

  "FutureUnlessShutdown" should {

    FutureUnlessShutdown.unit.unwrap.futureValue

    behave like canAbortDueToShutdown(
      CanAbortDueToShutdown[FutureUnlessShutdown],
      FutureUnlessShutdownCanAbortDueToShutdownTest.Fixture,
    )
  }
}

object FutureUnlessShutdownCanAbortDueToShutdownTest {
  private[lifecycle] object Fixture
      extends CanAbortDueToShutdownTest.Fixture[FutureUnlessShutdown]
      with ScalaFuturesWithPatience {
    override def pure[A](a: A): FutureUnlessShutdown[A] = FutureUnlessShutdown.pure(a)

    override def value[A](fa: FutureUnlessShutdown[A]): UnlessShutdown[A] = fa.unwrap.futureValue
  }
}

class EitherTCanAbortDueToShutdownTest extends CanAbortDueToShutdownTest with HasExecutionContext {
  "EitherT" when {
    "applied to FutureUnlessShutdown" should {
      val fixture = new EitherTCanAbortDueToShutdownTest.Fixture[FutureUnlessShutdown, String](
        FutureUnlessShutdownCanAbortDueToShutdownTest.Fixture
      )

      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[EitherT[FutureUnlessShutdown, String, *]],
        fixture,
      )
    }

    "applied to UnlessShutdown" should {
      val fixture = new EitherTCanAbortDueToShutdownTest.Fixture[UnlessShutdown, String](
        UnlessShutdownCanAbortDueToShutdownTest.Fixture
      )
      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[EitherT[UnlessShutdown, String, *]],
        fixture,
      )
    }
  }
}

object EitherTCanAbortDueToShutdownTest {
  private[lifecycle] class Fixture[F[_], L](fixture: CanAbortDueToShutdownTest.Fixture[F])
      extends CanAbortDueToShutdownTest.Fixture[EitherT[F, L, *]]
      with EitherValues {
    override def pure[A](a: A): EitherT[F, L, A] = EitherT(fixture.pure(Right(a)))

    override def value[A](fa: EitherT[F, L, A]): UnlessShutdown[A] =
      fixture.value(fa.value).map(_.value)
  }
}

class OptionTCanAbortDueToShutdownTest extends CanAbortDueToShutdownTest with HasExecutionContext {
  "OptionT" when {
    "applied to FutureUnlessShutdown" should {
      val fixture = new OptionTCanAbortDueToShutdownTest.Fixture(
        FutureUnlessShutdownCanAbortDueToShutdownTest.Fixture
      )

      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[OptionT[FutureUnlessShutdown, *]],
        fixture,
      )
    }

    "applied to UnlessShutdown" should {
      val fixture = new OptionTCanAbortDueToShutdownTest.Fixture(
        UnlessShutdownCanAbortDueToShutdownTest.Fixture
      )
      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[OptionT[UnlessShutdown, *]],
        fixture,
      )
    }
  }
}

object OptionTCanAbortDueToShutdownTest {
  private[lifecycle] class Fixture[F[_]](fixture: CanAbortDueToShutdownTest.Fixture[F])
      extends lifecycle.CanAbortDueToShutdownTest.Fixture[OptionT[F, *]]
      with OptionValues {
    override def pure[A](a: A): OptionT[F, A] = OptionT(fixture.pure(Some(a)))

    override def value[A](fa: OptionT[F, A]): UnlessShutdown[A] =
      fixture.value(fa.value).map(_.value)
  }
}

class CheckedTCanAbortDueToShutdownTest extends CanAbortDueToShutdownTest with HasExecutionContext {
  "CheckedT" when {
    "applied to FutureUnlessShutdown" should {
      val fixture = new CheckedTCanAbortDueToShutdownTest.Fixture[FutureUnlessShutdown, Unit, Unit](
        FutureUnlessShutdownCanAbortDueToShutdownTest.Fixture
      )

      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[CheckedT[FutureUnlessShutdown, Unit, Unit, *]],
        fixture,
      )
    }

    "applied to UnlessShutdown" should {
      val fixture = new CheckedTCanAbortDueToShutdownTest.Fixture[UnlessShutdown, Unit, Unit](
        UnlessShutdownCanAbortDueToShutdownTest.Fixture
      )
      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[CheckedT[UnlessShutdown, Unit, Unit, *]],
        fixture,
      )
    }
  }
}

object CheckedTCanAbortDueToShutdownTest {
  private[lifecycle] class Fixture[F[_], A, N](fixture: CanAbortDueToShutdownTest.Fixture[F])
      extends lifecycle.CanAbortDueToShutdownTest.Fixture[CheckedT[F, A, N, *]]
      with OptionValues {
    override def pure[X](a: X): CheckedT[F, A, N, X] = CheckedT(fixture.pure(Checked.result(a)))

    override def value[X](fa: CheckedT[F, A, N, X]): UnlessShutdown[X] =
      fixture.value(fa.value).map(_.getResult.value)
  }
}

class NestedCanAbortDueToShutdownTest extends CanAbortDueToShutdownTest with HasExecutionContext {
  "Nested" when {
    "applied to FutureUnlessShutdown and Future" should {
      val fixture = new NestedCanAbortDueToShutdownTest.Fixture(
        FutureUnlessShutdownCanAbortDueToShutdownTest.Fixture,
        CanAbortDueToShutdownTest.FixtureFuture,
      )

      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[Nested[FutureUnlessShutdown, Future, *]],
        fixture,
      )
    }

    "applied to Future and FutureUnlessShutdown" should {
      val fixture = new NestedCanAbortDueToShutdownTest.Fixture(
        CanAbortDueToShutdownTest.FixtureFuture,
        FutureUnlessShutdownCanAbortDueToShutdownTest.Fixture,
      )
      behave like canAbortDueToShutdown(
        CanAbortDueToShutdown[Nested[Future, FutureUnlessShutdown, *]],
        fixture,
      )
    }
  }
}

object NestedCanAbortDueToShutdownTest {
  private[lifecycle] class Fixture[F[_], G[_]](
      F: CanAbortDueToShutdownTest.Fixture[F],
      G: CanAbortDueToShutdownTest.Fixture[G],
  ) extends lifecycle.CanAbortDueToShutdownTest.Fixture[Nested[F, G, *]] {
    override def pure[A](a: A): Nested[F, G, A] = Nested(F.pure(G.pure(a)))

    override def value[A](fa: Nested[F, G, A]): UnlessShutdown[A] =
      F.value(fa.value).flatMap(G.value)
  }
}
