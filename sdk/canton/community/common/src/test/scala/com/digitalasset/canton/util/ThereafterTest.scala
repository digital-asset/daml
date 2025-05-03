// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{Chain, EitherT, Nested, OptionT}
import cats.syntax.either.*
import cats.syntax.traverse.*
import cats.{Applicative, Functor, Id, Traverse}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.unused
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

trait ThereafterTest extends AnyWordSpec with BaseTest {

  def thereafter[F[_], Content[_], Shape](
      sut: Thereafter.Aux[F, Content, Shape],
      fixture: ThereafterTest.Fixture[F, Content, Shape],
  ): Unit = {
    "thereafter" should {
      "run the body once" in {
        forEvery(fixture.contents) { content =>
          val runCount = new AtomicInteger(0)
          val x = fixture.fromContent(content)
          val res = sut.thereafter(x) { _ =>
            fixture.isCompleted(x) shouldBe true
            runCount.incrementAndGet()
            ()
          }
          fixture.await(res) shouldBe content
          runCount.get shouldBe 1
        }
      }

      "run the body after a failure" in {
        val ex = new RuntimeException("EXCEPTION")
        val x = fixture.fromTry(Failure[Unit](ex))
        val res = sut.thereafter(x) { content =>
          fixture.isCompleted(x) shouldBe true
          Try(fixture.theContent(content)) shouldBe Failure(ex)
          ()
        }
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "propagate an exception in the body" in {
        val ex = new RuntimeException("BODY FAILURE")
        val x = fixture.fromTry(Success(()))
        val res = sut.thereafter(x)(_ => throw ex)
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "chain failure and body failure via suppression" in {
        val ex1 = new RuntimeException("EXCEPTION")
        val ex2 = new RuntimeException("BODY FAILURE")
        val x = fixture.fromTry(Failure[Unit](ex1))
        val res = sut.thereafter(x)(_ => throw ex2)
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex1)
        ex1.getSuppressed should contain(ex2)
      }

      "chain body exceptions via suppression" in {
        val ex1 = new RuntimeException("BODY FAILURE 1")
        val ex2 = new RuntimeException("BODY FAILURE 2")
        val x = fixture.fromTry(Success(()))
        val y = sut.thereafter(x)(_ => throw ex1)
        val res = sut.thereafter(y)(_ => throw ex2)
        val z = fixture.await(res)
        Try(fixture.theContent(z)) shouldBe Failure(ex1)
        ex1.getSuppressed should contain(ex2)
      }

      "rethrow the exception in the body" in {
        val ex = new RuntimeException("FAILURE")
        val x = fixture.fromTry(Failure[Unit](ex))
        val res = sut.thereafter(x)(_ => throw ex)
        val z = fixture.await(res)
        Try(fixture.theContent(z)) shouldBe Failure(ex)
      }

      "call the body with the right content" in {
        forAll(fixture.contents) { content =>
          val called = new AtomicReference[Seq[Content[fixture.X]]](Seq.empty)
          val x = fixture.fromContent(content)
          val res = sut.thereafter(x) { c =>
            called.updateAndGet(_ :+ c).discard
          }
          val z = fixture.await(res)
          z shouldBe content
          called.get() shouldBe Seq(content)
        }
      }
    }

    "withShape" should {
      "assemble the content" in {
        forEvery(fixture.contents) { content =>
          fixture.splitContent(content) match {
            case Some((shape, x)) => sut.withShape(shape, x) shouldBe content
            case None => succeed
          }
        }
      }
    }
  }

  def thereafterAsync[F[_], Content[_], Shape](
      sut: ThereafterAsync.Aux[F, Content, Shape],
      fixture: ThereafterAsyncTest.Fixture[F, Content, Shape],
  )(implicit ec: ExecutionContext): Unit = {
    "thereafter" should {

      behave like thereafter(sut, fixture)

      "run the body only afterwards" in {
        val runCount = new AtomicInteger(0)
        val promise = Promise[Int]()
        val x = fixture.fromFuture(promise.future)
        val res = sut.thereafter(x) { content =>
          promise.future.isCompleted shouldBe true
          fixture.theContent(content) shouldBe 42
          runCount.incrementAndGet()
          ()
        }
        promise.success(42)
        val y = fixture.await(res)
        fixture.theContent(y) shouldBe 42
        runCount.get shouldBe 1
      }

      "run the body even after failure" in {
        val ex = new RuntimeException("EXCEPTION")
        val promise = Promise[Unit]()
        val x = fixture.fromFuture(promise.future)
        val res = sut.thereafter(x) { content =>
          fixture.isCompleted(x) shouldBe true
          promise.future.isCompleted shouldBe true
          Try(fixture.theContent(content)) shouldBe Failure(ex)
          ()
        }
        promise.failure(ex)
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }
    }

    "thereafterF" should {
      "run the body once upon completion" in {
        forEvery(fixture.contents) { content =>
          val runCount = new AtomicInteger(0)
          val bodyRunCount = new AtomicInteger(0)
          val x = fixture.fromContent(content)
          val res = sut.thereafterF(x) { _ =>
            fixture.isCompleted(x) shouldBe true
            runCount.incrementAndGet()
            Future {
              bodyRunCount.incrementAndGet()
              ()
            }
          }
          fixture.await(res) shouldBe content
          runCount.get shouldBe 1
          bodyRunCount.get shouldBe 1
        }
      }

      "run the body only afterwards" in {
        val runCount = new AtomicInteger(0)
        val promise = Promise[Int]()
        val x = fixture.fromFuture(promise.future)
        val res = sut.thereafterF(x) { content =>
          promise.future.isCompleted shouldBe true
          Future {
            fixture.theContent(content) shouldBe 42
            runCount.incrementAndGet()
            ()
          }
        }
        promise.success(42)
        val y = fixture.await(res)
        fixture.theContent(y) shouldBe 42
        runCount.get shouldBe 1
      }

      "propagate a synchronous exception in the body" in {
        val ex = new RuntimeException("BODY FAILURE")
        val x = fixture.fromFuture(Future.successful(()))
        val res = sut.thereafterF(x)(_ => throw ex)
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "propagate an asynchronous exception in the body" in {
        val ex = new RuntimeException("BODY FAILURE")
        val x = fixture.fromFuture(Future.successful(()))
        val res = sut.thereafterF(x)(_ => Future.failed(ex))
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "chain failure and body failure via suppression" in {
        val ex1 = new RuntimeException("EXCEPTION")
        val ex2 = new RuntimeException("BODY FAILURE")
        val x = fixture.fromFuture(Future.failed[Unit](ex1))
        val res = sut.thereafterF(x)(_ => Future.failed(ex2))
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex1)
        ex1.getSuppressed should contain(ex2)
      }

      "chain body exceptions via suppression" in {
        val ex1 = new RuntimeException("BODY FAILURE 1")
        val ex2 = new RuntimeException("BODY FAILURE 2")
        val x = fixture.fromFuture(Future.successful(()))
        val y = sut.thereafterF(x)(_ => Future.failed(ex1))
        val res = sut.thereafterF(y)(_ => Future.failed(ex2))
        val z = fixture.await(res)
        Try(fixture.theContent(z)) shouldBe Failure(ex1)
        ex1.getSuppressed should contain(ex2)
      }

      "rethrow the exception in the body" in {
        val ex = new RuntimeException("FAILURE")
        val x = fixture.fromFuture(Future.failed[Unit](ex))
        val res = sut.thereafterF(x)(_ => Future.failed(ex))
        val z = fixture.await(res)
        Try(fixture.theContent(z)) shouldBe Failure(ex)
      }

      "call the body with the right content" in {
        forAll(fixture.contents) { content =>
          val called = new AtomicReference[Seq[Content[fixture.X]]](Seq.empty)
          val x = fixture.fromContent(content)
          val res = sut.thereafterF(x) { c =>
            Future.successful(called.updateAndGet(_ :+ c).discard)
          }
          val z = fixture.await(res)
          z shouldBe content
          called.get() shouldBe Seq(content)
        }
      }
    }
  }
}

object ThereafterTest {

  trait Fixture[F[_], Content[_], Shape] {
    type X
    def fromTry[A](x: Try[A]): F[A]
    def fromContent[A](content: Content[A]): F[A]
    def isCompleted[A](x: F[A]): Boolean
    def await[A](x: F[A]): Content[A]
    def contents: Seq[Content[X]]
    def theContent[A](content: Content[A]): A
    def splitContent[A](content: Content[A]): Option[(Shape, A)]
  }

  /** Test that the scala compiler finds the [[Thereafter]] implicits */
  @unused
  private def implicitResolutionTest(): Unit = {
    import Thereafter.syntax.*

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit val ec: ExecutionContext = null

    Id(5).thereafter((x: Int) => ()).discard

    EitherT.rightT[Future, Unit]("EitherT Future").thereafter(_ => ()).discard
    EitherT
      .rightT[FutureUnlessShutdown, Unit]("EitherT FutureUnlessShutdown")
      .thereafter(_ => ())
      .discard
    OptionT.pure[Future]("OptionT Future").thereafter(_ => ()).discard
    OptionT.pure[FutureUnlessShutdown]("OptionT FutureUnlessShutdown").thereafter(_ => ()).discard

    // Type inference copes even with several Thereafter transformers
    EitherT
      .rightT[EitherT[Try, Unit, *], Unit]("EitherT EitherT Try")
      .thereafter(_ => ())
      .discard
    OptionT.pure[OptionT[Try, *]]("OptionT OptionT Try").thereafter(_ => ()).discard
    Nested(
      EitherT.pure[Try, Unit](OptionT.pure[Try]("Nested EitherT Try OptionT Try"))
    ).thereafter(_ => ()).discard
  }
}

object ThereafterAsyncTest {

  trait Fixture[F[_], Content[_], Shape] extends ThereafterTest.Fixture[F, Content, Shape] {
    override def fromTry[A](x: Try[A]): F[A] = fromFuture(Future.fromTry(x))
    def fromFuture[A](x: Future[A]): F[A]
  }

  /** Test that the scala compiler finds the [[ThereafterAsync]] implicits */
  @unused
  private def implicitResolutionTest(): Unit = {
    import Thereafter.syntax.*

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit val ec: ExecutionContext = null

    EitherT.rightT[Future, Unit]("EitherT Future").thereafterF(_ => Future.unit).discard
    EitherT
      .rightT[FutureUnlessShutdown, Unit]("EitherT FutureUnlessShutdown")
      .thereafterF(_ => Future.unit)
      .discard
    OptionT.pure[Future]("OptionT Future").thereafterF(_ => Future.unit).discard
    OptionT
      .pure[FutureUnlessShutdown]("OptionT FutureUnlessShutdown")
      .thereafterF(_ => Future.unit)
      .discard

    // Type inference copes even with several Thereafter transformers
    EitherT
      .rightT[EitherT[Future, Unit, *], Unit]("EitherT EitherT Future")
      .thereafterF(_ => Future.unit)
      .discard
    OptionT.pure[OptionT[Future, *]]("OptionT OptionT Future").thereafterF(_ => Future.unit).discard
    Nested(
      EitherT.pure[Future, Unit](OptionT.pure[Future]("Nested EitherT Future OptionT Future"))
    ).thereafterF(_ => Future.unit).discard
  }
}

class TryThereafterTest extends ThereafterTest {
  "Try" should {
    behave like thereafter(Thereafter[Try], TryThereafterTest.fixture)
  }
}

object TryThereafterTest {
  lazy val fixture: ThereafterTest.Fixture[Try, Try, Unit] =
    new ThereafterTest.Fixture[Try, Try, Unit] {
      override type X = Any
      override def fromTry[A](x: Try[A]): Try[A] = x
      override def fromContent[A](content: Try[A]): Try[A] = content
      override def isCompleted[A](x: Try[A]): Boolean = true
      override def await[A](x: Try[A]): Try[A] = x
      override def contents: Seq[Try[X]] = TryThereafterTest.contents
      override def theContent[A](content: Try[A]): A =
        content.fold(err => throw err, Predef.identity)
      override def splitContent[A](content: Try[A]): Option[(Unit, A)] =
        content.toOption.map(() -> _)
    }
  lazy val contents: Seq[Try[Any]] =
    Seq(TryUtil.unit, Success(5), Failure(new RuntimeException("failure")))
}

class FutureThereafterTest extends ThereafterTest with HasExecutionContext {
  "Future" should {
    behave like thereafter(Thereafter[Future], FutureThereafterTest.fixture)
    behave like thereafterAsync(ThereafterAsync[Future], FutureThereafterTest.fixture)
  }
}

object FutureThereafterTest {
  lazy val fixture: ThereafterAsyncTest.Fixture[Future, Try, Unit] =
    new ThereafterAsyncTest.Fixture[Future, Try, Unit] {
      override type X = Any
      override def fromFuture[A](x: Future[A]): Future[A] = x
      override def fromContent[A](content: Try[A]): Future[A] =
        Future.fromTry(content)
      override def isCompleted[A](x: Future[A]): Boolean = x.isCompleted
      override def await[A](x: Future[A]): Try[A] = Try(blocking {
        Await.result(x, Duration.Inf)
      })
      override def contents: Seq[Try[X]] = TryThereafterTest.contents
      override def theContent[A](content: Try[A]): A =
        content.fold(err => throw err, Predef.identity)
      override def splitContent[A](content: Try[A]): Option[(Unit, A)] =
        content.toOption.map(() -> _)
    }
}

class FutureUnlessShutdownThereafterTest extends ThereafterTest with HasExecutionContext {
  "FutureUnlessShutdown" should {
    behave like thereafterAsync(
      ThereafterAsync[FutureUnlessShutdown],
      FutureUnlessShutdownThereafterTest.fixture,
    )
  }
}

object FutureUnlessShutdownThereafterTest {
  def fixture(implicit
      ec: ExecutionContext
  ): ThereafterAsyncTest.Fixture[FutureUnlessShutdown, Lambda[a => Try[UnlessShutdown[a]]], Unit] =
    new ThereafterAsyncTest.Fixture[
      FutureUnlessShutdown,
      Lambda[a => Try[UnlessShutdown[a]]],
      Unit,
    ] {
      override type X = Any
      override def fromFuture[A](x: Future[A]): FutureUnlessShutdown[A] =
        FutureUnlessShutdown.outcomeF(x)
      override def fromContent[A](content: Try[UnlessShutdown[A]]): FutureUnlessShutdown[A] =
        FutureUnlessShutdown(Future.fromTry(content))
      override def isCompleted[A](x: FutureUnlessShutdown[A]): Boolean = x.unwrap.isCompleted
      override def await[A](
          x: FutureUnlessShutdown[A]
      ): Try[UnlessShutdown[A]] =
        Try(blocking {
          Await.result(x.unwrap, Duration.Inf)
        })
      override def contents: Seq[Try[UnlessShutdown[X]]] =
        Success(UnlessShutdown.AbortedDueToShutdown) +:
          TryThereafterTest.contents.map(_.map(UnlessShutdown.Outcome(_)))
      override def theContent[A](content: Try[UnlessShutdown[A]]): A =
        content.fold(err => throw err, _.onShutdown(throw new NoSuchElementException("No outcome")))
      override def splitContent[A](content: Try[UnlessShutdown[A]]): Option[(Unit, A)] =
        content.toOption.flatMap {
          case UnlessShutdown.Outcome(x) => Some(() -> x)
          case UnlessShutdown.AbortedDueToShutdown => None
        }
    }
}

class EitherTThereafterTest extends ThereafterTest with HasExecutionContext {
  "EitherT" when {
    "applied to Try" should {
      behave like thereafter(
        Thereafter[EitherT[Try, Unit, *]],
        EitherTThereafterTest.fixture(TryThereafterTest.fixture, NonEmpty(Seq, ())),
      )
    }

    "applied to Future" should {
      behave like thereafterAsync(
        ThereafterAsync[EitherT[Future, Unit, *]],
        EitherTThereafterTest.asyncFixture(FutureThereafterTest.fixture, NonEmpty(Seq, ())),
      )
    }

    "applied to FutureUnlessShutdown" should {
      implicit val appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]
      behave like thereafterAsync(
        ThereafterAsync[EitherT[FutureUnlessShutdown, String, *]],
        EitherTThereafterTest.asyncFixture(
          FutureUnlessShutdownThereafterTest.fixture,
          NonEmpty(Seq, "left", "another left"),
        ),
      )
    }
  }
}

object EitherTThereafterTest {
  private class EitherTFixture[F[_], Content[_], Shape, E](
      base: ThereafterTest.Fixture[F, Content, Shape],
      lefts: NonEmpty[Seq[E]],
  )(implicit M: Functor[F], C: Applicative[Content])
      extends ThereafterTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]], Shape] {
    override type X = Any
    override def fromTry[A](x: Try[A]): EitherT[F, E, A] = EitherT(M.map(base.fromTry(x))(Right(_)))
    override def fromContent[A](content: Content[Either[E, A]]): EitherT[F, E, A] =
      EitherT(base.fromContent(content))
    override def isCompleted[A](x: EitherT[F, E, A]): Boolean = base.isCompleted(x.value)
    override def await[A](x: EitherT[F, E, A]): Content[Either[E, A]] =
      base.await(x.value)
    override def contents: Seq[Content[Either[E, X]]] =
      lefts.map(l => C.pure(Either.left[E, X](l))) ++ base.contents.map(
        C.map(_)(Either.right[E, X](_))
      )
    override def theContent[A](content: Content[Either[E, A]]): A =
      base
        .theContent(content)
        .valueOr(l => throw new NoSuchElementException(s"Left($l) is not a Right"))
    override def splitContent[A](content: Content[Either[E, A]]): Option[(Shape, A)] =
      base.splitContent(content).flatMap(_.traverse(_.toOption))
  }

  def fixture[F[_], Content[_], Shape, E](
      base: ThereafterTest.Fixture[F, Content, Shape],
      lefts: NonEmpty[Seq[E]],
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]], Shape] =
    new EitherTFixture[F, Content, Shape, E](base, lefts)

  private class EitherTAsyncFixture[F[_], Content[_], Shape, E](
      base: ThereafterAsyncTest.Fixture[F, Content, Shape],
      lefts: NonEmpty[Seq[E]],
  )(implicit M: Functor[F], C: Applicative[Content])
      extends EitherTFixture[F, Content, Shape, E](base, lefts)
      with ThereafterAsyncTest.Fixture[
        EitherT[F, E, *],
        Lambda[a => Content[Either[E, a]]],
        Shape,
      ] {
    override def fromFuture[A](x: Future[A]): EitherT[F, E, A] =
      EitherT(M.map(base.fromFuture(x))(Right(_)))
  }

  def asyncFixture[F[_], Content[_], Shape, E](
      base: ThereafterAsyncTest.Fixture[F, Content, Shape],
      lefts: NonEmpty[Seq[E]],
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterAsyncTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]], Shape] =
    new EitherTAsyncFixture(base, lefts)
}

class OptionTThereafterTest extends ThereafterTest with HasExecutionContext {
  "OptionT" when {
    "applied to Try" should {
      behave like thereafter(
        Thereafter[OptionT[Try, *]],
        OptionTThereafterTest.fixture(TryThereafterTest.fixture),
      )
    }

    "applied to Future" should {
      behave like thereafterAsync(
        ThereafterAsync[OptionT[Future, *]],
        OptionTThereafterTest.asyncFixture(FutureThereafterTest.fixture),
      )
    }

    "applied to FutureUnlessShutdown" should {
      implicit val appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]
      behave like thereafterAsync(
        ThereafterAsync[OptionT[FutureUnlessShutdown, *]],
        OptionTThereafterTest.asyncFixture(FutureUnlessShutdownThereafterTest.fixture),
      )
    }
  }
}

object OptionTThereafterTest {
  private class OptionTFixture[F[_], Content[_], Shape](
      base: ThereafterTest.Fixture[F, Content, Shape]
  )(implicit M: Functor[F], C: Applicative[Content])
      extends ThereafterTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]], Shape] {
    override type X = Any
    override def fromTry[A](x: Try[A]): OptionT[F, A] = OptionT(M.map(base.fromTry(x))(Option(_)))
    override def fromContent[A](content: Content[Option[A]]): OptionT[F, A] =
      OptionT(base.fromContent(content))
    override def isCompleted[A](x: OptionT[F, A]): Boolean = base.isCompleted(x.value)
    override def await[A](x: OptionT[F, A]): Content[Option[A]] =
      base.await(x.value)
    override def contents: Seq[Content[Option[X]]] =
      base.contents.map(C.map(_)(Option[X](_))) :+ C.pure(None)
    override def theContent[A](content: Content[Option[A]]): A =
      base
        .theContent(content)
        .getOrElse(throw new NoSuchElementException("The option should not be empty"))
    override def splitContent[A](content: Content[Option[A]]): Option[(Shape, A)] =
      base.splitContent(content).flatMap(_.sequence)
  }

  def fixture[F[_], Content[_], Shape](base: ThereafterTest.Fixture[F, Content, Shape])(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]], Shape] =
    new OptionTFixture[F, Content, Shape](base)

  private class OptionTAsyncFixture[F[_], Content[_], Shape](
      base: ThereafterAsyncTest.Fixture[F, Content, Shape]
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ) extends OptionTFixture[F, Content, Shape](base)
      with ThereafterAsyncTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]], Shape] {
    override def fromFuture[A](x: Future[A]): OptionT[F, A] =
      OptionT(M.map(base.fromFuture(x))(Option(_)))
  }

  def asyncFixture[F[_], Content[_], Shape](base: ThereafterAsyncTest.Fixture[F, Content, Shape])(
      implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterAsyncTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]], Shape] =
    new OptionTAsyncFixture[F, Content, Shape](base)
}

class CheckedTThereafterTest extends ThereafterTest with HasExecutionContext {
  "CheckedT" when {
    "applied to Try" should {
      behave like thereafter(
        Thereafter[CheckedT[Try, Unit, String, *]],
        CheckedTThereafterTest.fixture(
          TryThereafterTest.fixture,
          NonEmpty(Seq, ()),
          NonEmpty(Seq, "ab", "cd"),
        ),
      )
    }

    "applied to Future" should {
      behave like thereafterAsync(
        ThereafterAsync[CheckedT[Future, Unit, String, *]],
        CheckedTThereafterTest.asyncFixture(
          FutureThereafterTest.fixture,
          NonEmpty(Seq, ()),
          NonEmpty(Seq, "ab", "cd"),
        ),
      )
    }

    "applied to FutureUnlessShutdown" should {
      implicit val appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]
      behave like thereafterAsync(
        ThereafterAsync[CheckedT[FutureUnlessShutdown, String, String, *]],
        CheckedTThereafterTest.asyncFixture(
          FutureUnlessShutdownThereafterTest.fixture,
          NonEmpty(Seq, "abort", "another abort"),
          NonEmpty(Seq, "nonabort", "another nonabort"),
        ),
      )
    }
  }
}

object CheckedTThereafterTest {
  private class CheckedTFixture[F[_], Content[_], Shape, E, N](
      base: ThereafterTest.Fixture[F, Content, Shape],
      aborts: NonEmpty[Seq[E]],
      nonaborts: NonEmpty[Seq[N]],
  )(implicit M: Functor[F], C: Applicative[Content])
      extends ThereafterTest.Fixture[
        CheckedT[F, E, N, *],
        Lambda[a => Content[Checked[E, N, a]]],
        (Shape, Chain[N]),
      ] {
    override type X = Any
    override def fromTry[A](x: Try[A]): CheckedT[F, E, N, A] = CheckedT(
      M.map(base.fromTry(x))(Checked.result)
    )
    override def fromContent[A](content: Content[Checked[E, N, A]]): CheckedT[F, E, N, A] =
      CheckedT(base.fromContent(content))
    override def isCompleted[A](x: CheckedT[F, E, N, A]): Boolean = base.isCompleted(x.value)
    override def await[A](x: CheckedT[F, E, N, A]): Content[Checked[E, N, A]] =
      base.await(x.value)
    override def contents: Seq[Content[Checked[E, N, X]]] =
      aborts.forgetNE.flatMap(e =>
        nonaborts.inits.map(ns => C.pure(Checked.Abort(e, Chain.fromSeq(ns)): Checked[E, N, X]))
      ) ++ base.contents.flatMap(x =>
        nonaborts.tails.map(ns => C.map(x)(Checked.Result(Chain.fromSeq(ns), _): Checked[E, N, X]))
      )
    override def theContent[A](content: Content[Checked[E, N, A]]): A =
      base.theContent(content) match {
        case Checked.Result(_, x) => x
        case abort @ Checked.Abort(_, _) =>
          throw new NoSuchElementException(s"$abort is not a Result")
      }
    override def splitContent[A](
        content: Content[Checked[E, N, A]]
    ): Option[((Shape, Chain[N]), A)] =
      base.splitContent(content).flatMap { case (fshape, checked) =>
        checked match {
          case Checked.Result(ns, x) => Some((fshape, ns) -> x)
          case _ => None
        }
      }
  }

  def fixture[F[_], Content[_], Shape, E, N](
      base: ThereafterTest.Fixture[F, Content, Shape],
      aborts: NonEmpty[Seq[E]],
      nonaborts: NonEmpty[Seq[N]],
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterTest.Fixture[
    CheckedT[F, E, N, *],
    Lambda[a => Content[Checked[E, N, a]]],
    (Shape, Chain[N]),
  ] = new CheckedTFixture[F, Content, Shape, E, N](base, aborts, nonaborts)

  private class CheckedTAsyncFixture[F[_], Content[_], Shape, E, N](
      base: ThereafterAsyncTest.Fixture[F, Content, Shape],
      aborts: NonEmpty[Seq[E]],
      nonaborts: NonEmpty[Seq[N]],
  )(implicit M: Functor[F], C: Applicative[Content])
      extends CheckedTFixture[F, Content, Shape, E, N](base, aborts, nonaborts)
      with ThereafterAsyncTest.Fixture[
        CheckedT[F, E, N, *],
        Lambda[a => Content[Checked[E, N, a]]],
        (Shape, Chain[N]),
      ] {
    override def fromFuture[A](x: Future[A]): CheckedT[F, E, N, A] =
      CheckedT(M.map(base.fromFuture(x))(Checked.result))
  }

  def asyncFixture[F[_], Content[_], Shape, E, N](
      base: ThereafterAsyncTest.Fixture[F, Content, Shape],
      aborts: NonEmpty[Seq[E]],
      nonaborts: NonEmpty[Seq[N]],
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterAsyncTest.Fixture[
    CheckedT[F, E, N, *],
    Lambda[a => Content[Checked[E, N, a]]],
    (Shape, Chain[N]),
  ] = new CheckedTAsyncFixture(base, aborts, nonaborts)
}

class NestedThereafterTest extends ThereafterTest with HasExecutionContext {
  "Nested" when {
    // These test cases covers the following aspects of nesting:
    // - Combine two Thereafter instances where at least one of them does not have
    //   a ThereafterAsync instance (Future/Try)
    // - Combine two ThereafterAsync instances (Future, FutureUnlessShutdown)
    // - Check that shapes are properly treated with (CheckedT)
    // - Use every other type constructor with a Thereafter instance (other than Id
    //   because the exception handling is very different for Id and the Thereafter
    //   test cases don't make sense for Id), including Nested inside Nested.

    "applied to Future and Try" should {
      behave like thereafter(
        Thereafter[Nested[Future, Try, *]],
        NestedThereafterTest.fixture(
          FutureThereafterTest.fixture,
          TryThereafterTest.fixture,
        ),
      )
    }

    "applied to CheckedT-Future and CheckedT-Try" should {
      implicit val traverse: Traverse[Lambda[a => Try[Checked[Unit, String, a]]]] =
        Traverse[Try].compose(Traverse[Checked[Unit, String, *]])

      behave like thereafter(
        Thereafter[Nested[CheckedT[Future, Unit, String, *], CheckedT[Try, Int, Double, *], *]],
        NestedThereafterTest.fixture(
          CheckedTThereafterTest.fixture(
            FutureThereafterTest.fixture,
            NonEmpty(Seq, ()),
            NonEmpty(Seq, "ab", "cd"),
          ),
          CheckedTThereafterTest.fixture(
            TryThereafterTest.fixture,
            NonEmpty(Seq, 5),
            NonEmpty(Seq, 1.0, 2.0),
          ),
        ),
      )
    }

    "applied to Future and FutureUnlessShutdown" should {
      behave like thereafterAsync(
        ThereafterAsync[Nested[Future, FutureUnlessShutdown, *]],
        NestedThereafterTest.asyncFixture(
          FutureThereafterTest.fixture,
          FutureUnlessShutdownThereafterTest.fixture,
        ),
      )
    }

    "applied to EitherT[Future] and OptionT[Future]" should {
      implicit val traverse: Traverse[Lambda[a => Try[Either[Unit, a]]]] =
        Traverse[Try].compose(Traverse[Either[Unit, *]])

      behave like thereafterAsync(
        ThereafterAsync[Nested[EitherT[Future, Unit, *], OptionT[Future, *], *]],
        NestedThereafterTest.asyncFixture(
          EitherTThereafterTest.asyncFixture(FutureThereafterTest.fixture, NonEmpty(Seq, ())),
          OptionTThereafterTest.asyncFixture(FutureThereafterTest.fixture),
        ),
      )
    }

    "with multiple nestings" should {
      implicit val traverse: Traverse[Lambda[a => Try[Try[UnlessShutdown[a]]]]] =
        Traverse[Try].compose(Traverse[Try]).compose(Traverse[UnlessShutdown])

      behave like thereafter(
        Thereafter[
          Nested[Nested[Future, FutureUnlessShutdown, *], Nested[Try, OptionT[Future, *], *], *]
        ],
        NestedThereafterTest.fixture(
          NestedThereafterTest.fixture(
            FutureThereafterTest.fixture,
            FutureUnlessShutdownThereafterTest.fixture,
          ),
          NestedThereafterTest.fixture(
            TryThereafterTest.fixture,
            OptionTThereafterTest.asyncFixture(FutureThereafterTest.fixture),
          ),
        ),
      )
    }
  }
}

object NestedThereafterTest {
  private class NestedFixture[F[_], FContent[_], FShape, G[_], GContent[_], GShape](
      val baseF: ThereafterTest.Fixture[F, FContent, FShape],
      val baseG: ThereafterTest.Fixture[G, GContent, GShape],
  )(implicit MF: Functor[F], CF: Traverse[FContent])
      extends ThereafterTest.Fixture[
        Nested[F, G, *],
        Lambda[a => FContent[GContent[a]]],
        (FShape, GShape),
      ] {
    override type X = baseG.X

    override def fromTry[A](x: Try[A]): Nested[F, G, A] =
      Nested(baseF.fromTry(Success(baseG.fromTry(x))))

    override def fromContent[A](content: FContent[GContent[A]]): Nested[F, G, A] =
      Nested(MF.map(baseF.fromContent(content))(baseG.fromContent))

    override def isCompleted[A](x: Nested[F, G, A]): Boolean =
      if (baseF.isCompleted(x.value)) {
        CF.forall(baseF.await(x.value))(baseG.isCompleted)
      } else false

    override def await[A](x: Nested[F, G, A]): FContent[GContent[A]] =
      baseF.await(MF.map(x.value)(baseG.await))

    override def contents: Seq[FContent[GContent[X]]] =
      baseF.contents.flatMap { fc =>
        CF.traverse(fc)(_ => baseG.contents)
      }

    override def theContent[A](content: FContent[GContent[A]]): A =
      baseG.theContent(baseF.theContent(content))

    override def splitContent[A](content: FContent[GContent[A]]): Option[((FShape, GShape), A)] =
      baseF
        .splitContent(content)
        .flatMap(_.traverse(baseG.splitContent))
        .map { case (fs, (gs, x)) => ((fs, gs), x) }
  }

  def fixture[F[_], FContent[_], FShape, G[_], GContent[_], GShape](
      baseF: ThereafterTest.Fixture[F, FContent, FShape],
      baseG: ThereafterTest.Fixture[G, GContent, GShape],
  )(implicit
      MF: Functor[F],
      CF: Traverse[FContent],
  ): ThereafterTest.Fixture[
    Nested[F, G, *],
    Lambda[a => FContent[GContent[a]]],
    (FShape, GShape),
  ] = new NestedFixture[F, FContent, FShape, G, GContent, GShape](baseF, baseG)

  private class NestedAsyncFixture[F[_], FContent[_], FShape, G[_], GContent[_], GShape](
      override val baseF: ThereafterAsyncTest.Fixture[F, FContent, FShape],
      override val baseG: ThereafterAsyncTest.Fixture[G, GContent, GShape],
  )(implicit MF: Functor[F], CF: Traverse[FContent])
      extends NestedFixture[F, FContent, FShape, G, GContent, GShape](baseF, baseG)
      with ThereafterAsyncTest.Fixture[
        Nested[F, G, *],
        Lambda[a => FContent[GContent[a]]],
        (FShape, GShape),
      ] {
    override def fromFuture[A](x: Future[A]): Nested[F, G, A] =
      Nested(baseF.fromTry(Success(baseG.fromFuture(x))))
  }

  def asyncFixture[F[_], FContent[_], FShape, G[_], GContent[_], GShape](
      baseF: ThereafterAsyncTest.Fixture[F, FContent, FShape],
      baseG: ThereafterAsyncTest.Fixture[G, GContent, GShape],
  )(implicit
      MF: Functor[F],
      CF: Traverse[FContent],
  ): ThereafterAsyncTest.Fixture[
    Nested[F, G, *],
    Lambda[a => FContent[GContent[a]]],
    (FShape, GShape),
  ] = new NestedAsyncFixture[F, FContent, FShape, G, GContent, GShape](baseF, baseG)
}
