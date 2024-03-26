// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.{Applicative, Functor}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.{BaseTest, DiscardOps, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

trait ThereafterTest extends AnyWordSpec with BaseTest {

  def thereafter[F[_], Content[_]](
      sut: Thereafter.Aux[F, Content],
      fixture: ThereafterTest.Fixture[F, Content],
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
        val res = sut.thereafter(x)(_content => throw ex)
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "chain failure and body failure via suppression" in {
        val ex1 = new RuntimeException("EXCEPTION")
        val ex2 = new RuntimeException("BODY FAILURE")
        val x = fixture.fromTry(Failure[Unit](ex1))
        val res = sut.thereafter(x)(_content => throw ex2)
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
    }

    "theContent" should {
      "return the content" in {
        forEvery(fixture.contents) { content =>
          sut.maybeContent(content) match {
            case Some(x) => x shouldBe fixture.theContent(content)
            case None => a[Throwable] should be thrownBy fixture.theContent(content)
          }
        }
      }
    }
  }

  def thereafterAsync[F[_], Content[_]](
      sut: ThereafterAsync.Aux[F, Content],
      fixture: ThereafterAsyncTest.Fixture[F, Content],
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
        val res = sut.thereafterF(x)(_content => throw ex)
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "propagate an asynchronous exception in the body" in {
        val ex = new RuntimeException("BODY FAILURE")
        val x = fixture.fromFuture(Future.successful(()))
        val res = sut.thereafterF(x)(_content => Future.failed(ex))
        val y = fixture.await(res)
        Try(fixture.theContent(y)) shouldBe Failure(ex)
      }

      "chain failure and body failure via suppression" in {
        val ex1 = new RuntimeException("EXCEPTION")
        val ex2 = new RuntimeException("BODY FAILURE")
        val x = fixture.fromFuture(Future.failed[Unit](ex1))
        val res = sut.thereafterF(x)(_content => Future.failed(ex2))
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
    }
  }
}

object ThereafterTest {

  trait Fixture[F[_], Content[_]] {
    type X
    def fromTry[A](x: Try[A]): F[A]
    def fromContent[A](content: Content[A]): F[A]
    def isCompleted[A](x: F[A]): Boolean
    def await[A](x: F[A]): Content[A]
    def contents: Seq[Content[X]]
    def theContent[A](content: Content[A]): A
  }

  /** Test that the scala compiler finds the [[Thereafter]] implicits */
  private def implicitResolutionTest(): Unit = {
    import Thereafter.syntax.*

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit val ec: ExecutionContext = null

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
  }
}

object ThereafterAsyncTest {

  trait Fixture[F[_], Content[_]] extends ThereafterTest.Fixture[F, Content] {
    override def fromTry[A](x: Try[A]): F[A] = fromFuture(Future.fromTry(x))
    def fromFuture[A](x: Future[A]): F[A]
  }

  /** Test that the scala compiler finds the [[ThereafterAsync]] implicits */
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
  }
}

class TryThereafterTest extends ThereafterTest {
  "Try" should {
    behave like thereafter(Thereafter[Try], TryThereafterTest.fixture)
  }
}

object TryThereafterTest {
  lazy val fixture: ThereafterTest.Fixture[Try, Try] = new ThereafterTest.Fixture[Try, Try] {
    override type X = Any
    override def fromTry[A](x: Try[A]): Try[A] = x
    override def fromContent[A](content: Try[A]): Try[A] = content
    override def isCompleted[A](x: Try[A]): Boolean = true
    override def await[A](x: Try[A]): Try[A] = x
    override def contents: Seq[Try[X]] = TryThereafterTest.contents
    override def theContent[A](content: Try[A]): A = content.fold(err => throw err, Predef.identity)
  }
  lazy val contents: Seq[Try[Any]] =
    Seq(Success(()), Success(5), Failure(new RuntimeException("failure")))
}

class FutureThereafterTest extends ThereafterTest with HasExecutionContext {
  "Future" should {
    behave like thereafter(Thereafter[Future], FutureThereafterTest.fixture)
    behave like thereafterAsync(ThereafterAsync[Future], FutureThereafterTest.fixture)
  }
}

object FutureThereafterTest {
  lazy val fixture: ThereafterAsyncTest.Fixture[Future, Try] =
    new ThereafterAsyncTest.Fixture[Future, Try] {
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
  ): ThereafterAsyncTest.Fixture[FutureUnlessShutdown, Lambda[a => Try[UnlessShutdown[a]]]] =
    new ThereafterAsyncTest.Fixture[
      FutureUnlessShutdown,
      Lambda[a => Try[UnlessShutdown[a]]],
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
    }
}

class EitherTThereafterTest extends ThereafterTest with HasExecutionContext {
  "EitherT" when {
    "applied to Try" should {
      behave like thereafter(
        Thereafter[EitherT[Try, Unit, *]],
        EitherTThereafterTest.fixture(TryThereafterTest.fixture, Seq(())),
      )
    }

    "applied to Future" should {
      behave like thereafterAsync(
        ThereafterAsync[EitherT[Future, Unit, *]],
        EitherTThereafterTest.asyncFixture(FutureThereafterTest.fixture, Seq(())),
      )
    }

    "applied to FutureUnlessShutdown" should {
      implicit val appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]
      behave like thereafterAsync(
        ThereafterAsync[EitherT[FutureUnlessShutdown, String, *]],
        EitherTThereafterTest.asyncFixture(
          FutureUnlessShutdownThereafterTest.fixture,
          Seq("left", "another left"),
        ),
      )
    }
  }
}

object EitherTThereafterTest {
  private class EitherTFixture[F[_], Content[_], E](
      base: ThereafterTest.Fixture[F, Content],
      lefts: Seq[E],
  )(implicit M: Functor[F], C: Applicative[Content])
      extends ThereafterTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]]] {
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
  }

  def fixture[F[_], Content[_], E](base: ThereafterTest.Fixture[F, Content], lefts: Seq[E])(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]]] =
    new EitherTFixture[F, Content, E](base, lefts)

  private class EitherTAsyncFixture[F[_], Content[_], E](
      base: ThereafterAsyncTest.Fixture[F, Content],
      lefts: Seq[E],
  )(implicit M: Functor[F], C: Applicative[Content])
      extends EitherTFixture[F, Content, E](base, lefts)
      with ThereafterAsyncTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]]] {
    override def fromFuture[A](x: Future[A]): EitherT[F, E, A] =
      EitherT(M.map(base.fromFuture(x))(Right(_)))
  }

  def asyncFixture[F[_], Content[_], E](
      base: ThereafterAsyncTest.Fixture[F, Content],
      lefts: Seq[E],
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterAsyncTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]]] =
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
  private class OptionTFixture[F[_], Content[_]](
      base: ThereafterTest.Fixture[F, Content]
  )(implicit M: Functor[F], C: Applicative[Content])
      extends ThereafterTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]]] {
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
  }

  def fixture[F[_], Content[_]](base: ThereafterTest.Fixture[F, Content])(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]]] =
    new OptionTFixture[F, Content](base)

  private class OptionTAsyncFixture[F[_], Content[_]](
      base: ThereafterAsyncTest.Fixture[F, Content]
  )(implicit
      M: Functor[F],
      C: Applicative[Content],
  ) extends OptionTFixture[F, Content](base)
      with ThereafterAsyncTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]]] {
    override def fromFuture[A](x: Future[A]): OptionT[F, A] =
      OptionT(M.map(base.fromFuture(x))(Option(_)))
  }

  def asyncFixture[F[_], Content[_]](base: ThereafterAsyncTest.Fixture[F, Content])(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterAsyncTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]]] =
    new OptionTAsyncFixture[F, Content](base)
}
