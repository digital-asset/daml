// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.Applicative
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.digitalasset.canton.DiscardedFutureTest.{
  TraitWithFuture,
  Transformer0,
  Transformer1,
  WannabeFuture,
  assertErrors,
}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.Future

class DiscardedFutureTest extends AnyWordSpec with Matchers with org.mockito.MockitoSugar {

  "DiscardedFuture" should {
    "detect statements in blocks that discard a future" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Future.successful(())
        ()
      }
      assertErrors(result, 1)
    }

    "detect statements in class defs that discard a future" in {
      val result = WartTestTraverser(DiscardedFuture) {
        class Foo(x: Int) {
          Future.successful(x)
        }
        new Foo(5)
      }
      assertErrors(result, 1)
    }

    "allow explicit discard calls" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val _ = Future.successful(())
      }
      assertErrors(result, 0)
    }

    "allow Mockito verify calls" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val mocked = mock[TraitWithFuture]
        verify(mocked).returnsFuture
        verify(mocked).returnsFutureNoArgs()
        verify(mocked).returnsFutureOneArg(0)
        verify(mocked).returnsFutureTwoArgs(1)("")
        verify(mocked).returnsFutureThreeArgs(2)("string")(new Object)
        verify(mocked).returnsFutureTypeArgs("string")
        ()
      }
      assertErrors(result, 0)
    }

    "detects discarded futures wrapped in an EitherT" in {
      val result = WartTestTraverser(DiscardedFuture) {
        EitherT(Future.successful(Either.right(())))
        ()
      }
      assertErrors(result, 1)
    }

    "detects discarded futures wrapped in an OptionT" in {
      val result = WartTestTraverser(DiscardedFuture) {
        OptionT(Future.successful(Option(())))
        ()
      }
      assertErrors(result, 1)
    }

    "detects discarded futures that are deeply wrapped" in {
      val result = WartTestTraverser(DiscardedFuture) {
        OptionT(EitherT(Future.successful(Either.right(Option(())))))
        ()
      }
      assertErrors(result, 1)
    }

    "detects annotated future-like classes" in {
      val result = WartTestTraverser(DiscardedFuture) {
        new WannabeFuture[Int]()
        ()
      }
      assertErrors(result, 1)
    }

    "detects annotated transformers of future-like computations" in {
      val result = WartTestTraverser(DiscardedFuture) {
        new Transformer0[Future, Int]
        new Transformer1[Int, Future]
        new Transformer0[Transformer1[*, OptionT[WannabeFuture, *]], Int]
        ()
      }
      assertErrors(result, 3)
    }

    "allow annotated transformers of future-unlike computations" in {
      val result = WartTestTraverser(DiscardedFuture) {
        new Transformer0[List, Int]
        new Transformer0[Transformer1[*, OptionT[List, *]], Int]
        ()
      }
      result.errors shouldBe empty
    }

    "detects nested annotated future-like classes" in {
      val result = WartTestTraverser(DiscardedFuture) {
        EitherT(new WannabeFuture[Either[String, Int]])
        ()
      }
      assertErrors(result, 1)
    }

    "detect discarded futures inside foreach" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Seq.empty[Int].foreach(i => Future.successful(i))
        List.empty[Int].foreach(i => EitherT(Future.successful(Either.right(i))))
        Iterable(1).foreach(_ => OptionT(Future.successful(Option.empty[Int])))
        Iterator(1).foreach(_ => new WannabeFuture())
        ()
      }
      assertErrors(result, 4)
    }

    "detect discarded futures inside tapEach" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Seq.empty[Int].tapEach(i => Future.successful(i))
        List.empty[Int].tapEach(i => EitherT(Future.successful(Either.right(i))))
        Iterable(1).tapEach(_ => OptionT(Future.successful(Option.empty[Int])))
        Iterator(1).tapEach(_ => new WannabeFuture())
        ()
      }
      assertErrors(result, 4)
    }

  }
}

object DiscardedFutureTest {
  trait TraitWithFuture {
    def returnsFuture: Future[Unit]
    def returnsFutureNoArgs(): Future[Unit]
    def returnsFutureOneArg(x: Int): Future[Int]
    def returnsFutureTwoArgs(x: Int)(y: String): Future[Int]
    def returnsFutureThreeArgs(x: Int)(y: String)(z: Any): Future[Int]
    def returnsFutureTypeArgs[A](x: A): Future[Unit]
  }

  @DoNotDiscardLikeFuture class WannabeFuture[A]

  @FutureTransformer(0) class Transformer0[F[_], A]
  object Transformer0 {
    implicit def applicativeTransformer0[F[_]](implicit
        F: Applicative[F]
    ): Applicative[Transformer0[F, *]] = ???
  }

  @FutureTransformer(1) class Transformer1[A, F[_]]
  object Transformer1 {
    implicit def applicativeTransformer1[F[_]](implicit
        F: Applicative[F]
    ): Applicative[Transformer1[*, F]] = ???
  }

  def assertErrors(result: WartTestTraverser.Result, expectedErrors: Int): Assertion = {
    import Matchers.*
    result.errors.length shouldBe expectedErrors
    result.errors.foreach { _ should include(DiscardedFuture.message) }
    succeed
  }
}
