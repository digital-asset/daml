// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import cats.{Applicative, Foldable, Traverse, TraverseFilter}
import com.digitalasset.canton.DiscardedFutureTest.{Transformer0, Transformer1}
import com.digitalasset.canton.FutureTraverseTest.WannabeFuture
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.{ExecutionContext, Future}

class FutureTraverseTest extends AnyWordSpec with Matchers {

  def assertIsError(
      result: WartTestTraverser.Result,
      methodName: String,
      count: Int,
  ): Assertion = {
    result.errors.length shouldBe count
    result.errors.foreach { _ should include(FutureTraverse.errorMessageFor(methodName)) }
    succeed
  }

  "FutureTraverse" should {
    implicit def ec: ExecutionContext = ExecutionContext.parasitic

    "detect traverse instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).traverse(x => Future.successful(x))
        Traverse[Seq].traverse(Seq.empty[String])(x => Future.successful(x))
      }
      assertIsError(result, "traverse", 2)
    }

    "detect traverseTap instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).traverseTap(x => Future.successful(x))
        Traverse[Seq].traverseTap(Seq.empty[String])(x => Future.successful(x))
      }
      assertIsError(result, "traverseTap", 2)
    }

    "detect flatTraverse instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).flatTraverse(x => Future.successful(Seq(x)))
        Traverse[Seq].flatTraverse(Seq.empty[String])(x => Future.successful(Seq(x)))
      }
      assertIsError(result, "flatTraverse", 2)
    }

    "allow sequence instances with Future" in {
      // Traverse.sequence is OK because the futures in the container have already been started
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Future[Int]]).sequence
        Traverse[Seq].sequence(??? : Seq[Future[Int]])
      }
      result.errors shouldBe empty
    }

    "detect traverse_ instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).traverse_(_ => Future.successful(()))
        Foldable[Seq].traverse_(Seq.empty[String])(_ => Future.successful(()))
        // Also check that we find calls using a richer typeclass
        Traverse[Seq].traverse_(Seq.empty[String])(_ => Future.successful(()))
      }
      assertIsError(result, "traverse_", 3)
    }

    "allow sequence_ instances with Future" in {
      // Foldable.sequence is OK because the futures in the container have already been started
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Future[Int]]).sequence_
        Foldable[Seq].sequence_(??? : Seq[Future[Int]])
        Traverse[Seq].sequence_(??? : Seq[Future[Int]])
      }
      result.errors shouldBe empty
    }

    "detect traverseFilter instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        Seq.empty[Int].traverseFilter(x => Future.successful(Option(x)))
        TraverseFilter[Seq].traverseFilter(Seq.empty[Int])(x => Future.successful(Option(x)))
      }
      assertIsError(result, "traverseFilter", 2)
    }

    "detect filterA instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        Seq.empty[Int].filterA(x => Future.successful(true))
        TraverseFilter[Seq].filterA(Seq.empty[Int])(x => Future.successful(true))
      }
      assertIsError(result, "filterA", 2)
    }

    "allow sequenceFilter instances with Future" in {
      val result = WartTestTraverser(FutureTraverse) {
        Seq.empty[Future[Option[Int]]].sequenceFilter
        TraverseFilter[Seq].sequenceFilter(Seq.empty[Future[Option[Int]]])
      }
      result.errors shouldBe empty
    }

    "detect composed traverse instances" in {
      val result = WartTestTraverser(FutureTraverse) {
        Traverse[Seq].compose(Traverse[Option]).traverse(Seq(Some(1)))(_ => Future.successful(()))
      }
      assertIsError(result, "traverse", 1)
    }

    "detect EitherT usages" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).traverse(_ => EitherT.pure[Future, String](0))
        Traverse[Seq].traverse(Seq(1))(_ => EitherT.pure[Future, String](0))
      }
      assertIsError(result, "traverse", 2)
    }

    "detect OptionT usages" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).traverse_(_ => OptionT.pure[Future](0))
        Foldable[Seq].traverse_(Seq(1))(_ => OptionT.pure[Future](0))
        Traverse[Seq].traverse_(Seq(1))(_ => OptionT.pure[Future](0))
      }
      assertIsError(result, "traverse_", 3)
    }

    "detect usages with annotated future-like types" in {
      val result = WartTestTraverser(FutureTraverse) {
        (??? : Seq[Int]).traverse_(_ => new WannabeFuture[Int])
        Foldable[Seq].traverse_(Seq(1))(_ => new WannabeFuture[Int])
        Traverse[Seq].traverse_(Seq(1))(_ => new WannabeFuture[Int])
      }
      assertIsError(result, "traverse_", 3)
    }

    "detect usages with annotated transformers of future-like types" in {
      val result = WartTestTraverser(FutureTraverse) {
        Seq.empty[Int].traverse_(_ => new Transformer0[Future, Int])
        Seq
          .empty[Int]
          .traverse_[Transformer1[*, WannabeFuture], Int](_ => new Transformer1[Int, WannabeFuture])
        Seq
          .empty[Int]
          .traverse_[Transformer1[*, Transformer0[WannabeFuture, *]], Int](_ =>
            new Transformer1[Int, Transformer0[WannabeFuture, *]]
          )
      }
      assertIsError(result, "traverse_", 3)
    }

    "allow singleton containers" in {
      val result = WartTestTraverser(FutureTraverse) {
        (1.some).traverse(_ => Future.successful(()))
        Foldable[Option].traverse_(Option.empty[Int])(_ => Future.successful(()))
        Traverse[Option].traverse(Option.empty[Int])(_ => Future.successful(()))
        (1.some).traverseFilter(_ => Future.successful(Option.empty))
        TraverseFilter[Option].traverseFilter(Option.empty[Int])(_ =>
          Future.successful(Option.empty)
        )
        // Since we've imported cats.syntax.either.*, this call does not go through Cats' Traverse typeclass anyway.
        (Either.left[String, Int]("")).traverse(_ => Future.successful(()))
        Foldable[Either[String, *]].traverse_(Left(""))(_ => Future.successful(()))
        Traverse[Either[String, *]].traverse(Left(""))(_ => Future.successful(()))
      }
      result.errors shouldBe empty
    }

    "not try to guess whether the applicative can be instantiated to a future-like type" in {
      // That's a limitation we accept
      val result = WartTestTraverser(FutureTraverse) {
        def myTraverse[F[_]: Applicative](f: Int => F[Unit]): F[Seq[Unit]] =
          Seq(1).traverse(f)
        myTraverse(x => Future.successful(()))
      }
      result.errors shouldBe empty
    }
  }
}

object FutureTraverseTest {
  @DoNotTraverseLikeFuture class WannabeFuture[A]

  object WannabeFuture {
    implicit def wannabeFutureApplicative: Applicative[WannabeFuture] = ???
  }
}
