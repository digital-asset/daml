// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{Chain, EitherT, NonEmptyChain, OptionT, Validated}
import cats.instances.either.*
import cats.laws.discipline.{ApplicativeTests, FunctorTests, MonadErrorTests, ParallelTests}
import cats.syntax.either.*
import cats.{Eq, Monad}
import com.digitalasset.canton.BaseTestWordSpec
import org.scalacheck.Arbitrary
import org.scalatest.wordspec.AnyWordSpec

class CheckedTTest extends AnyWordSpec with BaseTestWordSpec {

  // We use Either as the transformed monad in the tests
  type Monad[A] = Either[Int, A]

  def failure[A](x: A): Nothing = throw new RuntimeException
  def failure2[A, B](x: A, y: B): Nothing = throw new RuntimeException

  "map" should {
    "not touch aborts" in {
      val sut: CheckedT[Monad, Int, String, String] = CheckedT.abortT(5)
      assert(sut.map(failure) == CheckedT.abortT(5))
    }

    "change the result" in {
      val sut = CheckedT.resultT[Monad, Int, String](10)
      assert(sut.map(x => x + 1) == CheckedT.resultT(11))
    }

    "respect the transformed monad" in {
      val sut = CheckedT[Monad, Int, Int, Int](Left(1))
      assert(sut.map(x => x + 1) == CheckedT.result(Either.left(1)))
    }

    "not touch the non-aborts" in {
      val sut = Checked.continueWithResult("non-abort", 5)
      assert(sut.map(x => x + 1) == Checked.continueWithResult("non-abort", 6))
    }
  }

  "mapAbort" should {
    "change the abort" in {
      assert(CheckedT.abortT[Monad, String, Int](5).mapAbort(x => x + 1) == CheckedT.abortT(6))
    }

    "not touch the non-aborts" in {
      val sut: CheckedT[Monad, String, String, Unit] = CheckedT.continueT("non-abort")
      assert(sut.mapAbort(failure) == CheckedT.continueT("non-abort"))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(10))
      assert(sut.mapAbort(failure) == CheckedT.result(Either.left(10)))
    }

    "not touch the result" in {
      val sut: CheckedT[Monad, String, String, Int] = CheckedT.resultT(5)
      assert(sut.mapAbort(failure) == CheckedT.resultT(5))
    }
  }

  "mapNonaborts" should {
    "not touch aborts" in {
      val sut = CheckedT.abortT[Monad, Int, Int](5).mapNonaborts(x => x ++ Chain.one(1))
      assert(
        sut.value.exists(checked => checked.getAbort.contains(5) && checked.nonaborts == Chain(1))
      )
    }

    "not touch results" in {
      val sut = CheckedT.resultT[Monad, Int, Int](3).mapNonaborts(x => x ++ Chain.one(1))
      assert(sut == CheckedT.continueWithResultT(1, 3))
    }

    "change the nonaborts" in {
      val sut = Checked.continue("nonaborts").mapNonaborts(x => Chain.one("test") ++ x)
      assert(sut.nonaborts == Chain("test", "nonaborts"))
      assert(sut.getResult.contains(()))
    }

    "repsect the transformed monad" in {
      val sut = CheckedT.result(Either.left(5))
      assert(sut.mapNonaborts(failure) == sut)
    }
  }

  "mapNonabort" should {
    "only touch nonaborts" in {
      val sut1: CheckedT[Monad, Int, String, Double] = CheckedT.abortT(5)
      assert(sut1.mapNonabort(failure) == sut1)

      val sut2: CheckedT[Monad, Int, String, Double] = CheckedT.resultT(5.7)
      assert(sut2.mapNonabort(failure) == sut2)
    }

    "change the nonaborts" in {
      val sut1 = CheckedT.abortT(5).appendNonaborts(Chain(3, 5)).mapNonabort(x => x + 1)
      assert(sut1.value.exists(_.getAbort.contains(5)))
      assert(sut1.value.exists(_.nonaborts == Chain(4, 6)))

      val sut2 = CheckedT.resultT(5).appendNonaborts(Chain(3, 5)).mapNonabort(x => x + 1)
      assert(sut2.value.exists(_.getResult.contains(5)))
      assert(sut2.value.exists(_.nonaborts == Chain(4, 6)))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(10))
      assert(sut.mapNonabort(failure) == sut)
    }
  }

  "trimap" should {
    "work for aborts" in {
      val sut = CheckedT.abortT[Monad, String, Int]("abort").appendNonabort("nonabort")
      val result = sut.trimap(x => x + "test", y => "test " + y, failure)

      assert(result.value.exists(_.getAbort.contains("aborttest")))
      assert(result.value.exists(_.nonaborts == Chain("test nonabort")))
    }

    "work for results" in {
      val sut = CheckedT.continueWithResultT[Monad, Double]("nonabort", 5)
      val expected = CheckedT.continueWithResultT[Monad, Double]("test nonabort", 6)
      assert(sut.trimap(failure, x => "test " + x, y => y + 1) == expected)
    }

    "respect the transformed monad" in {
      val sut = CheckedT.abort(Either.left(10))
      assert(sut.trimap(failure, failure, failure) == sut)
    }
  }

  "semiflatMap" should {
    "act on results" in {
      val sut = CheckedT.continueWithResultT[Monad, Double]("nonabort", 5)
      assert(sut.semiflatMap(x => Right(x + 1)) == CheckedT.continueWithResultT("nonabort", 6))
    }

    "not touch aborts" in {
      val sut = CheckedT.abortT(10)
      assert(sut.semiflatMap(failure) == sut)
    }

    "act according to the transformed monad" in {
      val sut = CheckedT.resultT[Monad, Int, String](5)
      assert(sut.semiflatMap(x => Left(3)) == CheckedT.result(Either.left(3)))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(10))
      assert(sut.semiflatMap(failure) == sut)
    }
  }

  "fold" should {
    "fold an abort" in {
      val sut = CheckedT.abortT(5).prependNonabort("nonabort")
      assert(
        sut.fold((x, y) => Chain.one(x.toString) ++ y, failure2) == Either.right(
          Chain("5", "nonabort")
        )
      )
    }

    "fold a result" in {
      val sut = CheckedT.continueWithResultT("nonabort", 7.5)
      assert(
        sut.fold(failure2, (x, y) => Chain.one(y.toString) ++ x) == Either.right(
          Chain("7.5", "nonabort")
        )
      )
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(3))
      assert(sut.fold(failure2, failure2) == Either.left(3))
    }
  }

  "prependNonaborts" should {
    "work on an abort" in {
      val sut =
        CheckedT.abortT(1).prependNonaborts(Chain("a", "b")).prependNonaborts(Chain("c", "d"))
      assert(sut.value.exists(_.getAbort.contains(1)))
      assert(sut.value.exists(_.nonaborts == Chain("c", "d", "a", "b")))
    }

    "work on a result" in {
      val sut =
        CheckedT.resultT(1).prependNonaborts(Chain("a", "b")).prependNonaborts(Chain("c", "d"))
      assert(sut.value.exists(_.getResult.contains(1)))
      assert(sut.value.exists(_.nonaborts == Chain("c", "d", "a", "b")))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(3))
      assert(sut.prependNonaborts(Chain("a", "b")) == sut)
    }
  }

  "prependNonabort" should {
    "work on an abort" in {
      val sut = CheckedT.abortT(1).prependNonabort("a").prependNonabort("b")
      assert(
        sut == CheckedT[Monad, Int, String, Int](Either.right(Checked.Abort(1, Chain("b", "a"))))
      )
    }

    "work on a result" in {
      val sut = CheckedT.resultT(1).prependNonabort("a").prependNonabort("b")
      assert(
        sut == CheckedT[Monad, Int, String, Int](Either.right(Checked.Result(Chain("b", "a"), 1)))
      )
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(3))
      assert(sut.prependNonabort("a") == sut)
    }
  }

  "appendNonaborts" should {
    "work on an abort" in {
      val sut = CheckedT.abortT(1).appendNonaborts(Chain("a", "b")).appendNonaborts(Chain("c", "d"))
      assert(sut.value.exists(_.getAbort.contains(1)))
      assert(sut.value.exists(_.nonaborts == Chain("a", "b", "c", "d")))
    }

    "work on a result" in {
      val sut =
        CheckedT.resultT(1).appendNonaborts(Chain("a", "b")).appendNonaborts(Chain("c", "d"))
      assert(sut.value.exists(_.getResult.contains(1)))
      assert(sut.value.exists(_.nonaborts == Chain("a", "b", "c", "d")))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(4))
      assert(sut.appendNonaborts(Chain("a", "b")) == sut)
    }
  }

  "appendNonabort" should {
    "work on an abort" in {
      val sut = CheckedT.abortT(1).appendNonabort("a").appendNonabort("b")
      assert(sut.value.exists(_.getAbort.contains(1)))
      assert(sut.value.exists(_.nonaborts == Chain("a", "b")))
    }

    "work on a result" in {
      val sut = CheckedT.resultT(1).appendNonabort("a").appendNonabort("b")
      assert(sut.value.exists(_.getResult.contains(1)))
      assert(sut.value.exists(_.nonaborts == Chain("a", "b")))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(4))
      assert(sut.appendNonabort("a") == sut)
    }
  }

  "product" should {
    "prefer aborts from the left" in {
      val sut1 =
        CheckedT.abortT[Monad, Double, String](5).product(CheckedT.abortT[Monad, Double, String](7))
      assert(sut1 == CheckedT.abortT(5))

      val sut2 =
        CheckedT
          .abortT[Monad, Double, String](5)
          .product(CheckedT.continueWithResultT[Monad, Int](1.0, "result"))
      assert(sut2 == CheckedT.abortT(5))
    }

    "combine nonaborts" in {
      val sut1 = CheckedT
        .continueWithResultT[Monad, Int]("left", 5)
        .product(CheckedT.continueWithResultT[Monad, Int]("right", 7))
      assert(sut1.value.exists(_.nonaborts == Chain("left", "right")))
      assert(sut1.value.exists(_.getResult.contains((5, 7))))

      val sut2 = CheckedT
        .continueWithResultT[Monad, String]("left", 6)
        .product(CheckedT.abortT[Monad, String, Int]("failure").appendNonabort("right"))
      assert(sut2.value.exists(_.getAbort.contains("failure")))
      assert(sut2.value.exists(_.nonaborts == Chain("left", "right")))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(0))
      assert(sut.product(CheckedT.resultT[Monad, Double, String](3)) == sut)
    }
  }

  "flatMap" should {
    "propagate aborts" in {
      val sut = CheckedT.abortT("failure").prependNonaborts(Chain("a", "b"))
      assert(sut.flatMap(failure) == sut)
    }

    "combine non-aborts" in {
      val sut = CheckedT
        .continueWithResultT[Monad, String]("first", 1)
        .flatMap(x =>
          if (x == 1) CheckedT.continueWithResultT[Monad, String]("second", 2) else failure(x)
        )
      assert(sut.value.exists(_.nonaborts == Chain("first", "second")))
      assert(sut.value.exists(_.getResult.contains(2)))
    }

    "act according to the transformed monad" in {
      val sut = CheckedT.resultT[Monad, Int, String](10)
      val expected = CheckedT[Monad, Int, String, Int](Left(10))
      assert(sut.flatMap(x => CheckedT.result[Int, String](Either.left[Int, Int](x))) == expected)
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(5))
      assert(sut.flatMap(failure) == sut)
    }
  }

  "biflatMap" should {
    "call the second continuation for results" in {
      val sut = CheckedT.continueWithResultT[Monad, Int]("first", 1)
      val test = sut.biflatMap(
        failure,
        result => CheckedT.continueWithResultT[Monad, Int]("second", result + 2),
      )
      assert(test.value.exists(_.nonaborts == Chain("first", "second")))
      assert(test.value.exists(_.getResult.contains(3)))
    }

    "call the first continuation for aborts" in {
      val sut: Checked[Int, String, Int] = Checked.abort(1).prependNonabort("first")
      val test = sut.biflatMap(
        abort => Checked.continueWithResult("second", abort + 3),
        _ => throw new RuntimeException("Called continuation for results"),
      )
      assert(test.nonaborts == Chain("first", "second"))
      assert(test.getResult.contains(4))
    }

    "act according to the transformed monad" in {
      val sut1 = CheckedT
        .resultT[Monad, Int, String](1)
        .biflatMap(failure, result => CheckedT.result[Int, String](Either.left[Int, Int](result)))
      assert(sut1 == CheckedT[Monad, Int, String, Int](Either.left(1)))

      val sut2 = CheckedT
        .abortT[Monad, String, Int](1)
        .biflatMap(abort => CheckedT.result[Int, String](Either.left[Int, Int](abort)), failure)
      assert(sut2 == CheckedT[Monad, Int, String, Int](Either.left(1)))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(1))
      assert(sut.biflatMap(failure, failure) == sut)
    }
  }

  "abortFlatMap" should {
    "not touch results" in {
      val sut = CheckedT.continueWithResultT[Monad, Int]("first", 1)
      assert(sut.abortFlatMap(failure) == sut)
    }

    "call the continuation for aborts" in {
      val sut = CheckedT.abortT[Monad, String, Int](1).prependNonabort("first")
      val test =
        sut.abortFlatMap(abort => CheckedT.continueWithResultT[Monad, Int]("second", abort + 3))
      assert(test.value.exists(_.nonaborts == Chain("first", "second")))
      assert(test.value.exists(_.getResult.contains(4)))
    }

    "act according to the transformed monad" in {
      val sut = CheckedT
        .abortT[Monad, String, Int](1)
        .abortFlatMap(abort => CheckedT.result[Int, String](Either.left[Int, Int](abort)))
      assert(sut == CheckedT[Monad, Int, String, Int](Either.left(1)))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(1))
      assert(sut.abortFlatMap(failure) == sut)
    }
  }

  "subflatMap" should {
    "propagate aborts" in {
      val sut = CheckedT.abortT("failure").prependNonaborts(Chain("a", "b"))
      assert(sut.subflatMap(failure) == sut)
    }

    "combine non-aborts" in {
      val sut = CheckedT
        .continueWithResultT[Monad, String]("first", 1)
        .subflatMap(x => if (x == 1) Checked.continueWithResult("second", 2) else failure(x))
      assert(sut.value.exists(_.nonaborts == Chain("first", "second")))
      assert(sut.value.exists(_.getResult.contains(2)))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(5))
      assert(sut.subflatMap(failure) == sut)
    }
  }

  "abortSubflatMap" should {
    "not touch results" in {
      val sut = CheckedT.resultT("result").prependNonaborts(Chain("a", "b"))
      assert(sut.abortSubflatMap(failure) == sut)
    }

    "combine non-aborts" in {
      val sut = CheckedT
        .abortT[Monad, String, Int](1)
        .prependNonabort("first")
        .abortSubflatMap(x => if (x == 1) Checked.continueWithResult("second", 2) else failure(x))
      assert(sut.value.exists(_.nonaborts == Chain("first", "second")))
      assert(sut.value.exists(_.getResult.contains(2)))
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(5))
      assert(sut.abortSubflatMap(failure) == sut)
    }
  }

  "flatmapIfSuccess" should {
    "not touch results when there are non-aborts" in {
      val sut = CheckedT.resultT("result").prependNonaborts(Chain("a", "b"))
      assert(sut.flatMapIfSuccess(failure) == sut)
    }

    "map the result if success " in {
      val result = "result"
      val sut = CheckedT.resultT(result)
      assert(
        sut.flatMapIfSuccess[String, String, String](x => CheckedT.resultT(x.reverse)) == CheckedT
          .resultT(result.reverse)
      )
    }

    "propagate aborts" in {
      val sut = CheckedT.abortT("failure").prependNonaborts(Chain("a", "b"))
      assert(sut.flatMapIfSuccess(failure) == sut)
    }
  }

  "toResult" should {
    "not touch results" in {
      val sut = CheckedT.continueWithResultT("first", 1)
      assert(sut.toResult(2) == sut)
    }

    "merge abort with the nonaborts" in {
      val sut = CheckedT
        .abortT[Either[String, *], String, Int]("abort")
        .prependNonaborts(Chain("first", "second"))
      assert(
        sut.toResult(3) == CheckedT
          .continueWithResultT("second", 3)
          .prependNonaborts(Chain("abort", "first"))
      )
    }

  }

  "foreach" should {
    "do nothing on an abort" in {
      val sut = CheckedT.abortT[Monad, String, Int]("failure")
      assert(sut.foreach(failure) == Right(()))
    }

    "run the function on the result" in {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var run = false
      assert(
        CheckedT.resultT[Monad, Int, String](5).foreach(x => if (x == 5) run = true) == Right(())
      )
      assert(run)
    }

    "respect the transformed monad" in {
      val sut = CheckedT.result(Either.left(3))
      assert(sut.foreach(failure) == Left(3))
    }
  }

  "exists" should {
    "return false on an abort" in {
      val sut = CheckedT.abortT("abort").appendNonabort("nonabort")
      assert(sut.exists(failure) == Right(false))
    }

    "evaluate the predicate on the result" in {
      assert(CheckedT.resultT(5).exists(x => x == 5) == Right(true))
      assert(CheckedT.resultT(5).exists(x => x != 5) == Right(false))
    }

    "respect the transformed monad" in {
      assert(CheckedT.result(Either.left(3)).exists(failure) == Left(3))
    }
  }

  "forall" should {
    "return true on an abort" in {
      val sut = CheckedT.abortT("abort").appendNonabort("nonabort")
      assert(sut.forall(failure) == Right(true))
    }

    "evaluate the predicate on the result" in {
      assert(CheckedT.resultT(5).forall(x => x == 5) == Right(true))
      assert(CheckedT.resultT(5).forall(x => x != 5) == Right(false))
    }

    "respect the transformed monad" in {
      assert(CheckedT.result(Either.left(3)).forall(failure) == Left(3))
    }
  }

  "toEitherT" should {
    "map abort to left" in {
      assert(CheckedT.abortT[Monad, String, Int](3).toEitherT == EitherT.leftT(3))
    }

    "map result to right" in {
      assert(CheckedT.continueWithResultT[Monad, Int]("nonabort", 4).toEitherT == EitherT.rightT(4))
    }

    "respect the transformed monad" in {
      assert(CheckedT.result(Either.left(3)).toEitherT == EitherT.right(Either.left(3)))
    }
  }

  "toEitherTWithNonAborts" should {
    "map abort to Left" in {
      assert(
        CheckedT
          .abortT[Monad, String, Int]("abort")
          .prependNonabort("nonabort")
          .toEitherTWithNonaborts == EitherT
          .leftT(NonEmptyChain("abort", "nonabort"))
      )
    }

    "map result to Right" in {
      assert(CheckedT.resultT[Monad, String, String](5).toEitherTWithNonaborts == EitherT.rightT(5))
    }

    "map nonaborts to Left" in {
      assert(
        CheckedT.continueWithResultT[Monad, String]("nonabort", 4).toEitherTWithNonaborts == EitherT
          .leftT(NonEmptyChain("nonabort"))
      )
    }
  }

  "toOptionT" should {
    "map abort to None" in {
      assert(CheckedT.abortT[Monad, String, Int](3).toOptionT == OptionT.none)
    }

    "map result to Some" in {
      assert(CheckedT.pure[Monad, Int, String](3).toOptionT == OptionT.pure(3))
    }

    "respect the transformed monad" in {
      assert(CheckedT.result(Either.left(3)).toOptionT == OptionT[Monad, Int](Either.left(3)))
    }
  }

  "widenResult, widenAbort, and widenNonabort" should {
    "change only the type" in {
      val sut1 = CheckedT.continueWithResultT[Monad, Int]("nonabort", 5)
      assert(sut1.widenResult[AnyVal] == sut1)
      assert(sut1.widenAbort[AnyVal] == sut1)
      assert(sut1.widenNonabort[AnyRef] == sut1)

      val sut2 = CheckedT.abortT[Monad, String, Int](10)
      assert(sut2.widenResult[AnyVal] == sut2)
      assert(sut2.widenAbort[AnyVal] == sut2)
      assert(sut2.widenNonabort[AnyRef] == sut2)
    }
  }

  "fromChecked" should {
    "map aborts to abort" in {
      val sut = CheckedT.fromChecked[Monad](Checked.abort(5).prependNonabort("a"))
      assert(sut == CheckedT.abortT(5).prependNonabort("a"))
    }

    "map results to results" in {
      assert(CheckedT.fromChecked[Monad](Checked.continue(5)) == CheckedT.continueT(5))
    }
  }

  "fromEitherT" should {
    "map Left to abort" in {
      val sut = EitherT.leftT[Monad, Int](3)
      assert(CheckedT.fromEitherT(sut) == CheckedT.abortT(3))
    }

    "map Right to result" in {
      val sut = EitherT.rightT[Monad, Int](4)
      assert(CheckedT.fromEitherT(sut) == CheckedT.resultT(4))
    }

    "respect the transformed monad" in {
      val sut = EitherT[Monad, Int, Int](Either.left(3))
      assert(CheckedT.fromEitherT(sut) == CheckedT.abort(Either.left(3)))
    }
  }

  "fromEitherTNonabort" should {
    "map Left to nonabort" in {
      val sut = EitherT.leftT[Monad, String](3)
      assert(
        CheckedT.fromEitherTNonabort("result", sut) == CheckedT.continueWithResultT(3, "result")
      )
    }

    "map Right to result" in {
      val sut = EitherT.rightT[Monad, Int](4)
      assert(CheckedT.fromEitherTNonabort(throw new RuntimeException, sut) == CheckedT.resultT(4))
    }

    "respect the transformed monad" in {
      val sut = EitherT[Monad, Int, Int](Either.left(3))
      assert(
        CheckedT.fromEitherTNonabort(throw new RuntimeException, sut) == CheckedT.abort(
          Either.left(3)
        )
      )
    }
  }

  "Monad.tailRecM" should {
    "iterate the step function until it returns Right" in {
      val bound = 10
      val run = Monad[CheckedT[Monad, Int, Int, *]].tailRecM[Int, Int](0) { n =>
        if (n < bound) CheckedT.continueWithResultT(n, Left(n + 1)) else CheckedT.resultT(Right(n))
      }
      assert(run.value.exists(_.nonaborts == Chain.fromSeq(0 until bound)))
      assert(run.value.exists(_.getResult.contains(bound)))
    }

    "abort if the step function aborts" in {
      val bound = 10
      val run = Monad[CheckedT[Monad, Int, Int, *]].tailRecM[Int, Int](0) { n =>
        if (n < bound) CheckedT.continueWithResultT(n, Left(n + 1)) else CheckedT.abortT(bound)
      }
      assert(run.value.exists(_.getAbort.contains(bound)))
      assert(run.value.exists(_.nonaborts == Chain.fromSeq(0 until bound)))
    }

    "run in constant stack space" in {
      val bound = 1000000
      val run = Monad[CheckedT[Monad, Int, Int, *]].tailRecM[Int, Int](0) { n =>
        if (n < bound) CheckedT.resultT(Left(n + 1)) else CheckedT.resultT(Right(n))
      }
      assert(run == CheckedT.resultT(bound))
    }

    "respect the transformed monad" in {
      val run = Monad[CheckedT[Monad, Int, Int, *]].tailRecM[Int, Int](0) { n =>
        CheckedT.result(Either.left(n))
      }
      assert(run.value == Either.left(0))
    }
  }

  {
    import CheckedTTest.{arbitraryCheckedT, eqCheckedT}
    import CheckedTest.{arbitraryChecked, eqChecked}

    "Functor" should {
      checkAllLaws(
        "MonadError",
        FunctorTests[CheckedT[Monad, Int, String, *]].functor[Int, Int, String],
      )
    }

    "Applicative" should {
      import cats.laws.discipline.arbitrary.catsLawsArbitraryForValidated
      checkAllLaws(
        "Applicative",
        ApplicativeTests[CheckedT[Validated[String, *], Int, String, *]]
          .applicative[Int, Int, String],
      )
    }

    "MonadError" should {
      checkAllLaws(
        "MonadError",
        MonadErrorTests[CheckedT[Monad, Int, String, *], Int].monadError[Int, Int, String],
      )
    }

    "Parallel" should {
      import cats.laws.discipline.arbitrary.{
        catsLawsArbitraryForValidated,
        catsLawsArbitraryForNested,
      }
      checkAllLaws(
        name = "Parallel",
        ParallelTests[CheckedT[Monad, Int, String, *]].parallel[Int, String],
      )
    }
  }

}

object CheckedTTest {

  implicit def eqCheckedT[F[_], A, N, R](implicit
      F: Eq[F[Checked[A, N, R]]]
  ): Eq[CheckedT[F, A, N, R]] = { (x: CheckedT[F, A, N, R], y: CheckedT[F, A, N, R]) =>
    F.eqv(x.value, y.value)
  }

  implicit def arbitraryCheckedT[F[_], A, N, R](implicit
      F: Arbitrary[F[Checked[A, N, R]]]
  ): Arbitrary[CheckedT[F, A, N, R]] =
    Arbitrary(F.arbitrary.map(CheckedT(_)))
}
