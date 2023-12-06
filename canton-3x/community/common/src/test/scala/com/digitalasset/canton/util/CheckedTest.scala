// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Eq
import cats.data.{Chain, EitherT, NonEmptyChain}
import cats.laws.discipline.MonadErrorTests
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.BaseTestWordSpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}

class CheckedTest extends AnyWordSpec with BaseTestWordSpec {

  def failure[A](x: A): Nothing = throw new RuntimeException

  def failure2[A, B](x: A, y: B): Nothing = throw new RuntimeException

  "map" should {
    "not touch aborts" in {
      val sut: Checked[Int, String, String] = Checked.abort(5)
      assert(sut.map(failure) == Checked.abort(5))
    }

    "change the result" in {
      assert(Checked.result(10).map(x => x + 1) == Checked.result(11))
    }

    "not touch the non-aborts" in {
      assert(
        Checked.continueWithResult("non-abort", 5).map(x => x + 1) == Checked.continueWithResult(
          "non-abort",
          6,
        )
      )
    }
  }

  "mapAbort" should {
    "change the abort" in {
      assert(Checked.abort(5).mapAbort(x => x + 1) == Checked.abort(6))
    }

    "not touch the non-aborts" in {
      val sut: Checked[String, String, Unit] = Checked.continue("non-abort")
      assert(sut.mapAbort(failure) == Checked.continue("non-abort"))
    }

    "not touch the result" in {
      val sut: Checked[String, String, Int] = Checked.result(5)
      assert(sut.mapAbort(failure) == Checked.result(5))
    }
  }

  "mapNonaborts" should {
    "not touch aborts" in {
      val sut = Checked.abort(5).mapNonaborts(x => x ++ Chain.one(1))
      assert(sut.getAbort.contains(5))
      assert(sut.nonaborts == Chain(1))
    }

    "not touch results" in {
      assert(
        Checked.result(3).mapNonaborts(x => x ++ Chain.one(1)) == Checked.continueWithResult(1, 3)
      )
    }

    "change the nonaborts" in {
      val sut = Checked.continue("nonaborts").mapNonaborts(x => Chain.one("test") ++ x)
      assert(sut.nonaborts == Chain("test", "nonaborts"))
      assert(sut.getResult.contains(()))
    }
  }

  "mapNonabort" should {
    "only touch nonaborts" in {
      val sut1: Checked[Int, String, Double] = Checked.abort(5)
      assert(sut1.mapNonabort(failure) == sut1)

      val sut2: Checked[Int, String, Double] = Checked.result(5.7)
      assert(sut2.mapNonabort(failure) == sut2)
    }

    "change the nonaborts" in {
      val sut1 = Checked.abort(5).appendNonaborts(Chain(3, 5)).mapNonabort(x => x + 1)
      assert(sut1.getAbort.contains(5))
      assert(sut1.nonaborts == Chain(4, 6))

      val sut2 = Checked.result(5).appendNonaborts(Chain(3, 5)).mapNonabort(x => x + 1)
      assert(sut2.getResult.contains(5))
      assert(sut2.nonaborts == Chain(4, 6))
    }
  }

  "trimap" should {
    "work for aborts" in {
      val sut: Checked[String, String, Int] = Checked.abort("abort").appendNonabort("nonabort")
      val result = sut.trimap(x => x + "test", y => "test " + y, failure)

      assert(result.getAbort.contains("aborttest"))
      assert(result.nonaborts == Chain("test nonabort"))
    }

    "work for results" in {
      val sut: Checked[Double, String, Int] = Checked.continueWithResult("nonabort", 5)
      assert(
        sut.trimap(failure, x => "test " + x, y => y + 1) == Checked.continueWithResult(
          "test nonabort",
          6,
        )
      )
    }
  }

  "fold" should {
    "fold an abort" in {
      val sut: Checked[Int, String, Double] = Checked.Abort(5, Chain("nonabort"))
      assert(sut.fold((x, y) => Chain.one(x.toString) ++ y, failure2) == Chain("5", "nonabort"))
    }

    "fold a result" in {
      val sut: Checked[Int, String, Double] = Checked.continueWithResult("nonabort", 7.5)
      assert(sut.fold(failure2, (x, y) => Chain.one(y.toString) ++ x) == Chain("7.5", "nonabort"))
    }
  }

  "prependNonaborts" should {
    "work on an abort" in {
      val sut = Checked.abort(1).prependNonaborts(Chain("a", "b")).prependNonaborts(Chain("c", "d"))
      assert(sut.getAbort.contains(1))
      assert(sut.nonaborts == Chain("c", "d", "a", "b"))
    }

    "work on a result" in {
      val sut =
        Checked.result(1).prependNonaborts(Chain("a", "b")).prependNonaborts(Chain("c", "d"))
      assert(sut.getResult.contains(1))
      assert(sut.nonaborts == Chain("c", "d", "a", "b"))
    }
  }

  "prependNonabort" should {
    "work on an abort" in {
      assert(
        Checked.abort(1).prependNonabort("a").prependNonabort("b") == Checked.Abort(
          1,
          Chain("b", "a"),
        )
      )
    }

    "work on a result" in {
      assert(
        Checked.result(1).prependNonabort("a").prependNonabort("b") == Checked.Result(
          Chain("b", "a"),
          1,
        )
      )
    }
  }

  "appendNonaborts" should {
    "work on an abort" in {
      val sut = Checked.abort(1).appendNonaborts(Chain("a", "b")).appendNonaborts(Chain("c", "d"))
      assert(sut.getAbort.contains(1))
      assert(sut.nonaborts == Chain("a", "b", "c", "d"))
    }

    "work on a result" in {
      val sut = Checked.result(1).appendNonaborts(Chain("a", "b")).appendNonaborts(Chain("c", "d"))
      assert(sut.getResult.contains(1))
      assert(sut.nonaborts == Chain("a", "b", "c", "d"))
    }
  }

  "appendNonabort" should {
    "work on an abort" in {
      val sut = Checked.abort(1).appendNonabort("a").appendNonabort("b")
      assert(sut.getAbort.contains(1))
      assert(sut.nonaborts == Chain("a", "b"))
    }

    "work on a result" in {
      val sut = Checked.result(1).appendNonabort("a").appendNonabort("b")
      assert(sut.getResult.contains(1))
      assert(sut.nonaborts == Chain("a", "b"))
    }
  }

  "product" should {
    "prefer aborts from the left" in {
      assert(Checked.abort(5).product(Checked.abort(7)) == Checked.abort(5))
      assert(
        Checked.abort(5).product(Checked.continueWithResult(1.0, "result")) == Checked.abort(5)
      )
    }

    "combine nonaborts" in {
      val sut1 =
        Checked.continueWithResult("left", 5).product(Checked.continueWithResult("right", 7))
      assert(sut1.nonaborts == Chain("left", "right"))
      assert(sut1.getResult.contains((5, 7)))

      val sut2 = Checked
        .continueWithResult("left", 6)
        .product(Checked.abort("failure").appendNonabort("right"))
      assert(sut2.getAbort.contains("failure"))
      assert(sut2.nonaborts == Chain("left", "right"))
    }
  }

  "ap" should {
    "prefer aborts from the operator" in {
      assert(Checked.abort(5).ap(Checked.abort(7)) == Checked.abort(7))
      assert(Checked.continueWithResult(7, 12).ap(Checked.abort(5)) == Checked.abort(5))
    }

    "combine nonaborts" in {
      val sut1 =
        Checked
          .continueWithResult("right", 7)
          .ap(Checked.continueWithResult[Nothing, String, Int => Int]("left", x => x + 1))
      assert(sut1.nonaborts == Chain("left", "right"))
      assert(sut1.getResult.contains(8))

      val sut2 = Checked
        .abort("failure")
        .appendNonabort("right")
        .ap(Checked.continueWithResult[Nothing, String, Int => Int]("left", x => x + 1))
      assert(sut2.getAbort.contains("failure"))
      assert(sut2.nonaborts == Chain("left", "right"))
    }
  }

  "reverseAp" should {
    "prefer aborts from the operand" in {
      assert(Checked.abort(5).ap(Checked.abort(7)) == Checked.abort(7))
      assert(Checked.continueWithResult(7, 12).ap(Checked.abort(5)) == Checked.abort(5))
    }

    "combine nonaborts" in {
      val sut1 =
        Checked
          .continueWithResult("right", 7)
          .reverseAp(Checked.continueWithResult[Nothing, String, Int => Int]("left", x => x + 1))
      assert(sut1.nonaborts == Chain("right", "left"))
      assert(sut1.getResult.contains(8))

      val sut2 = Checked
        .continueWithResult("right", 7)
        .reverseAp(Checked.abort("failure").appendNonabort("left"))
      assert(sut2.getAbort.contains("failure"))
      assert(sut2.nonaborts == Chain("right", "left"))
    }
  }

  "flatMap" should {
    "propagate aborts" in {
      val sut = Checked.abort("failure").prependNonaborts(Chain("a", "b"))
      assert(sut.flatMap(failure) == sut)
    }

    "combine non-aborts" in {
      val sut = Checked
        .continueWithResult("first", 1)
        .flatMap(x => if (x == 1) Checked.continueWithResult("second", 2) else failure(x))
      assert(sut.nonaborts == Chain("first", "second"))
      assert(sut.getResult.contains(2))
    }
  }

  "biflatMap" should {
    "call the second continuation for results" in {
      val sut = Checked.continueWithResult("first", 1)
      val test = sut.biflatMap(failure, result => Checked.continueWithResult("second", result + 2))
      assert(test.nonaborts == Chain("first", "second"))
      assert(test.getResult.contains(3))
    }

    "call the first continuation for aborts" in {
      val sut = Checked.abort(1).prependNonabort("first")
      val test = sut.biflatMap(abort => Checked.continueWithResult("second", abort + 3), failure)
      assert(test.nonaborts == Chain("first", "second"))
      assert(test.getResult.contains(4))
    }
  }

  "abortFlatMap" should {
    "not touch results" in {
      val sut = Checked.continueWithResult("first", 1)
      assert(sut.abortFlatMap(failure) == sut)
    }

    "call the continuation for aborts" in {
      val sut: Checked[Int, String, Int] = Checked.abort(1).prependNonabort("first")
      val test = sut.abortFlatMap(abort => Checked.continueWithResult("second", abort + 3))
      assert(test.nonaborts == Chain("first", "second"))
      assert(test.getResult.contains(4))
    }
  }

  "toResult" should {
    "not touch results" in {
      val sut = Checked.continueWithResult("first", 1)
      assert(sut.toResult(2) == sut)
    }

    "merge abort with the nonaborts" in {
      val sut = Checked.abort("abort").prependNonaborts(Chain("first", "second"))
      assert(
        sut.toResult(3) == Checked
          .continueWithResult("second", 3)
          .prependNonaborts(Chain("abort", "first"))
      )
    }

  }

  "foreach" should {
    "do nothing on an abort" in {
      val sut: Checked[String, Int, Double] = Checked.abort("failure")
      sut.foreach(failure)
    }

    "run the function on the result" in {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var run = false
      Checked.result(5).foreach(x => if (x == 5) run = true)
      assert(run)
    }
  }

  "exists" should {
    "return false on an abort" in {
      val sut: Checked[String, String, String] = Checked.abort("abort").appendNonabort("nonabort")
      assert(!sut.exists(failure))
    }

    "evaluate the predicate on the result" in {
      assert(Checked.result(5).exists(x => x == 5))
      assert(!Checked.result(5).exists(x => x != 5))
    }
  }

  "forall" should {
    "return true on an abort" in {
      val sut: Checked[String, String, String] = Checked.abort("abort").appendNonabort("nonabort")
      assert(sut.forall(failure))
    }

    "evaluate the predicate on the result" in {
      assert(Checked.result(5).forall(x => x == 5))
      assert(!Checked.result(5).forall(x => x != 5))
    }
  }

  "traverse" should {
    def f(x: String): Option[Int] = Some(x.length)

    def g(x: String): Option[Int] = None

    "run on a result" in {
      val sut = Checked.continueWithResult("nonabort", "result")
      assert(sut.traverse(f) == Some(Checked.continueWithResult("nonabort", 6)))
      assert(sut.traverse(g) == None)
    }

    "embed the abort" in {
      val sut: Checked[String, String, String] = Checked.abort("abort").appendNonabort("nonabort")
      assert(sut.traverse(f) == Some(sut))
      assert(sut.traverse(g) == Some(sut))
    }
  }

  "toEither" should {
    "map abort to Left" in {
      assert(Checked.abort("abort").appendNonabort("nonabort").toEither == Left("abort"))
    }

    "map result to Right" in {
      assert(Checked.continueWithResult("nonabort", "result").toEither == Right("result"))
    }
  }

  "toEitherWithNonaborts" should {
    "map abort to Left" in {
      assert(
        Checked.abort("abort").appendNonabort("nonabort").toEitherWithNonaborts == Left(
          NonEmptyChain("abort", "nonabort")
        )
      )
    }

    "map result to Right" in {
      assert(Checked.result("result").toEitherWithNonaborts == Right("result"))
    }

    "map nonaborts to Left" in {
      assert(
        Checked.continueWithResult("nonabort", "result").toEitherWithNonaborts == Left(
          NonEmptyChain("nonabort")
        )
      )
    }
  }

  "toOption" should {
    "map abort to None" in {
      assert(Checked.abort("abort").appendNonabort("nonabort").toOption == None)
    }

    "map result to Some" in {
      assert(Checked.continueWithResult("nonabort", "result").toOption == Some("result"))
    }
  }

  "isAbort" should {
    "identify aborts" in {
      assert(Checked.abort(5).isAbort)
      assert(!Checked.result(5).isAbort)
      assert(!Checked.continueWithResult(4, 5).isAbort)
    }
  }

  "isResult" should {
    "identify results" in {
      assert(!Checked.abort(5).isResult)
      assert(Checked.result(5).isResult)
      assert(Checked.continueWithResult(4, 5).isResult)
    }
  }

  "successful" should {
    "identify results without nonaborts" in {
      assert(!Checked.abort(5).successful)
      assert(Checked.result(5).successful)
      assert(!Checked.continueWithResult(4, 5).successful)
    }
  }

  "fromEither" should {
    "map Left to abort" in {
      assert(Checked.fromEither(Left("abc")) == Checked.abort("abc"))
    }

    "map Right to result" in {
      assert(Checked.fromEither(Right("abc")) == Checked.result("abc"))
    }
  }

  "fromEitherNonabort" should {
    "map Left to nonabort" in {
      assert(Checked.fromEitherNonabort(1)(Left(3)) == Checked.continueWithResult(3, 1))
    }
    "map Right to result" in {
      assert(Checked.fromEitherNonabort(1)(Right(3)) == Checked.result(3))
    }
  }

  "fromEitherT" should {
    "map Left to abort" in {
      assert(Checked.fromEitherT(EitherT.leftT[Option, Int](5)) == Some(Checked.abort(5)))
      assert(Checked.fromEitherT(EitherT.left[Int](Option.empty[Int])) == None)
    }

    "map Right to result" in {
      assert(Checked.fromEitherT(EitherT.rightT[Option, Int](5)) == Some(Checked.result(5)))
      assert(Checked.fromEitherT(EitherT.right[Int](Option.empty[Int])) == None)
    }
  }

  "fromEitherTNonabort" should {
    "map Left to nonabort" in {
      assert(
        Checked.fromEitherTNonabort(12)(EitherT.leftT[Option, Int](5)) == Some(
          Checked.continueWithResult(5, 12)
        )
      )
      assert(Checked.fromEitherTNonabort(12)(EitherT.left[Int](Option.empty[Int])) == None)
    }

    "map Right to result" in {
      assert(
        Checked.fromEitherTNonabort(12)(EitherT.rightT[Option, Int](5)) == Some(Checked.result(5))
      )
      assert(Checked.fromEitherTNonabort(12)(EitherT.right[Int](Option.empty[Int])) == None)
    }
  }

  "MonadError" should {
    import CheckedTest.{arbitraryChecked, eqChecked}
    checkAllLaws(
      "MonadError",
      MonadErrorTests[Checked[Int, String, *], Int].monadError[Int, Int, String],
    )
  }

  private lazy val stackSafetyDepth = 20000

  "traverse" should {
    "be stack safe" in {
      (1 to stackSafetyDepth: Seq[Int]).traverse(x => Checked.result(x)).getResult.value should
        have size stackSafetyDepth.toLong
    }
  }

  "traverse_" should {
    "be stack safe" in {
      (1 to stackSafetyDepth: Seq[Int])
        .traverse_(x => Checked.result(x))
        .getResult
        .value should be(())
    }
  }
}

object CheckedTest extends AnyWordSpecLike {

  implicit def eqChecked[A: Eq, N: Eq, R: Eq]: Eq[Checked[A, N, R]] = Eq.fromUniversalEquals

  implicit def arbitraryChecked[A: Arbitrary, N: Arbitrary, R: Arbitrary]
      : Arbitrary[Checked[A, N, R]] =
    Arbitrary(
      Gen.oneOf(
        for {
          abort <- Arbitrary.arbitrary[A]
          nonaborts <- Arbitrary.arbitrary[List[N]].map(Chain.fromSeq)
        } yield Checked.Abort(abort, nonaborts),
        for {
          nonaborts <- Arbitrary.arbitrary[List[N]].map(Chain.fromSeq)
          result <- Arbitrary.arbitrary[R]
        } yield Checked.Result(nonaborts, result),
      )
    )
}
