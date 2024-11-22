// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class FutureAndThenTest extends AnyWordSpec with Matchers {

  def assertIsError(result: WartTestTraverser.Result): Assertion = {
    result.errors.length should be >= 1
    result.errors.foreach {
      _ should include(FutureAndThen.message)
    }
    succeed
  }

  "FutureAndThen" should {
    implicit def ec: ExecutionContext = ???

    "flag calls to Future.andThen" in {
      val result = WartTestTraverser(FutureAndThen) {
        val future = ??? : Future[Int]
        future.andThen(_ => ())
      }
      assertIsError(result)
    }

    "flag unapplied calls to Future.andThen" in {
      val result = WartTestTraverser(FutureAndThen) {
        val future = ??? : Future[Int]
        val x: PartialFunction[Try[Int], Any] => Future[Int] = future.andThen
        x(_ => ())
      }
      assertIsError(result)
    }

    "allow other methods" in {
      val result = WartTestTraverser(FutureAndThen) {
        val future = ??? : Future[Int]
        future.value
      }
      result.errors shouldBe List.empty
    }
  }
}
