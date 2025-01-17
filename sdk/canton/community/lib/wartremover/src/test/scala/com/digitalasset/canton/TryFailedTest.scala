// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.util.{Success, Try}

class TryFailedTest extends AnyWordSpec with Matchers {

  def assertIsError(result: WartTestTraverser.Result): Assertion = {
    result.errors.length should be >= 1
    result.errors.foreach {
      _ should include(TryFailed.message)
    }
    succeed
  }

  "Try.failed" should {
    "flag calls Try.failed" in {
      val result = WartTestTraverser(TryFailed) {
        val attempt = (??? : Try[Int])
        attempt.failed.foreach { _ => () }
      }
      assertIsError(result)
    }

    "flag calls Success.failed" in {
      val result = WartTestTraverser(TryFailed) {
        val attempt = (??? : Success[Int])
        attempt.failed.foreach { _ => () }
      }
      assertIsError(result)
    }

    "allow other methods" in {
      val result = WartTestTraverser(TryFailed) {
        val attempt = (??? : Try[Int])
        attempt.get
      }
      result.errors shouldBe List.empty
    }
  }
}
