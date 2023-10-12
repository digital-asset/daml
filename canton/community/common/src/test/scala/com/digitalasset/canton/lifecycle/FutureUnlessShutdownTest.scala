// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.{BaseTest, DiscardedFuture, DiscardedFutureTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

class FutureUnlessShutdownTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "DiscardedFuture" should {
    "detect discarded FutureUnlessShutdown" in {
      val result = WartTestTraverser(DiscardedFuture) {
        FutureUnlessShutdown.pure(())
        ()
      }
      DiscardedFutureTest.assertErrors(result, 1)
    }

    "detect discarded FutureunlessShutdown when wrapped" in {
      val result = WartTestTraverser(DiscardedFuture) {
        EitherT(FutureUnlessShutdown.pure(Either.right(())))
        ()
      }
      DiscardedFutureTest.assertErrors(result, 1)
    }
  }

  "failOnShutdownTo" should {
    "fail to a Throwable on shutdown" in {
      val fus = FutureUnlessShutdown.abortedDueToShutdown.failOnShutdownTo(
        new RuntimeException("boom")
      )

      a[RuntimeException] shouldBe thrownBy {
        fus.futureValue
      }
    }

    "not evaluate the Throwable if the result is an outcome" in {
      var wasEvaluated = false
      val fus = FutureUnlessShutdown.unit.failOnShutdownTo {
        wasEvaluated = true
        new RuntimeException("boom")
      }

      fus.futureValue
      wasEvaluated shouldBe false
    }
  }
}
