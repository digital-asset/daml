// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.future

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.scalautil.future.FutureConversionSpec.TestException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.jdk.FutureConverters._

class FutureConversionSpec extends AsyncWordSpec with Matchers {
  import FutureConversion._

  "converting a java CompletionStage into a scala Future" should {

    "succeed" in {
      CompletableFuture.completedStage(()).thenApply(_ => succeed).toScalaUnwrapped
    }

    "fail the future with the same exception as the CompletionStage when not wrapped in a CompletionException" in {
      val exception = new TestException
      // build a completion stage that fails with CompletionException
      // this is NOT the same as CompletableFuture.failedStage
      val cs: CompletionStage[Unit] = CompletableFuture.failedStage(exception)
      recoverToExceptionIf[TestException](cs.toScalaUnwrapped).map { ex => ex shouldBe exception }
    }

    "fail the future with the same exception as the CompletionStage when wrapped in a CompletionException" in {
      val exception = new TestException
      // build a completion stage that fails with CompletionException
      // this is NOT the same as CompletableFuture.failedStage
      val cs: CompletionStage[Unit] =
        CompletableFuture.completedStage(()).thenApply(_ => throw exception)
      recoverToExceptionIf[TestException](cs.toScalaUnwrapped).map { ex => ex shouldBe exception }
    }

    "convert futures and have the same result when wrapped in a CompletionException" in {
      val exception = new TestException
      val failedFuture = Future.failed(exception)
      // For the CompletionStage to complete with CompletionException
      // we need to call whenComplete even if that does not affect the result of the CompletionStage
      recoverToExceptionIf[TestException](failedFuture.asJava.whenComplete {
        (_: Unit, _: Throwable) =>
          ()
      }.toScalaUnwrapped)
        .map { ex =>
          ex shouldBe exception
        }
    }

  }

}

object FutureConversionSpec {
  private class TestException extends RuntimeException
}
