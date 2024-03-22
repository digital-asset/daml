// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.future

import java.util.concurrent.{CompletableFuture, CompletionException, CompletionStage}

import com.daml.scalautil.future.FutureConversionSpec.TestException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.jdk.FutureConverters._

class FutureConversionSpec extends AsyncWordSpec with Matchers {
  import FutureConversion._

  "converting a java CompletionStage into a scala Future" should {

    "succeed" in {
      CompletableFuture.completedFuture(()).thenApply(_ => succeed).toScalaUnwrapped
    }

    "fail the future with the same exception as the CompletionStage when not wrapped in a CompletionException" in {
      val exception = new TestException
      // build a completion stage that fails with CompletionException
      // this is NOT the same as CompletableFuture.failedStage
      val cs: CompletionStage[Unit] = CompletableFuture.failedFuture(exception)
      recoverToExceptionIf[TestException](cs.toScalaUnwrapped).map(_ shouldBe exception)
    }

    "fail the future with the same exception as the CompletionStage when wrapped in a CompletionException" in {
      val exception = new TestException
      // build a completion stage that fails with CompletionException
      // this is NOT the same as CompletableFuture.failedStage
      val cs: CompletionStage[Unit] =
        CompletableFuture.completedFuture(()).thenApply(_ => throw exception)
      recoverToSucceededIf[CompletionException](cs.asScala).flatMap(_ =>
        recoverToExceptionIf[TestException](cs.toScalaUnwrapped).map(_ shouldBe exception)
      )
    }

    "convert futures and have the same result when wrapped in a CompletionException" in {
      val exception = new TestException
      val failedFuture = Future.failed(exception)
      // For the CompletionStage to complete with CompletionException
      // we need to call whenComplete even if that does not affect the result of the CompletionStage
      val cs = failedFuture.asJava.whenComplete { (_: Unit, _: Throwable) =>
        ()
      }
      recoverToSucceededIf[CompletionException](cs.asScala).flatMap(_ =>
        recoverToExceptionIf[TestException](cs.toScalaUnwrapped).map(_ shouldBe exception)
      )
    }

  }

}

object FutureConversionSpec {
  private class TestException extends RuntimeException
}
