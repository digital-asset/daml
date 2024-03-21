// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.Semaphore
import scala.concurrent.Future
import scala.util.{Failure, Success}

class DirectExecutionContextTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val testException = new RuntimeException("test exception")

  val testThread: Thread = Thread.currentThread()
  def assertRunsInTestThread(): Assertion = Thread.currentThread() shouldBe testThread
  def assertRunsNotInTestThread(): Assertion = Thread.currentThread() should not be testThread

  "A direct execution context" should {
    "execute new futures synchronously in the calling thread" in {
      val future = Future {
        assertRunsInTestThread()
        1 + 1
      }(directExecutionContext)

      future.value shouldEqual Some(Success(2))
    }

    "embed exceptions from synchronous computations into the future" in {
      val future = Future {
        throw testException
      }(directExecutionContext)

      future.value shouldEqual Some(Failure(testException))
    }

    "log exceptions from asynchronous computations" in {
      loggerFactory.assertLogs(
        {
          val blocker = new Semaphore(0)

          val longRunningFuture = Future {
            assertRunsNotInTestThread()
            blocker.acquire()
            1 + 1
          }(parallelExecutionContext)

          longRunningFuture.onComplete { _ =>
            assertRunsNotInTestThread()
            throw testException
          }(directExecutionContext)

          blocker.release()

          longRunningFuture.futureValue shouldEqual 2
        },
        { err =>
          err.errorMessage shouldBe "A fatal error has occurred in DirectExecutionContext. Terminating thread."
          err.throwable shouldBe Some(testException)
        },
      )
    }

    "be stack-safe in general" in {
      logger.debug("Entering 'be stack-safe in general'...")
      def rec(n: Int): Future[Int] =
        Future
          .successful(n)
          .flatMap(i => if (i > 0) rec(i - 1) else Future.successful(0))(directExecutionContext)

      rec(100000).futureValue
    }
  }
}
