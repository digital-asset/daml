// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.{Future, Promise}

class SynchronizedFutureTest extends AnyWordSpec with Matchers {
  import SynchronizedFutureTest.*

  def assertIsErrorSynchronized(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach { _ should include(SynchronizedFuture.messageSynchronized) }
    succeed
  }

  "SynchronizedFuture" should {
    "detect qualified synchronized statements" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        this.synchronized { Future.unit }
      }
      assertIsErrorSynchronized(result)
    }

    "work without qualifier" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized { Future.unit }
      }
      assertIsErrorSynchronized(result)
    }

    "detect nested synchronized statements in the body" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized {
          synchronized {
            Future.unit
          }
        }
      }
      assertIsErrorSynchronized(result)
    }

    "detect nested synchronized statements in the receiver" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        { synchronized { Future.unit } }.synchronized { 23 }
      }
      assertIsErrorSynchronized(result)
    }

    "allow non-future types in synchronized blocks" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized { 42 }
      }
      result.errors.length shouldBe 0
    }

    "report a false positive when futures are data" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized {
          Promise[Unit]().future
        }
      }
      assertIsErrorSynchronized(result)
    }

    "detects futures wrapped in an EitherT" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized { EitherT(Future.successful(Either.right(()))) }
      }
      assertIsErrorSynchronized(result)
    }

    "detects futures wrapped in an OptionT" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized { OptionT(Future.successful(Option(()))) }
      }
      assertIsErrorSynchronized(result)
    }

    "detects futures that are deeply wrapped" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized { OptionT(EitherT(Future.successful(Either.right(Option(()))))) }
      }
      assertIsErrorSynchronized(result)
    }

    "detect future-like types" in {
      val result = WartTestTraverser(SynchronizedFuture) {
        synchronized { new LooksLikeAFuture() }
      }
      assertIsErrorSynchronized(result)
    }
  }
}

object SynchronizedFutureTest {
  @DoNotReturnFromSynchronizedLikeFuture class LooksLikeAFuture
}
