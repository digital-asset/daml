// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.blocking

class RequireBlockingTest extends AnyWordSpec with Matchers {

  def assertIsErrorSynchronized(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach { _ should include(RequireBlocking.messageSynchronized) }
    succeed
  }

  def assertIsErrorThreadSleep(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach { _ should include(RequireBlocking.messageThreadSleep) }
    succeed
  }

  "RequireBlocking" should {
    "detect this-qualified synchronized statements without blocking context" in {
      val result = WartTestTraverser(RequireBlocking) {
        this.synchronized { 42 }
      }
      assertIsErrorSynchronized(result)
    }

    "detect unqualified synchronized statements without blocking context" in {
      val result = WartTestTraverser(RequireBlocking) {
        synchronized { 42 }
      }
      assertIsErrorSynchronized(result)
    }

    "detect arbitrary synchronized statements without blocking context" in {
      val result = WartTestTraverser(RequireBlocking) {
        new Object().synchronized { 42 }
      }
      assertIsErrorSynchronized(result)
    }

    "detect nested synchronized statements in the receiver" in {
      val result = WartTestTraverser(RequireBlocking) {
        this.synchronized(this).synchronized(32)
      }
      result.errors.length shouldBe 2
      result.errors.foreach { _ should include(RequireBlocking.messageSynchronized) }
    }

    "detect nested synchronized calls in the body" in {
      // Technically we shouldn't require another blocking around the inner synchronized,
      // but that's a false positive we can live with as nested synchronization calls are anyway
      // dangerous for their deadlock potential.
      val result = WartTestTraverser(RequireBlocking) {
        blocking { this.synchronized { new Object().synchronized { 17 } } }
      }
      assertIsErrorSynchronized(result)
    }

    "detect escaping synchronized calls in the body" in {
      // The inner synchronize call escapes the blocking scope
      val result = WartTestTraverser(RequireBlocking) {
        val f = blocking {
          this.synchronized { () =>
            this.synchronized(42)
          }
        }
        f()
      }
      assertIsErrorSynchronized(result)
    }

    "fail to detect renamed synchronized" in {
      val result = WartTestTraverser(RequireBlocking) {
        val x = new Object()
        import x.{synchronized as foo}
        foo(19)
      }
      // assertIsErrorSynchronized(result)
      result.errors shouldBe Seq.empty
    }

    "allow synchronized statements inside blocking calls" in {
      val result = WartTestTraverser(RequireBlocking) {
        blocking { this.synchronized { 42 } }
        blocking { synchronized { 23 } }
        blocking { synchronized { blocking { new Object().synchronized { 17 } } } }
      }
      result.errors shouldBe Seq.empty
    }

    "forbid Thread.sleep" in {
      val result = WartTestTraverser(RequireBlocking) {
        Thread.sleep(1)
      }
      assertIsErrorThreadSleep(result)
    }

    "fail to forbid renamed Thread.sleep" in {
      val result = WartTestTraverser(RequireBlocking) {
        import Thread.{sleep as foo}
        foo(1)
      }
      // assertIsErrorThreadSleep(result)
      result.errors shouldBe Seq.empty
    }
  }
}
