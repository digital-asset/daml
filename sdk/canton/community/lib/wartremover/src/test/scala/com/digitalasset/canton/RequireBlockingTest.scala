// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

class RequireBlockingTest extends AnyWordSpec with Matchers {

  def assertIsErrorSynchronized(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach(_ should include(RequireBlocking.messageSynchronized))
    succeed
  }

  def assertIsErrorThreadSleep(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach(_ should include(RequireBlocking.messageThreadSleep))
    succeed
  }

  "RequireBlocking" should {
    "detect this-qualified synchronized statements without blocking context" in {
      val result = WartTestTraverser(RequireBlocking) {
        this.synchronized(42)
      }
      assertIsErrorSynchronized(result)
    }

    "detect unqualified synchronized statements without blocking context" in {
      val result = WartTestTraverser(RequireBlocking) {
        synchronized(42)
      }
      assertIsErrorSynchronized(result)
    }

    "detect arbitrary synchronized statements without blocking context" in {
      val result = WartTestTraverser(RequireBlocking) {
        new Object().synchronized(42)
      }
      assertIsErrorSynchronized(result)
    }

    "detect nested synchronized statements in the receiver" in {
      val result = WartTestTraverser(RequireBlocking) {
        this.synchronized(this).synchronized(32)
      }
      result.errors.length shouldBe 2
      result.errors.foreach(_ should include(RequireBlocking.messageSynchronized))
    }

    "forbid Thread.sleep" in {
      val result = WartTestTraverser(RequireBlocking) {
        Thread.sleep(1)
      }
      assertIsErrorThreadSleep(result)
    }

    "fail to forbid renamed Thread.sleep" in {
      val result = WartTestTraverser(RequireBlocking) {
        import Thread.sleep as foo
        foo(1)
      }
      // assertIsErrorThreadSleep(result)
      result.errors shouldBe Seq.empty
    }
  }
}
