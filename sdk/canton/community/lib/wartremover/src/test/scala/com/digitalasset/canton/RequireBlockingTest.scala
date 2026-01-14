// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
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

    "detect renamed synchronized (fails on Scala 2)" in {
      val result = WartTestTraverser(RequireBlocking) {
        val x = new Object()
        import x.synchronized as foo
        foo(19)
      }

      if (ScalaVersion.isScala3) assertIsErrorSynchronized(result)
      else result.errors shouldBe Seq.empty
    }

    "forbid Thread.sleep" in {
      val result = WartTestTraverser(RequireBlocking) {
        Thread.sleep(1)
      }
      assertIsErrorThreadSleep(result)
    }

    "forbid renamed Thread.sleep (fails on Scala 2)" in {
      val result = WartTestTraverser(RequireBlocking) {
        import Thread.sleep as foo
        foo(1)
      }

      if (ScalaVersion.isScala3) assertIsErrorThreadSleep(result)
      else result.errors shouldBe Seq.empty
    }
  }
}
