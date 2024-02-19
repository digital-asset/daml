// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.ExecutionContext

class GlobalExecutionContextTest extends AnyWordSpec with Matchers {

  def assertIsErrorGlobal(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach {
      _ should include(GlobalExecutionContext.messageGlobal)
    }
    succeed
  }

  def assertIsErrorParasitic(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach {
      _ should include(GlobalExecutionContext.messageParasitic)
    }
    succeed
  }

  "GlobalExecutionContext" should {
    "detect usages of the global execution context" in {
      val result = WartTestTraverser(GlobalExecutionContext) {
        ExecutionContext.global
      }
      assertIsErrorGlobal(result)
    }

    "detect implicit usages of the global execution context" in {
      val result = WartTestTraverser(GlobalExecutionContext) {
        import scala.concurrent.ExecutionContext.Implicits.*
        implicitly[ExecutionContext]
      }
      assertIsErrorGlobal(result)
    }

    "detect usages of the parasitic execution context" in {
      val result = WartTestTraverser(GlobalExecutionContext) {
        ExecutionContext.parasitic
      }
      assertIsErrorParasitic(result)
    }
  }
}
