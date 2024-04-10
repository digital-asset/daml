// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.scalatest.wordspec.AnyWordSpec

class CheckedTest extends AnyWordSpec with BaseTest {

  "checked" must {
    "be the identity operation" in {
      checked[Int](5) shouldBe 5
      checked[String]("abc") shouldBe "abc"
    }
  }

  class TestException extends RuntimeException

  "checked" must {
    "show up in the stack trace" in {
      def throwException: TestException = throw new TestException

      val ex: TestException =
        try { checked(throwException) }
        catch { case ex: TestException => ex }
      assert(
        ex.getStackTrace.exists(ste =>
          ste.getMethodName == "checked" && ste.getClassName == "com.digitalasset.canton.package$"
        ),
        ex.getStackTrace.mkString(", "),
      )
    }
  }

}
