// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.scalatest.wordspec.AnyWordSpec

class PrivateConstructorTest extends AnyWordSpec with BaseTest {
  "private constructor" must {
    "taken into account" in {
      assertDoesNotCompile(
        """
          |final case class MyClass private (i: Int)
          |
          |object Other {
          |  val instance = MyClass(42)
          |  println(instance)
          |}
          |""".stripMargin
      )
    }
  }
}
