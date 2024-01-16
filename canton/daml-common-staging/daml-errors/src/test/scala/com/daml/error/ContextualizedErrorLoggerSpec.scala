// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ContextualizedErrorLoggerSpec extends AnyFlatSpec with Matchers {

  it should "sort entries by keys and skip empty values" in {
    val contextMap = Map("c" -> "C", "a" -> "A", "b" -> "B", "empty value" -> "")

    val actual =
      ContextualizedErrorLogger.formatContextAsString(contextMap)

    actual shouldBe "a=A, b=B, c=C"
  }
}
