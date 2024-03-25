// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import com.daml.logging.entries.{LoggingEntries, LoggingValue}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoggingValueStringSerializerSpec extends AnyFlatSpec with Matchers {

  it should "convert logging values to strings" in {

    def test(v: LoggingValue): String =
      LoggingValueStringSerializer.makeString(v)

    test(LoggingValue.Empty) shouldBe ""
    test(LoggingValue.False) shouldBe "false"
    test(LoggingValue.True) shouldBe "true"
    test(LoggingValue.OfString("string123")) shouldBe "'string123'"
    test(LoggingValue.OfInt(123)) shouldBe "123"
    test(LoggingValue.OfLong(123L)) shouldBe "123"
    test(LoggingValue.OfIterable(Seq(1, 2, 3))) shouldBe "[1, 2, 3]"
    test(
      LoggingValue.Nested(
        LoggingEntries(
          "key1" -> LoggingValue.Empty,
          "key2" -> LoggingValue.Nested(
            LoggingEntries(
              "key3" -> LoggingValue.True
            )
          ),
        )
      )
    ) shouldBe "{key1: , key2: {key3: true}}"
    test(LoggingValue.OfJson(spray.json.JsTrue)) shouldBe "true"

  }

}
