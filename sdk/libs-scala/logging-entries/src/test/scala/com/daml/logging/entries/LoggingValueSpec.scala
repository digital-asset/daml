// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging.entries

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LoggingValueSpec extends AnyWordSpec with Matchers {
  "a string logging value" should {
    "be constructed" in {
      val loggingValue = LoggingValue.OfString("hello")
      loggingValue.value should be("hello")
    }

    "truncate to a specified length" in {
      val loggingValue =
        LoggingValue.OfString("Hello, O beautious and bountiful world!").truncated(16)
      loggingValue.value should be("Hello, O beauti…")
    }

    "not truncate if it's under the length specified." in {
      val loggingValue = LoggingValue.OfString("Hello, world!").truncated(16)
      loggingValue.value should be("Hello, world!")
    }

    "not truncate if it's exactly the length specified." in {
      val loggingValue = LoggingValue.OfString("Hi, Earth!").truncated(10)
      loggingValue.value should be("Hi, Earth!")
    }
  }
}
