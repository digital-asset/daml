// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LoggingValueSpec extends AnyWordSpec with Matchers {
  "a logging value" can {
    "be constructed from a string" in {
      LoggingValue.from("foo bar").value should be("foo bar")
    }

    "be constructed from an int" in {
      LoggingValue.from(1981).value should be("1981")
    }

    "be constructed from a long" in {
      LoggingValue.from(Long.MaxValue).value should be("9223372036854775807")
    }

    "be constructed from a defined optional value" in {
      LoggingValue.from(Some(99)).value should be("99")
    }

    "be constructed from an empty optional value" in {
      LoggingValue.from(None: Option[String]).value should be("")
    }

    "be constructed from a sequence" in {
      LoggingValue.from(Seq("a", "b", "c")).value should be("[a, b, c]")
    }

    "be constructed from an empty sequence" in {
      LoggingValue.from(Seq.empty[Long]).value should be("[]")
    }

    "be constructed from a sequence view" in {
      LoggingValue.from(Seq(1, 4, 7).view.map(_ * 2)).value should be("[2, 8, 14]")
    }
  }
}
