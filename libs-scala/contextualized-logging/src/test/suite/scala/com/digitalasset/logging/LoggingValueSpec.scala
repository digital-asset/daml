// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.io.StringWriter

import com.daml.logging.LoggingValueSpec._
import com.fasterxml.jackson.core.JsonFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LoggingValueSpec extends AnyWordSpec with Matchers {
  "a logging value" can {
    "be constructed from a string" in {
      val value = LoggingValue.from("foo bar")
      writingToGenerator(value) should be("\"foo bar\"")
    }

    "be constructed from an int" in {
      val value = LoggingValue.from(1981)
      writingToGenerator(value) should be("1981")
    }

    "be constructed from a long" in {
      val value = LoggingValue.from(Long.MaxValue)
      writingToGenerator(value) should be("9223372036854775807")
    }

    "be constructed from a defined optional value" in {
      val value = LoggingValue.from(Some(99))
      writingToGenerator(value) should be("99")
    }

    "be constructed from an empty optional value" in {
      val value = LoggingValue.from(None: Option[String])
      writingToGenerator(value) should be("null")
    }

    "be constructed from a sequence" in {
      val value = LoggingValue.from(Seq("a", "b", "c"))
      writingToGenerator(value) should be("[\"a\",\"b\",\"c\"]")
    }

    "be constructed from an empty sequence" in {
      val value = LoggingValue.from(Seq.empty[Long])
      writingToGenerator(value) should be("[]")
    }

    "be constructed from a sequence view" in {
      val value = LoggingValue.from(Seq(1, 4, 7).view.map(_ * 2))
      writingToGenerator(value) should be("[2,8,14]")
    }
  }
}

object LoggingValueSpec {
  private val testJsonFactory = new JsonFactory

  private def writingToGenerator(value: LoggingValue): String = {
    val writer = new StringWriter
    val generator = testJsonFactory.createGenerator(writer)
    value.writeTo(generator)
    generator.flush()
    writer.toString
  }
}
