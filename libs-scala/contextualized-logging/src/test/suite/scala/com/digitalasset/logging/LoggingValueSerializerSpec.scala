// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.io.StringWriter

import com.daml.logging.LoggingValueSerializerSpec._
import com.daml.logging.entries.LoggingValue
import com.fasterxml.jackson.core.JsonFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LoggingValueSerializerSpec extends AnyWordSpec with Matchers {
  "the serializer" can {
    "serialize the boolean `false`" in {
      val value = LoggingValue.from(false)
      writingToGenerator(value) should be("false")
    }

    "serialize the boolean `true`" in {
      val value = LoggingValue.from(true)
      writingToGenerator(value) should be("true")
    }

    "serialize a string" in {
      val value = LoggingValue.from("foo bar")
      writingToGenerator(value) should be("\"foo bar\"")
    }

    "serialize an int" in {
      val value = LoggingValue.from(1981)
      writingToGenerator(value) should be("1981")
    }

    "serialize a long" in {
      val value = LoggingValue.from(Long.MaxValue)
      writingToGenerator(value) should be("9223372036854775807")
    }

    "serialize a defined optional value" in {
      val value = LoggingValue.from(Some(99))
      writingToGenerator(value) should be("99")
    }

    "serialize an empty optional value" in {
      val value = LoggingValue.from(None: Option[String])
      writingToGenerator(value) should be("null")
    }

    "serialize a sequence" in {
      val value = LoggingValue.from(Seq("a", "b", "c"))
      writingToGenerator(value) should be("[\"a\",\"b\",\"c\"]")
    }

    "serialize an empty sequence" in {
      val value = LoggingValue.from(Seq.empty[Long])
      writingToGenerator(value) should be("[]")
    }

    "serialize a sequence view" in {
      val value = LoggingValue.from(Seq(1, 4, 7).view.map(_ * 2))
      writingToGenerator(value) should be("[2,8,14]")
    }
  }
}

object LoggingValueSerializerSpec {
  private val testJsonFactory = new JsonFactory

  private def writingToGenerator(value: LoggingValue): String = {
    val writer = new StringWriter
    val generator = testJsonFactory.createGenerator(writer)
    LoggingValueSerializer.writeValue(value, generator)
    generator.flush()
    writer.toString
  }
}
