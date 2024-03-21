// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JsonStringSerializerSpec extends AnyWordSpec with Matchers {
  "serializing a value to a string" can {
    "serialize null" in {
      val result = JsonStringSerializer.serialize { generator => generator.writeNull() }
      result should be("null")
    }

    "serialize booleans" in {
      val result = JsonStringSerializer.serialize { generator => generator.writeBoolean(true) }
      result should be("true")
    }

    "serialize strings" in {
      val result = JsonStringSerializer.serialize { generator => generator.writeString("hello") }
      result should be("\"hello\"")
    }

    "serialize numbers" in {
      val result = JsonStringSerializer.serialize { generator => generator.writeNumber(99) }
      result should be("99")
    }

    "serialize objects minimally, with spaces, and no quotes around field names" in {
      val result = JsonStringSerializer.serialize { generator =>
        generator.writeStartObject()
        generator.writeFieldName("foo")
        generator.writeNumber(7)
        generator.writeFieldName("bar")
        generator.writeString("eleven")
        generator.writeEndObject()
      }
      result should be("{foo: 7, bar: \"eleven\"}")
    }

    "serialize empty objects minimally" in {
      val result = JsonStringSerializer.serialize { generator =>
        generator.writeStartObject()
        generator.writeEndObject()
      }
      result should be("{}")
    }

    "serialize arrays minimally, but with spaces" in {
      val result = JsonStringSerializer.serialize { generator =>
        generator.writeStartArray()
        generator.writeNumber(1)
        generator.writeNumber(2)
        generator.writeString("fizz")
        generator.writeNull()
        generator.writeString("buzz")
        generator.writeEndArray()
      }
      result should be("[1, 2, \"fizz\", null, \"buzz\"]")
    }

    "serialize empty arrays minimally" in {
      val result = JsonStringSerializer.serialize { generator =>
        generator.writeStartArray()
        generator.writeEndArray()
      }
      result should be("[]")
    }
  }
}
