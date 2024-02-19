// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, TimestampConversionError}
import com.digitalasset.canton.serialization.ProtoConverter.{InstantConverter, required}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ProtoConverterTest extends AnyWordSpec with BaseTest {
  "InstantConverters" should {
    "deserialize a timestamp" in {
      val now = Instant.now()
      val timestamp = InstantConverter.toProtoPrimitive(now)
      val instant = InstantConverter.fromProtoPrimitive(timestamp)
      assertResult(now)(instant.value)
    }
    "fail if timestamp is out of range" in {
      val timestamp = InstantConverter.toProtoPrimitive(Instant.MAX)
      val greaterThanMax = timestamp.copy(seconds = timestamp.seconds + 1)
      val errorOrInstant = InstantConverter.fromProtoPrimitive(greaterThanMax)

      errorOrInstant.left.value should matchPattern { case TimestampConversionError(_) => }
    }
  }

  "required" should {
    "return an error if the field is missing" in {
      required("test", None).left.value should matchPattern { case FieldNotSet("test") =>
      }
    }

    "return field value if available" in {
      required("test", Some("value")).value shouldBe "value"
    }
  }
}
