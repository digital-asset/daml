// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  TimestampConversionError,
}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.serialization.ProtoConverter.{
  InstantConverter,
  parseNonNegativeInt,
  parseNonNegativeLong,
  parsePositiveInt,
  parsePositiveLong,
  required,
}
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

  "parse" should {

    "PositiveInt" in {
      parsePositiveInt("field name", 1).value shouldBe PositiveInt.one
      parsePositiveInt("field name", 0).left.value should matchPattern {
        case InvariantViolation(Some("field name"), _) =>
      }
    }

    "PositiveLong" in {
      parsePositiveLong("field name", 1).value shouldBe PositiveLong.one
      parsePositiveLong("field name", 0).left.value should matchPattern {
        case InvariantViolation(Some("field name"), _) =>
      }
    }

    "NonNegativeInt" in {
      parseNonNegativeInt("field name", 0).value shouldBe NonNegativeInt.zero
      parseNonNegativeInt("field name", -1).left.value should matchPattern {
        case InvariantViolation(Some("field name"), _) =>
      }
    }

    "NonNegativeLong" in {
      parseNonNegativeLong("field name", 0).value shouldBe NonNegativeLong.zero
      parseNonNegativeLong("field name", -1).left.value should matchPattern {
        case InvariantViolation(Some("field name"), _) =>
      }
    }
  }
}
