// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DecimalSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  private def d(s: String) = BigDecimal(s).setScale(Decimal.scale).bigDecimal

  "Decimal.fromString" should {

    "accept properly formed string" in {

      val testCases = Table(
        "decimal without sign",
        "0",
        "1",
        "123",
        "123.4",
        "0.123",
        "1.234",
        "12.34",
        "00000",
        "01",
      )

      val signs = Table("sign", "", "+", "-")

      forEvery(signs) { sign =>
        forEvery(testCases) { testCase =>
          val decimal = sign + testCase
          Decimal.fromString(decimal) shouldBe Right(d(decimal))
        }
      }
    }

    "reject strings that contain non expected characters" in {

      val testCases = Table("strings", "a", "decimal", "0x00", "1E10", "2-1", "2+2", "2*3", "55/11")

      forEvery(testCases) { testCase =>
        Decimal.fromString(testCase) shouldBe a[Left[_, _]]
      }

    }

    "reject improperly formatted decimal with one dot" in {

      val testCases = Table(
        "string",
        ".",
        ".0",
        "0.",
        "1.",
        ".1",
        "123.",
        ".0",
        ".0123",
        ".123",
      )

      val signs = Table("sign", "", "+", "-")

      forEvery(signs) { sign =>
        forEvery(testCases) { testCase =>
          val decimal = sign + testCase
          Decimal.fromString(decimal) shouldBe a[Left[_, _]]
        }
      }
    }

    "reject strings containing more than one dot" in {

      val testCases = Table(
        "string",
        "..",
        "0..",
        "..1",
        "112..123",
        "0.0.",
        ".1.",
      )

      forEvery(testCases)(testCase => Decimal.fromString(testCase) shouldBe a[Left[_, _]])

    }

    "reject string with too many signs" in {

      val testCases = Table(
        "decimal without sign",
        "1",
        "123",
        "12.34",
      )

      val signs = Table("sign", "++", "-+", "+-", "--", "+++", "-+-")

      forEvery(signs) { sign =>
        forEvery(testCases) { testCase =>
          val decimal = sign + testCase
          Decimal.fromString(decimal) shouldBe a[Left[_, _]]
        }
      }

    }

    "reject string with too many digit" in {

      val negativeTestCases = Table(
        "string",
        "0" * 1 + "." + "0" * 1,
        "0" * 28 + "." + "0" * 1,
        "0" * 1 + "." + "0" * 10,
        "0" * 28 + "." + "0" * 10,
      )

      val positiveTestCases = Table(
        "string",
        "0" * 29 + "." + "0" * 1,
        "0" * 1 + "." + "0" * 11,
        "0" * 29 + "." + "0" * 11,
        "1" * 29 + "." + "1" * 1,
        "1" * 50 + "." + "1" * 1,
        "1" * 1 + "." + "1" * 11,
        "1" * 1 + "." + "1" * 30,
        "1" * 29 + "." + "1" * 11,
        "1" * 55 + "." + "1" * 33,
      )

      forEvery(negativeTestCases)(testCase =>
        Decimal.fromString(testCase) shouldBe Right(d(testCase))
      )

      forEvery(positiveTestCases)(testCase => Decimal.fromString(testCase) shouldBe a[Left[_, _]])
    }

  }

}
