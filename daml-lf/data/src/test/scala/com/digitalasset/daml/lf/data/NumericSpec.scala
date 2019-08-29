// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.math.BigDecimal

import org.scalatest

import scala.language.implicitConversions
import scala.math.{BigDecimal => BigDec}
import scala.util.Random

class NumericSpec
    extends scalatest.WordSpec
    with scalatest.Matchers
    with scalatest.prop.TableDrivenPropertyChecks {

  "fromBigDecimal" should {
    implicit def toBigDecimal(s: String): BigDecimal =
      new BigDecimal(s)

    "succeed for valid inputs" in {

      val testCases = Table[Int, BigDecimal](
        ("scale", "bigDecimal"),
        (0, "99999999999999999999999999999999999999."),
        (0, "-1."),
        (0, "-99999999999999999999999999999999999999."),
        (5, "1.00000"),
        (7, "0."),
        (1, "0.0000"),
        (10, "10.01234567890000000"),
        (10, "9999999999999999999999999999.9999999999"),
        (37, "0.0000000000000000000000000000000000001"),
        (38, "0.99999999999999999999999999999999999999"),
        (38, "0.00000000000000000000016180339887499677"),
        (38, "-0.99999999999999999999999999999999999999")
      )

      forEvery(testCases) { (scale, bigDec) =>
        Numeric.fromBigDecimal(scale, bigDec) shouldBe Right(bigDec.setScale(scale))
      }
    }

    "fail on too Large inputs" in {
      val testCases = Table[Int, BigDecimal](
        ("scale", "bigDecimal"),
        (0, "100000000000000000000000000000000000000."),
        (0, "-100000000000000000000000000000000000000."),
        (17, "10000000000000000000000.0000000000000000"),
        (3, "8284590452353602874713526624977572470."),
        (38, "1.00000000000000000000000000000000000000"),
        (38, "-1.00000000000000000000000000000000000001"),
      )

      forEvery(testCases) { (scale, bigDec) =>
        Numeric.fromBigDecimal(scale, bigDec) shouldBe 'left
      }
    }

    "fail on too precise inputs" in {
      val testCases = Table(
        ("scale", "bigDecimal"),
        (0, "0.1"),
        (0, "-0.1"),
        (17, "10000000000000000.000000000000000001"),
        (3, "0.82845"),
        (38, "0.00000000000000000000000000000000000000000100"),
        (38, "-0.000000000000000000000000000000000000001"),
      )

      forEvery(testCases) { (scale, bigDec) =>
        Numeric.fromBigDecimal(scale, bigDec) shouldBe 'left
      }
    }
  }

  "Numeric.add" should {

    import Numeric.add
    implicit def toNumeric(s: String): Numeric = Numeric.assertFromString(s)

    "return an error in case of overflow" in {
      val testCases = Table[Numeric, Numeric](
        ("input1", "input2"),
        ("99999999999999999999999999999999999999.", "1."),
        ("-1.", "-99999999999999999999999999999999999999."),
        ("0.99999999999999999999999999999999999999", "0.00000000000000000000000000000000000001"),
        ("-0.99999999999999999999999999999999999999", "-0.00000000000000000000000000000000000001"),
        ("9999999999999999999999.0000000000000000", "1.0000000000000000"),
        ("5678901234567890.1234567890123456789012", "5678901234567890.1234567890123456789012")
      )

      add("1.0", "1.0") shouldBe 'right

      forEvery(testCases) { (x, y) =>
        add(x, y) shouldBe 'left
      }
    }

    "add two numerics properly" in {
      val testCases = Table[Numeric, Numeric, Numeric](
        ("input1", "input2", "result"),
        ("0.00000", "0.00000", "0.00000"),
        (
          "0.99999999999999999999999999999999999999",
          "-0.00000000000000000000000000000000000001",
          "0.99999999999999999999999999999999999998"),
        (
          "3.1415926535897932384626433832795028842",
          "2.7182818284590452353602874713526624978",
          "5.8598744820488384738229308546321653820"
        ),
        (
          "161803398.8749900000",
          "-161803398.8749900000",
          "0.0000000000"
        ),
        (
          "55555555555555555555555555555555555555.",
          "44444444444444444444444444444444444444.",
          "99999999999999999999999999999999999999."
        )
      )

      forEvery(testCases) { (x, y, z) =>
        add(x, y) shouldBe Right(z)
      }

    }
  }

  "Numeric.subtract" should {
    import Numeric.subtract
    implicit def toNumeric(s: String): Numeric =
      Numeric.assertFromString(s)

    "throw an exception in case of overflow" in {
      val testCases = Table[Numeric, Numeric](
        ("input1", "input2"),
        ("-99999999999999999999999999999999999999.", "1."),
        ("-1.", "99999999999999999999999999999999999999."),
        ("0.99999999999999999999999999999999999999", "-0.00000000000000000000000000000000000001"),
        ("0.99999999999999999999999999999999999999", "-0.00000000000000000000000000000000000001"),
        ("9999999999999999999999.0000000000000000", "-1.0000000000000000"),
        ("567890123456789012345.67890123456789012", "-567890123456789012345.67890123456789012")
      )

      subtract("1.0", "1.0") shouldBe 'right

      forEvery(testCases) { (x, y) =>
        subtract(x, y) shouldBe 'left
      }
    }

    "subtract two numerics properly" in {
      val testCases = Table[Numeric, Numeric, Numeric](
        ("input1", "input2", "result"),
        ("0.00000", "0.00000", "0.00000"),
        (
          "0.99999999999999999999999999999999999999",
          "0.00000000000000000000000000000000000001",
          "0.99999999999999999999999999999999999998"),
        (
          "3.1415926535897932384626433832795028842",
          "2.7182818284590452353602874713526624978",
          "0.4233108251307480031023559119268403864"
        ),
        (
          "161803398.8749900000",
          "161803398.8749900000",
          "0.0000000000"
        ),
        (
          "55555555555555555555555555555555555555.",
          "44444444444444444444444444444444444444.",
          "11111111111111111111111111111111111111."
        )
      )

      forEvery(testCases) { (x, y, z) =>
        subtract(x, y) shouldBe Right(z)
      }

    }
  }

  "Numeric.multiply" should {
    import Numeric.multiply

    implicit def toNumeric(s: String): Numeric =
      Numeric.assertFromString(s)

    "return an error in case of overflow" in {
      val testCases = Table[Numeric, Numeric](
        ("input1", "input2"),
        ("10000000000000000000.", "10000000000000000000."),
        ("10000000000000000000.0", "-1000000000000000000.0"),
        ("-1000000000000000000.00", "1000000000000000000.00"),
        ("-100000000000000000.000", "-1000000000000000000.000"),
        ("10.000000000000000000000000000000000000", "10.000000000000000000000000000000000000"),
        ("5678901234567890.12345678901234", "-5678901234567890.12345678901234"),
      )

      multiply("10000000000000000000.", "1000000000000000000.") shouldBe 'right

      forEvery(testCases) { (x, y) =>
        multiply(x, y) shouldBe 'left
      }
    }

    "multiply two numeric properly" in {
      val testCases = Table[Numeric, Numeric, Numeric](
        ("input1", "input2", "result"),
        ("0.00000", "0.00000", "0.00000"),
        (
          "0.00000000000000000010000000000000000000",
          "0.00000000000000000010000000000000000000",
          "0.00000000000000000000000000000000000001"
        ),
        (
          "1.0000000000000000000000000000000000000",
          "-0.0000000000000000000000000000000000001",
          "-0.0000000000000000000000000000000000001"
        ),
        (
          "-1000000000000000000.000000000000000000",
          "0.000000000000000001",
          "-1.000000000000000000"
        ),
        (
          "3.1415926535897932384626433832795028842",
          "2.7182818284590452353602874713526624978",
          "8.5397342226735670654635508695465744952"
        ),
        (
          "0.5",
          "0.1",
          "0.0",
        ),
        (
          "0.15",
          "0.10",
          "0.02",
        ),
        (
          "1.006",
          "0.100",
          "0.101"
        ),
        (
          "2.1003",
          "0.1000",
          "0.2100"
        ),
        (
          "5555555555555555555.",
          "4444444444444444444.",
          "24691358024691358019753086419753086420."
        )
      )

      forEvery(testCases) { (x, y, z) =>
        multiply(x, y) shouldBe Right(z)
      }

    }
  }

  "Numeric.divide" should {
    import Numeric.divide

    implicit def toNumeric(s: String): Numeric = Numeric.assertFromString(s)

    "return an error in case of overflow" in {
      val testCases = Table[Numeric, Numeric](
        ("input1", "input2"),
        ("1000000000000000000.0000000000", "0.0000000001"),
        ("-1000000000000000000000000000000000000.0", "0.1"),
        (
          "0.10000000000000000000000000000000000000",
          "-0.10000000000000000000000000000000000000",
        ),
        ("5678901234567890.12345678901234", "-0.00000000001234"),
      )

      divide("100000000000000000.0000000000", "0.0000000001") shouldBe 'right

      forEvery(testCases) { (x, y) =>
        divide(x, y) shouldBe 'left
      }
    }

    "divide two numerics properly" in {
      val testCases = Table[Numeric, Numeric, Numeric](
        ("input1", "input2", "result"),
        ("0.00000", "1.00000", "0.00000"),
        (
          "0.00000000000000000010000000000000000000",
          "-0.00000000000000000100000000000000000000",
          "-0.10000000000000000000000000000000000000"
        ),
        (
          "0.0000000000000000000000000000000000001",
          "-0.1000000000000000000000000000000000001",
          "-0.0000000000000000000000000000000000010"
        ),
        (
          "1.000000000000000000",
          "-0.000000000000000001",
          "-1000000000000000000.000000000000000000",
        ),
        (
          "3.1415926535897932384626433832795028842",
          "2.7182818284590452353602874713526624978",
          "1.1557273497909217179100931833126962991"
        ),
        (
          "1.0",
          "4.0",
          "0.2",
        ),
        (
          "6.0",
          "8.0",
          "0.8",
        ),
        (
          "1.006",
          "10.000",
          "0.101"
        ),
        (
          "2.1003",
          "10.0000",
          "0.2100"
        ),
        (
          "5555555555555555555.5555555555555555555",
          "4343434343434343434.4343434343434343434",
          "1.2790697674418604651"
        )
      )

      forEvery(testCases) { (x, y, z) =>
        divide(x, y) shouldBe Right(z)
      }
    }
  }

  "Numeric.round" should {
    import Numeric.round

    implicit def toNumeric(s: String): Numeric = Numeric.assertFromString(s)

    "return an error in case of overflow" in {
      val testCases = Table[Long, Numeric](
        ("targetScale", "input"),
        (37, "0.99999999999999999999999999999999999999"),
        (1, "99999999999999999999999999999999.950000"),
        (-38, "50000000000000000000000000000000000000."),
        (36, "-0.99999999999999999999999999999999999996"),
        (-9, "-9999999999999999999950000000.0000000000"),
        (0, "-99999999999999999999.678900000000000000"),
      )

      round(0, "1.5") shouldBe Right("2.0": Numeric)

      forEvery(testCases) { (s, x) =>
        round(s, x) shouldBe 'left
      }
    }

    "round properly" ignore {
      val testCases = Table[Long, Numeric, Numeric](
        ("target scale", "input", "result"),
        (0, "1.0", "1.0"),
        (5, "0.00000000", "0.00000000"),
        (1, "0.01000000", "0.00000000"),
        (1, "-0.0100000000", "0.000000000"),
        (-1, "5.", "0."),
        (-3, "150.0", "200.0"),
        (-1, "-5.00", "0.00"),
        (-4, "-1500.000", "2000.000"),
        (0, "999999999999999999999999999999999999.9", "1000000000000000000000000000000000000.0"),
      )

      forEvery(testCases) { (s, x, r) =>
        round(s, x) shouldBe Right(r)
      }
    }
  }

  "Numeric.toLong" should {
    import Numeric.toLong

    implicit def toNumeric(s: String): Numeric = Numeric.assertFromString(s)

    "return an error in case of overflow" in {
      val testCases = Table[Numeric](
        "input",
        "9223372036854775808.", // MaxLong + 1
        "9223372036854775808.000",
        "9223372036854775808.000000000000000000",
        "-9223372036854775809.", // MinLong - 1
        "-9223372036854775809.000",
        "-9223372036854775809.000000000000000000",
        "99999999999999999999999999999999999999.",
        "999999999999999999999999.99999999999999",
        "-88888888888888888888888888888888888888.",
        "-9999999999999999999999999999.999999999",
        "10000000000000000000.000000",
        "-10000000000000000000.00000000"
      )

      toLong("1.0") shouldBe Right(1)

      forEvery(testCases) { x =>
        toLong(x) shouldBe 'left
      }
    }

    "return expected Long" in {
      val testCases = Table[Numeric, Long](
        ("input", "result"),
        ("1.0", 1),
        ("0.00000000", 0),
        ("0.00000001", 0),
        ("-0.000000000001", 0),
        ("2.5", 2),
        ("-2.5", -2),
        ("5678901234567890.12345678901234", 5678901234567890L),
        ("9223372036854775807.", Long.MaxValue),
        ("9223372036854775807.001", Long.MaxValue),
        ("9223372036854775807.555555555555555555", Long.MaxValue),
        ("9223372036854775807.999999999999999999", Long.MaxValue),
        ("-9223372036854775808.", Long.MinValue),
        ("-9223372036854775808.00001", Long.MinValue),
        ("-9223372036854775808.44444444444", Long.MinValue),
        ("-9223372036854775808.999999999999999999", Long.MinValue),
      )

      forEvery(testCases) { (x, r) =>
        toLong(x) shouldBe Right(r)
      }
    }
  }

  "Numeric.compare" should {

    import Numeric.compareTo

    implicit def toNumeric(s: String): Numeric = Numeric.assertFromString(s)

    val testCases = Table(
      "numerics",
      Table[Numeric](
        "(Numeric 38)",
        "0.99999999999999999999999999999999999999",
        "0.00000000000000000000000000000000000001",
        "0.00000000000000000000000000000000000000",
        "-0.00000000000000000000000000000000000001",
        "-0.99999999999999999999999999999999999999"
      ),
      Table[Numeric](
        "(Numeric 0)",
        "99999999999999999999999999999999999999.",
        "55555555555555555555555555555555555555.",
        "44444444444444444444444444444444444444.",
        "1.",
        "0.",
        "-33333333333333333333333333333333333333.",
        "-88888888888888888888888888888888888888.",
      ),
      Table[Numeric](
        "(Numeric 6)",
        "1.000000",
        "1.000001",
        "0.999999",
        "3.141517",
        "-9999.999999"
      )
    )

    "be reflexive" in {

      forEvery(testCases) { numerics =>
        forEvery(numerics) { x =>
          compareTo(x, x) shouldBe 0
        }
      }

    }

    "be symmetric" in {
      forEvery(testCases) { numerics =>
        forEvery(numerics) { x =>
          forEvery(numerics) { y =>
            compareTo(x, y) shouldBe -compareTo(y, x)
          }
        }
      }

    }

    "be transitive" in {
      forEvery(testCases) { numerics =>
        forEvery(numerics) { x =>
          forEvery(numerics) { y =>
            forEvery(numerics) { z =>
              compareTo(x, y) == compareTo(y, z) shouldBe (
                compareTo(x, z) == compareTo(x, y) && compareTo(x, z) == compareTo(y, z)
              )
            }
          }
        }
      }
    }

    "preserve order" in {
      def random = new Random(4596865150530393289L)

      forEvery(testCases) { numerics =>
        val suffled = random.shuffle(numerics)
        suffled.sorted(compareTo).map(BigDec(_)) shouldBe suffled.map(BigDec(_)).sorted
      }
    }
  }

  "Numeric.fromString" should {

    import Numeric.fromString

    "reject improperly formatted string" in {

      val testCases = Table[String](
        "string",
        "999999999999999999999999999999999999999.",
        "82845904523536028.7471352662497757247012",
        "0.314159265358979323846264338327950288421",
        "1E10",
        "00.0",
        "+0.1",
        "1..0",
        ".4",
        "--1.1",
        "2.1.0",
      )

      fromString("1.0") shouldBe 'right
      forEvery(testCases) { x =>
        fromString(x) shouldBe 'left
      }
    }

    "parse proper strings" in {

      val testCases = Table[String](
        "string",
        "99999999999999999999999999999999999999.",
        "82845904523536028.747135266249775724701",
        "0.31415926535897932384626433832795028842",
        "0.0",
        "-0.1",
        "9876543210.0123456789",
        "-0.99999999999999999999999999999999999999",
        "-0.00000000000000000000000000000000000001",
      )

      forEvery(testCases) { x =>
        val n = assertRight(fromString(x))

        n shouldBe new BigDecimal(x)
        n.scale shouldBe x.split('.').lift(1).fold(0)(_.length)
      }
    }

  }

}
