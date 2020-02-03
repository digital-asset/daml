// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import java.util

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SError.{DamlEArithmeticError, SError, SErrorCrash}
import com.digitalasset.daml.lf.speedy.SResult.{SResultContinue, SResultError}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.value.Value.{ValueGenMap, ValueInt64, ValueText}
import org.scalactic.Equality
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.immutable.HashMap
import scala.language.implicitConversions
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SBuiltinTest extends FreeSpec with Matchers with TableDrivenPropertyChecks {

  import SBuiltinTest._

  private implicit def toScale(i: Int): Numeric.Scale = Numeric.Scale.assertFromInt(i)

  private def n(scale: Int, x: BigDecimal): Numeric = Numeric.assertFromBigDecimal(scale, x)
  private def n(scale: Int, str: String): Numeric = n(scale, BigDecimal(str))
  private def s(scale: Int, x: BigDecimal): String = Numeric.toString(n(scale, x))
  private def s(scale: Int, str: String): String = s(scale, BigDecimal(str))

  private def tenPowerOf(i: Int, scale: Int = 10) =
    if (i == 0)
      "1." + "0" * scale
    else if (i > 0)
      "1" + "0" * i + "." + "0" * scale
    else
      "0." + "0" * (-i - 1) + "1" + "0" * (scale + i)

  "Integer operations" - {

    val MaxInt64 = Long.MaxValue
    val MinInt64 = Long.MinValue
    val aBigOddInt64: Long = 0X67890ABCEDF12345L

    val smallInt64s = Table[Long]("small integer values", 167, 11, 2, 1, 0, -1, -2, -11, -167)

    "ADD_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"ADD_INT64 $MaxInt64 -1") shouldEqual Right(SInt64(MaxInt64 - 1))
        eval(e"ADD_INT64 $MaxInt64 1") shouldBe 'left
        eval(e"ADD_INT64 $MinInt64 1") shouldEqual Right(SInt64(MinInt64 + 1))
        eval(e"ADD_INT64 $MinInt64 -1") shouldBe 'left
        eval(e"ADD_INT64 $aBigOddInt64 $aBigOddInt64") shouldBe
          Left(DamlEArithmeticError(s"Int64 overflow when adding $aBigOddInt64 to $aBigOddInt64."))
      }
    }

    "SUB_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"SUB_INT64 $MaxInt64 1") shouldEqual Right(SInt64(MaxInt64 - 1))
        eval(e"SUB_INT64 $MaxInt64 -1") shouldBe 'left
        eval(e"SUB_INT64 $MinInt64 -1") shouldEqual Right(SInt64(MinInt64 + 1))
        eval(e"SUB_INT64 $MinInt64 1") shouldBe 'left
        eval(e"SUB_INT64 -$aBigOddInt64 $aBigOddInt64") shouldEqual Left(
          DamlEArithmeticError(
            s"Int64 overflow when subtracting $aBigOddInt64 from -$aBigOddInt64.",
          ),
        )
      }
    }

    "MUL_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"MUL_INT64 ${1L << 31} ${1L << 31}") shouldEqual Right(SInt64(1L << 62))
        eval(e"MUL_INT64 ${1L << 32} ${1L << 31}") shouldBe 'left
        eval(e"MUL_INT64 ${1L << 32} -${1L << 31}") shouldEqual Right(SInt64(1L << 63))
        eval(e"MUL_INT64 ${1L << 32} -${1L << 32}") shouldBe 'left
        eval(e"MUL_INT64 ${1L << 32} -${1L << 32}") shouldBe 'left
        eval(e"MUL_INT64 $aBigOddInt64 42") shouldEqual
          Left(DamlEArithmeticError(s"Int64 overflow when multiplying $aBigOddInt64 by 42."))
      }
    }

    "DIV_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"DIV_INT64 $MaxInt64 -1") shouldEqual Right(SInt64(-MaxInt64))
        eval(e"DIV_INT64 $MinInt64 -1") shouldEqual
          Left(DamlEArithmeticError(s"Int64 overflow when dividing $MinInt64 by -1."))
      }

      "throws an exception when dividing by 0" in {
        eval(e"DIV_INT64 1 $MaxInt64") shouldEqual Right(SInt64(0))
        eval(e"DIV_INT64 1 0") shouldBe 'left
        eval(e"DIV_INT64 $aBigOddInt64 0") shouldEqual
          Left(DamlEArithmeticError(s"Attempt to divide $aBigOddInt64 by 0."))
      }
    }

    "EXP_INT64" - {

      "throws an exception if the exponent is negative" in {
        eval(e"EXP_INT64 1 0") shouldEqual Right(SInt64(1))
        eval(e"EXP_INT64 1 -1") shouldBe 'left
        eval(e"EXP_INT64 0 -1") shouldBe 'left
        eval(e"EXP_INT64 10 -1") shouldBe 'left
        eval(e"EXP_INT64 10 -20") shouldBe 'left
        eval(e"EXP_INT64 $aBigOddInt64 -42") shouldEqual Left(
          DamlEArithmeticError(s"Attempt to raise $aBigOddInt64 to the negative exponent -42."),
        )
      }

      "throws an exception if it overflows" in {
        eval(e"EXP_INT64 ${1L << 6} 9") shouldEqual Right(SInt64(1L << 54))
        eval(e"EXP_INT64 ${1L << 7} 9") shouldBe 'left
        eval(e"EXP_INT64 ${-(1L << 7)} 9") shouldEqual Right(SInt64(1L << 63))
        eval(e"EXP_INT64 ${-(1L << 7)} 10") shouldBe 'left
        eval(e"EXP_INT64 3 $aBigOddInt64") shouldEqual Left(
          DamlEArithmeticError(s"Int64 overflow when raising 3 to the exponent $aBigOddInt64."),
        )
      }

      "accepts huge exponents for bases -1, 0 and, 1" in {
        eval(e"EXP_INT64 2 $aBigOddInt64") shouldBe 'left
        eval(e"EXP_INT64 -1 $aBigOddInt64") shouldEqual Right(SInt64(-1))
        eval(e"EXP_INT64 0 $aBigOddInt64") shouldEqual Right(SInt64(0))
        eval(e"EXP_INT64 1 $aBigOddInt64") shouldEqual Right(SInt64(1))
      }

      "returns the proper result" in {

        val testCases = Table[Long, Int](
          ("base", "exponent"),
          (1, 0),
          (-1, 0),
          (0, 0),
          (1, 1),
          (-1, 1),
          (-1, 2),
          (1, 2),
          (1048361, 0),
          (-349241, 0),
          (780996803, 1),
          (-163783859, 1),
          (293, 2),
          (-283, 2),
          (1669, 3),
          (-1913, 3),
          (-2128527769, 2),
          (-883061, 3),
          (-13, 7),
          (-5, 13),
          (-2, 63),
          (2, 62),
          (5, 11),
          (17, 11),
          (19051, 3),
          (1228961011, 2),
        )

        forEvery(testCases) { (base: Long, exponent: Int) =>
          val result = BigInt(base).pow(exponent)
          assert(result == result.longValue())
          eval(e"EXP_INT64 $base $exponent") shouldEqual Right(SInt64(result.longValue()))
        }
      }
    }

    "Integer binary operations computes proper results" in {
      // EXP_INT64 is tested independently

      val testCases = Table[String, (Long, Long) => Option[SValue]](
        ("builtin", "reference"),
        ("ADD_INT64", (a, b) => Some(SInt64(a + b))),
        ("SUB_INT64", (a, b) => Some(SInt64(a - b))),
        ("MUL_INT64", (a, b) => Some(SInt64(a * b))),
        ("DIV_INT64", (a, b) => if (b == 0) None else Some(SInt64(a / b))),
        ("MOD_INT64", (a, b) => if (b == 0) None else Some(SInt64(a % b))),
        ("LESS_EQ_INT64", (a, b) => Some(SBool(a <= b))),
        ("GREATER_EQ_INT64", (a, b) => Some(SBool(a >= b))),
        ("LESS_INT64", (a, b) => Some(SBool(a < b))),
        ("GREATER_INT64", (a, b) => Some(SBool(a > b))),
        ("EQUAL @Int64", (a, b) => Some(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(smallInt64s) { a =>
          forEvery(smallInt64s) { b =>
            eval(e"$builtin $a $b").right.toOption shouldEqual ref(a, b)
          }
        }
      }
    }

    "TO_TEXT_INT64" - {
      "return proper results" in {
        forEvery(smallInt64s) { a =>
          eval(e"TO_TEXT_INT64 $a") shouldEqual Right(SText(a.toString))
        }
      }
    }

  }

  "Decimal operations" - {

    val maxDecimal = Decimal.MaxValue

    val minPosDecimal = BigDecimal("0000000000000000000000000000.0000000001")
    val bigBigDecimal = BigDecimal("8765432109876543210987654321.0987654321")
    val zero = BigDecimal("0.0000000000")
    val one = BigDecimal("1.0000000000")
    val two = BigDecimal("2.0000000000")

    val decimals = Table[String](
      "Decimals",
      "161803398.87499",
      "3.1415926536",
      "2.7182818285",
      "0.0000000001",
      "0.0",
      "100.0",
      "-0.0000000001",
      "-2.7182818285",
      "-3.1415926536",
      "-161803398.87499",
    )

    "ADD_NUMERIC" - {
      val builtin = "ADD_NUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @0 ${"9" * 38}. -1.") shouldBe 'right
        eval(e"$builtin @0 ${"9" * 38}. 1.") shouldBe 'left
        eval(e"$builtin @37 9.${"9" * 37} -0.${"0" * 36}1") shouldBe 'right
        eval(e"$builtin @37 9.${"9" * 37} 0.${"0" * 36}1") shouldBe 'left
        eval(e"$builtin @10 ${s(10, bigBigDecimal)} ${s(10, two)}") shouldEqual Right(
          SNumeric(n(10, bigBigDecimal + 2)),
        )
        eval(e"$builtin @10 ${s(10, maxDecimal)} ${s(10, minPosDecimal)}") shouldBe 'left
        eval(e"$builtin @10 ${s(10, maxDecimal.negate)} ${s(10, -minPosDecimal)}") shouldBe 'left
        eval(e"$builtin @10 ${s(10, bigBigDecimal)} ${s(10, bigBigDecimal - 1)}") shouldEqual
          Left(
            DamlEArithmeticError(
              s"(Numeric 10) overflow when adding ${s(10, bigBigDecimal - 1)} to ${s(10, bigBigDecimal)}.",
            ),
          )
      }
    }

    "SUB_NUMERIC" - {
      val builtin = "SUB_NUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @0 -${"9" * 38}. -1.") shouldBe 'right
        eval(e"$builtin @0 -${"9" * 38}. 1.") shouldBe 'left
        eval(e"$builtin @37 -9.${"9" * 37} -0.${"0" * 36}1") shouldBe 'right
        eval(e"$builtin @37 -9.${"9" * 37} 0.${"0" * 36}1") shouldBe 'left
        eval(e"$builtin @10 $bigBigDecimal ${s(10, two)}") shouldEqual Right(
          SNumeric(n(10, bigBigDecimal - 2)),
        )
        eval(e"$builtin @10 ${s(10, maxDecimal)} -$minPosDecimal") shouldBe 'left
        eval(e"$builtin @10 ${maxDecimal.negate} ${s(10, minPosDecimal)}") shouldBe 'left
        eval(e"$builtin @10 ${-bigBigDecimal} ${s(10, bigBigDecimal)}") shouldBe
          Left(
            DamlEArithmeticError(
              s"(Numeric 10) overflow when subtracting ${s(10, bigBigDecimal)} from ${s(10, -bigBigDecimal)}.",
            ),
          )
      }
    }

    "MUL_NUMERIC" - {
      val builtin = "MUL_NUMERIC"
      val underSqrtOfTen = "3.1622776601683793319988935444327185337"
      val overSqrtOfTen = "3.1622776601683793319988935444327185338"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @0 @0 @0 1${"0" * 18}. 1${"0" * 19}.") shouldBe 'right
        eval(e"$builtin @0 @0 @0 1${"0" * 19}.  1${"0" * 19}.") shouldBe 'left
        eval(e"$builtin @37 @37 @37 $underSqrtOfTen $underSqrtOfTen") shouldBe 'right
        eval(e"$builtin @37 @37 @37 $overSqrtOfTen $underSqrtOfTen") shouldBe 'left
        eval(e"$builtin @10 @10 @10 1.1000000000 2.2000000000") shouldEqual Right(
          SNumeric(n(10, 2.42)),
        )
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(13)} ${tenPowerOf(14)}") shouldEqual Right(
          SNumeric(n(10, "1E27")),
        )
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(14)} ${tenPowerOf(14)}") shouldBe 'left
        eval(e"$builtin @10 @10 @10 ${s(10, bigBigDecimal)} ${bigBigDecimal - 1}") shouldEqual Left(
          DamlEArithmeticError(
            s"(Numeric 10) overflow when multiplying ${s(10, bigBigDecimal)} by ${s(10, bigBigDecimal - 1)}.",
          ),
        )
      }
    }

    "DIV_NUMERIC" - {
      val builtin = "DIV_NUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @37 @37 @37 ${s(37, "1E-18")} ${s(37, "-1E-18")}") shouldBe 'right
        eval(e"$builtin @37 @37 @37 ${s(37, "1E-18")} ${s(37, "-1E-19")}") shouldBe 'left
        eval(e"$builtin @1 @1 @1 ${s(1, "1E36")} 0.2") shouldBe 'right
        eval(e"$builtin @1 @1 @1 ${s(1, "1E36")} 0.1") shouldBe 'left
        eval(e"$builtin @10 @10 @10 1.1000000000 2.2000000000") shouldEqual Right(
          SNumeric(n(10, 0.5)),
        )
        eval(e"$builtin @10 @10 @10 ${s(10, bigBigDecimal)} ${tenPowerOf(-10)}") shouldBe 'left
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(17)} ${tenPowerOf(-10)}") shouldEqual Right(
          SNumeric(n(10, "1E27")),
        )
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(18)} ${tenPowerOf(-10)}") shouldEqual Left(
          DamlEArithmeticError(
            s"(Numeric 10) overflow when dividing ${tenPowerOf(18)} by ${tenPowerOf(-10)}.",
          ),
        )
      }

      "throws an exception when divided by 0" in {
        eval(e"$builtin @10 @10 @10 ${s(10, one)} ${tenPowerOf(-10)}") shouldEqual Right(
          SNumeric(n(10, tenPowerOf(10))),
        )
        eval(e"$builtin @10 @10 @10 ${s(10, one)} ${s(10, zero)}") shouldBe 'left
        eval(e"$builtin @10 @10 @10 ${s(10, bigBigDecimal)} ${s(10, zero)}") shouldEqual Left(
          DamlEArithmeticError(s"Attempt to divide ${s(10, bigBigDecimal)} by 0.0000000000."),
        )

      }
    }

    "ROUND_NUMERIC" - {
      "throws an exception if second argument is not between -27 and 10 exclusive" in {
        val testCases = Table("rounding", 100 :: -100 :: List.range(-30, 13): _*)

        forEvery(testCases) { i =>
          eval(e"ROUND_NUMERIC @10 $i ${s(10, bigBigDecimal)}") shouldBe (
            if (-27 <= i && i <= 10) 'right else 'left
          )
        }
      }

      "returns proper result" in {
        val d = "8765432109876543210987654321.0987654321"
        val testCases = Table[Long, String, String](
          ("rounding", "decimal", "result"),
          (-27, d, "9000000000000000000000000000.0000000000"),
          (-1, "45.0", "40.0"),
          (-1, "55.0", "60.0"),
          (10, d, d),
        )

        forEvery(testCases) { (rounding, input, result) =>
          eval(e"ROUND_NUMERIC @10 $rounding ${n(10, input)}") shouldEqual Right(
            SNumeric(n(10, result)),
          )
        }
      }
    }

    "Decimal binary operations compute proper results" in {

      def round(x: BigDecimal) = n(10, x.setScale(10, BigDecimal.RoundingMode.HALF_EVEN))

      val testCases = Table[String, (Numeric, Numeric) => Option[SValue]](
        ("builtin", "reference"),
        ("ADD_NUMERIC @10", (a, b) => Some(SNumeric(n(10, a add b)))),
        ("SUB_NUMERIC @10", (a, b) => Some(SNumeric(n(10, a subtract b)))),
        ("MUL_NUMERIC @10 @10 @10 ", (a, b) => Some(SNumeric(round(a multiply b)))),
        (
          "DIV_NUMERIC @10 @10 @10",
          (a, b) =>
            if (b.signum != 0) Some(SNumeric(round(BigDecimal(a) / BigDecimal(b)))) else None,
        ),
        ("LESS_EQ_NUMERIC @10", (a, b) => Some(SBool(BigDecimal(a) <= BigDecimal(b)))),
        ("GREATER_EQ_NUMERIC @10", (a, b) => Some(SBool(BigDecimal(a) >= BigDecimal(b)))),
        ("LESS_NUMERIC @10", (a, b) => Some(SBool(BigDecimal(a) < BigDecimal(b)))),
        ("GREATER_NUMERIC @10", (a, b) => Some(SBool(BigDecimal(a) > BigDecimal(b)))),
        ("EQUAL @(Numeric 10)", (a, b) => Some(SBool(BigDecimal(a) == BigDecimal(b)))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(decimals) { a =>
          forEvery(decimals) { b =>
            eval(e"$builtin ${s(10, a)} ${s(10, b)}").right.toOption shouldEqual
              ref(n(10, a), n(10, b))
          }
        }
      }
    }

    "TO_TEXT_NUMERIC" - {
      "returns proper results" in {
        forEvery(decimals) { a =>
          eval(e"TO_TEXT_NUMERIC @10 ${s(10, a)}") shouldEqual Right(SText(a))
        }
      }
    }

    "CAST_NUMERIC" - {
      "throws an error in case of overflow" in {
        val testCases = Table[Int, Int, String](
          ("input scale", "output scale", "x"),
          (0, 1, s(0, Numeric.maxValue(0))),
          (0, 37, "10."),
          (20, 30, tenPowerOf(15, 20)),
          (36, 37, s(36, Numeric.minValue(36))),
        )

        forEvery(testCases) { (inputScale, outputScale, x) =>
          eval(e"CAST_NUMERIC @$inputScale @$outputScale $x") shouldBe 'left
        }

      }

      "throws an error in case of precision loss" in {
        val testCases = Table[Int, Int, String](
          ("input scale", "output scale", "x"),
          (1, 0, tenPowerOf(-1, 1)),
          (37, 0, tenPowerOf(-37, 37)),
          (20, 10, "-" + tenPowerOf(-15, 20)),
          (37, 36, tenPowerOf(-37, 37)),
        )

        forEvery(testCases) { (inputScale, outputScale, x) =>
          eval(e"CAST_NUMERIC @$inputScale @$outputScale $x") shouldBe 'left
        }
      }

      "returns proper result" in {
        val testCases = Table[Int, Int, String](
          ("input scale", "output scale", "x"),
          (1, 0, "1.0"),
          (10, 20, tenPowerOf(-5, 10)),
          (20, 10, tenPowerOf(-5, 20)),
          (10, 20, tenPowerOf(10, 10)),
          (20, 10, tenPowerOf(10, 20)),
        )
        forEvery(testCases) { (inputScale, outputScale, x) =>
          eval(e"CAST_NUMERIC @$inputScale @$outputScale $x") shouldEqual Right(
            SNumeric(n(outputScale, x)),
          )
        }
      }
    }

    "SHIFT_NUMERIC" - {

      "returns proper result" in {
        val testCases = Table[Int, Int, String, String](
          ("input scale", "output scale", "input", "output"),
          (0, 1, s(0, Numeric.maxValue(0)), s(1, Numeric.maxValue(1))),
          (0, 37, tenPowerOf(1, 0), tenPowerOf(-36, 37)),
          (20, 30, tenPowerOf(15, 20), tenPowerOf(5, 30)),
          (20, 10, tenPowerOf(15, 20), tenPowerOf(25, 10)),
          (10, 20, tenPowerOf(-5, 10), tenPowerOf(-15, 20)),
          (20, 10, tenPowerOf(-5, 20), tenPowerOf(5, 10)),
          (10, 20, tenPowerOf(10, 10), tenPowerOf(0, 20)),
          (20, 10, tenPowerOf(10, 20), tenPowerOf(20, 10)),
        )
        forEvery(testCases) { (inputScale, outputScale, input, output) =>
          eval(e"SHIFT_NUMERIC @$inputScale @$outputScale $input") shouldEqual Right(
            SNumeric(n(outputScale, output)),
          )
        }
      }
    }

  }

  "Text operations" - {

    val strings =
      Table("string", "", "aa", "ab", "b", "a¶‱😂", "a¶‱😃", "a", "¶", "‱", "😂", "😃", "｡", "1.1")

    "EXPLODE_TEXT" - {
      "works on full unicode" in {
        eval(e"""EXPLODE_TEXT "a¶‱😂"""") shouldEqual Right(
          SList(
            FrontStack(
              SText("a"),
              SText("¶"),
              SText("‱"),
              SText("😂"),
            ),
          ),
        )
      }
    }

    "IMPLODE_TEXT" - {
      "works properly" in {
        eval(e"""IMPLODE_TEXT (Cons @Text ["", "", ""] (Nil @Text)) """) shouldEqual Right(
          SText(""),
        )
        eval(e"""IMPLODE_TEXT (Cons @Text ["a", "¶", "‱", "😂"] (Nil @Text)) """) shouldBe
          Right(SText("a¶‱😂"))
        eval(
          e"""IMPLODE_TEXT Cons @Text ["IMPLODE_TEXT", " ", "works", " ", "properly"] Nil @Text """,
        ) shouldBe
          Right(SText("IMPLODE_TEXT works properly"))
      }
    }

    "SHA256_TEXT" - {
      "work as expected" in {
        val testCases = Table(
          "input" -> "output",
          "" ->
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ->
            "cd372fb85148700fa88095e3492d3f9f5beb43e555e5ff26d95f5a6adc36f8e6",
          """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
            |eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
            |minim veniam, quis nostrud exercitation ullamco laboris nisi ut
            |aliquip ex ea commodo consequat. Duis aute irure dolor in
            |reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
            |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
            |culpa qui officia deserunt mollit anim id est laborum..."""
            .replaceAll("\r", "")
            .stripMargin ->
            "c045064089460b634bb47e71d2457cd0e8dbc1327aaf9439c275c9796c073620",
          "a¶‱😂" ->
            "8f1cc14a85321115abcd2854e34f9ca004f4f199d367c3c9a84a355f287cec2e",
        )
        forEvery(testCases) { (input, output) =>
          eval(e"""SHA256_TEXT "$input"""") shouldEqual Right(SText(output))
        }

      }
    }

    "TEXT_TO_TEXT" - {
      "is idempotent" in {
        forEvery(strings) { s =>
          eval(e""" TO_TEXT_TEXT "$s" """) shouldEqual Right(SText(s))
        }
      }
    }

    "Text binary operations computes proper results" in {

      // a naive Unicode ordering for string
      val unicodeOrdering =
        Ordering.by((s: String) => s.codePoints().toArray.toIterable)

      assert(Ordering.String.gt("｡", "😂"))
      assert(unicodeOrdering.lt("｡", "😂"))

      val testCases = Table[String, (String, String) => Either[SError, SValue]](
        ("builtin", "reference"),
        ("APPEND_TEXT", (a, b) => Right(SText(a + b))),
        ("LESS_EQ_TEXT", (a, b) => Right(SBool(unicodeOrdering.lteq(a, b)))),
        ("GREATER_EQ_TEXT", (a, b) => Right(SBool(unicodeOrdering.gteq(a, b)))),
        ("LESS_TEXT", (a, b) => Right(SBool(unicodeOrdering.lt(a, b)))),
        ("GREATER_TEXT", (a, b) => Right(SBool(unicodeOrdering.gt(a, b)))),
        ("EQUAL @Text", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(strings) { a =>
          forEvery(strings) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldEqual ref(a, b)
          }
        }
      }
    }

    "TEXT_FROM_CODE_POINTS" - {

      "accepts legal code points" in {
        val testCases = Table(
          "codePoints",
          0x000000, // smallest code point
          0x000061,
          0x00007F, // biggest ASCII code point
          0x000080, // smallest non-ASCII code point
          0x0000E9,
          0x008EE0,
          0x00D7FF, // smallest surrogate - 1
          0x00E000, // biggest surrogate + 1
          0x00E568,
          0x00FFFF, // biggest code point of the Basic Multilingual Plan
          0x010000, // smallest code point of the Supplementary Plan 1
          0x01D81A,
          0x01FFFF, // biggest code point of the Supplementary Plan 1
          0x020000, // smallest code point of the Supplementary Plan 2
          0x0245AD,
          0x02FFFF, // biggest code point of the Supplementary Plan 2
          0x030000, // smallest code point of the Supplementary Plan 3
          0x03AE2D,
          0x03FFFF, // biggest code point of the Supplementary Plan 3
          0x040000, // smallest code point of the Supplementary Plans 4-13
          0x09EA6D,
          0x0DFFFF, // biggest code point of the Supplementary Plans 4-13
          0x0E0000, // smallest code point of the Supplementary Plan 14
          0x0EAE2D,
          0x0EFFFF, // biggest code point of the Supplementary Plan 14
          0x0F0000, // smallest code point of the Supplementary Plans 15-16
          0x10AE2D,
          0x10FFFF, // biggest code point of the Supplementary Plans 15-16
        )

        forEvery(testCases)(cp =>
          eval(e"""TEXT_FROM_CODE_POINTS ${intList('\''.toLong, cp.toLong, '\''.toLong)}""") shouldEqual Right(
            SText("'" + new String(Character.toChars(cp)) + "'"),
          ),
        )
      }

      "rejects surrogate code points " in {
        val testCases = Table(
          "surrogate",
          0x00D800, // smallest surrogate
          0x00D924,
          0x00DBFF, // biggest high surrogate
          0x00DC00, // smallest low surrogate
          0x00DDE0,
          0x00DFFF, // biggest surrogate
        )

        forEvery(testCases)(cp =>
          eval(e"""TEXT_FROM_CODE_POINTS ${intList('\''.toLong, cp.toLong, '\''.toLong)}""") shouldBe 'left,
        )
      }

      "rejects to small or to big code points" in {
        val testCases = Table(
          "codepoint",
          Long.MinValue,
          Int.MinValue,
          -0x23456L
            - 2L,
          -1L,
          Character.MAX_CODE_POINT + 1L,
          Character.MAX_CODE_POINT + 2L,
          0x345678L,
          Int.MaxValue,
          Long.MaxValue,
        )

        forEvery(testCases)(cp =>
          eval(e"""TEXT_FROM_CODE_POINTS ${intList('\''.toLong, cp, '\''.toLong)}""") shouldBe 'left,
        )

      }
    }
  }

  "Timestamp operations" - {

    "Timestamp comparison operations computes proper results" in {

      // Here lexicographical order of string representation corresponds to chronological order

      val timeStamp = Table[String](
        "timestamp",
        "1969-07-21T02:56:15.000000Z",
        "1970-01-01T00:00:00.000000Z",
        "2000-12-31T23:00:00.000000Z",
      )

      val testCases = Table[String, (String, String) => Either[SError, SValue]](
        ("builtin", "reference"),
        ("LESS_EQ_TIMESTAMP", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ_TIMESTAMP", (a, b) => Right(SBool(a >= b))),
        ("LESS_TIMESTAMP", (a, b) => Right(SBool(a < b))),
        ("GREATER_TIMESTAMP", (a, b) => Right(SBool(a > b))),
        ("EQUAL @Timestamp", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(timeStamp) { a =>
          forEvery(timeStamp) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldEqual ref(a, b)
          }
        }
      }
    }

    "TEXT_TO_TIMESTAMP" - {
      "works as expected" in {
        val testCases =
          Table[String](
            "timestamp",
            "2000-12-31T22:59:59.900Z",
            "2000-12-31T22:59:59.990Z",
            "2000-12-31T22:59:59.999Z",
            "2000-12-31T22:59:59.999900Z",
            "2000-12-31T22:59:59.999990Z",
            "2000-12-31T22:59:59.999999Z",
            "2000-12-31T23:00:00Z",
          )

        forEvery(testCases) { s =>
          eval(e"TO_TEXT_TEXT $s") shouldEqual Right(SText(s))
        }
      }
    }

  }

  "Date operations" - {

    "Date comparison operations compute proper results" in {

      // Here lexicographical order of string representation corresponds to chronological order

      val dates = Table[String]("dates", "1969-07-21", "1970-01-01", "2001-01-01")

      val testCases = Table[String, (String, String) => Either[SError, SValue]](
        ("builtin", "reference"),
        ("LESS_EQ_DATE", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ_DATE", (a, b) => Right(SBool(a >= b))),
        ("LESS_DATE", (a, b) => Right(SBool(a < b))),
        ("GREATER_DATE", (a, b) => Right(SBool(a > b))),
        ("EQUAL @Date", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(dates) { a =>
          forEvery(dates) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldEqual ref(a, b)
          }
        }
      }
    }

    "TEXT_TO_DATE" - {
      "works as expected" in {
        eval(e"TO_TEXT_TEXT  1879-03-14").left.map(_ => ()) shouldEqual Right(SText("1879-03-14"))
      }
    }
  }

  "ContractId operations" - {

    "EQUAL @ContractId" - {
      "works as expected" in {
        eval(e"EQUAL @(ContractId Mod:T) 'contract1' 'contract1'") shouldEqual Right(SBool(true))
        eval(e"EQUAL @(ContractId Mod:T) 'contract1' 'contract2'") shouldEqual Right(SBool(false))
      }
    }

  }

  "List operations" - {

    val f = """(\ (x:Int64) (y:Int64) ->  ADD_INT64 3 (MUL_INT64 x y))"""

    "FOLDL" - {
      "works as expected" in {
        eval(e"FOLDL @Int64 @Int64 $f 5 ${intList()}") shouldEqual Right(SInt64(5))
        eval(e"FOLDL @Int64 @Int64 $f 5 ${intList(7, 11, 13)}") shouldEqual Right(SInt64(5476))
      }
    }

    "FOLDR" - {
      "works as expected" in {
        eval(e"FOLDR @Int64 @Int64 $f 5 ${intList()}") shouldEqual Right(SInt64(5))
        eval(e"FOLDR @Int64 @Int64 $f 5 ${intList(7, 11, 13)}") shouldEqual Right(SInt64(5260))
      }
    }

    "EQUAL_LIST" - {
      "works as expected" in {
        val sameParity =
          """(\ (x:Int64) (y:Int64) -> EQUAL @Int64 (MOD_INT64 x 2) (MOD_INT64 y 2))"""

        eval(e"EQUAL_LIST @Int64 $sameParity ${intList()} ${intList()}") shouldEqual Right(
          SBool(true),
        )
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1, 2, 3)} ${intList(5, 6, 7)}") shouldEqual Right(
          SBool(true),
        )
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList()} ${intList(1)}") shouldEqual Right(
          SBool(false),
        )
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1)} ${intList(1, 2)}") shouldEqual Right(
          SBool(false),
        )
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1, 2, 3)} ${intList(5, 6, 4)}") shouldEqual Right(
          SBool(false),
        )
      }
    }
  }

  "TextMap operations" - {

    def buildMap[X](typ: String, l: (String, X)*) =
      ("TEXTMAP_EMPTY @Int64" /: l) {
        case (acc, (k, v)) => s"""(TEXTMAP_INSERT @$typ "$k" $v $acc)"""
      }

    "TEXTMAP_EMPTY" - {
      "produces a map" in {
        eval(e"TEXTMAP_EMPTY @Int64") shouldEqual Right(STextMap(HashMap.empty))
      }
    }

    "TEXTMAP_INSERT" - {

      "inserts as expected" in {
        eval(e"${buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)}") shouldBe
          Right(STextMap(HashMap("a" -> SInt64(1), "b" -> SInt64(2), "c" -> SInt64(3))))
      }

      "replaces already present key" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

        eval(e"$map") shouldBe
          Right(STextMap(HashMap("a" -> SInt64(1), "b" -> SInt64(2), "c" -> SInt64(3))))
        eval(e"""TEXTMAP_INSERT @Int64 "b" 4 $map""") shouldEqual Right(
          STextMap(HashMap("a" -> SInt64(1), "b" -> SInt64(4), "c" -> SInt64(3))),
        )
      }
    }

    "TEXTMAP_LOOKUP" - {
      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "finds existing key" in {
        for {
          x <- List("a" -> 1L, "b" -> 2L, "c" -> 3L)
          (k, v) = x
        } eval(e"""TEXTMAP_LOOKUP @Int64 "$k" $map""") shouldEqual Right(SOptional(Some(SInt64(v))))
      }
      "not finds non-existing key" in {
        eval(e"""TEXTMAP_LOOKUP @Int64 "d" $map""") shouldEqual Right(SOptional(None))
      }
    }

    "TEXTMAP_DELETE" - {
      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "deletes existing key" in {
        eval(e"""TEXTMAP_DELETE @Int64 "a" $map""") shouldEqual Right(
          STextMap(HashMap("b" -> SInt64(2), "c" -> SInt64(3))),
        )
        eval(e"""TEXTMAP_DELETE @Int64 "b" $map""") shouldEqual Right(
          STextMap(HashMap("a" -> SInt64(1), "c" -> SInt64(3))),
        )
      }
      "does nothing with non-existing key" in {
        eval(e"""TEXTMAP_DELETE @Int64 "d" $map""") shouldEqual Right(
          STextMap(HashMap("a" -> SInt64(1), "b" -> SInt64(2), "c" -> SInt64(3))),
        )
      }
    }

    "TEXTMAP_TO_LIST" - {

      "returns the keys in order" in {
        val words = List(
          "slant" -> 0,
          "visit" -> 1,
          "ranch" -> 2,
          "first" -> 3,
          "patch" -> 4,
          "trend" -> 5,
          "sweat" -> 6,
          "enter" -> 7,
          "cover" -> 8,
          "favor" -> 9,
        )

        eval(e"TEXTMAP_TO_LIST @Int64 ${buildMap("Int64", words: _*)}") shouldEqual
          Right(
            SList(
              FrontStack(
                mapEntry("cover", SInt64(8)),
                mapEntry("enter", SInt64(7)),
                mapEntry("favor", SInt64(9)),
                mapEntry("first", SInt64(3)),
                mapEntry("patch", SInt64(4)),
                mapEntry("ranch", SInt64(2)),
                mapEntry("slant", SInt64(0)),
                mapEntry("sweat", SInt64(6)),
                mapEntry("trend", SInt64(5)),
                mapEntry("visit", SInt64(1)),
              ),
            ),
          )
      }
    }

    "TEXTMAP_SIZE" - {
      "returns 0 for empty Map" in {
        eval(e"TEXTMAP_SIZE @Int64 (TEXTMAP_EMPTY @Int64)") shouldEqual Right(SInt64(0))
      }

      "returns the expected size for non-empty Map" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)
        eval(e"TEXTMAP_SIZE @Int64 $map") shouldEqual Right(SInt64(3))
      }
    }

  }

  // FIXME: https://github.com/digital-asset/daml/pull/3260
  // test output order of GENMAP_KEYS and GENMAP_VALUES
  "GenMap operations" - {

    //    implicit def valueToKey(v: SValue): SGenMap.Key = SGenMap.Key(v)

    def buildMap[X](typ: String, l: (String, X)*) =
      ("GENMAP_EMPTY @Text @Int64" /: l) {
        case (acc, (k, v)) => s"""(GENMAP_INSERT @Text @$typ "$k" $v $acc)"""
      }

    "GENMAP_EMPTY" - {
      "produces an empty GenMap" in {
        eval(e"GENMAP_EMPTY @Text @Int64") shouldEqual Right(SGenMap(InsertOrdMap.empty))
      }
    }

    val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

    "GENMAP_INSERT" - {

      "inserts as expected" in {
        val e = e"$map"
        eval(e) shouldEqual Right(
          SValue.fromValue(
            ValueGenMap(
              ImmArray(
                ValueText("a") -> ValueInt64(1),
                ValueText("b") -> ValueInt64(2),
                ValueText("c") -> ValueInt64(3),
              ),
            ),
          ),
        )
      }

      "replaces already present key" in {
        eval(e"""GENMAP_INSERT @Text @Int64 "b" 4 $map""") shouldEqual Right(
          SValue.fromValue(
            ValueGenMap(
              ImmArray(
                ValueText("a") -> ValueInt64(1),
                ValueText("b") -> ValueInt64(4),
                ValueText("c") -> ValueInt64(3),
              ),
            ),
          ),
        )
      }

      "crash on non Hashable key" in {
        val expr =
          e"""GENMAP_INSERT @(Int64 -> Int64) @Int64 (\(x: Int64) -> x) 1 (GENMAP_EMPTY @(Int64 -> Int64) @Int64)"""
        eval(expr) shouldBe Left(SErrorCrash("function are not hashable"))
      }
    }

    "GENMAP_LOOKUP" - {

      val builtin = "GENMAP_LOOKUP"

      "finds existing key" in {
        for {
          x <- List("a" -> 1L, "b" -> 2L, "c" -> 3L)
          (k, v) = x
        } eval(e"""$builtin @Text @Int64 "$k" $map""") shouldEqual Right(SOptional(Some(SInt64(v))))
      }
      "not finds non-existing key" in {
        eval(e"""$builtin @Text @Int64 "d" $map""") shouldEqual Right(SOptional(None))
      }

      "crash on non Hashable key" in {
        val expr =
          e"""$builtin @(Int64 -> Int64) @Int64 (\(x: Int64) -> x) (GENMAP_EMPTY @(Int64 -> Int64) @Int64)"""
        eval(expr) shouldBe Left(SErrorCrash("function are not hashable"))
      }
    }

    "GENMAP_DELETE" - {

      val builtin = "GENMAP_DELETE"

      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "deletes existing key" in {
        eval(e"""$builtin @Text @Int64 "a" $map""") shouldEqual Right(
          SValue.fromValue(
            ValueGenMap(ImmArray(ValueText("b") -> ValueInt64(2), ValueText("c") -> ValueInt64(3))),
          ),
        )
        eval(e"""$builtin @Text @Int64 "b" $map""") shouldEqual Right(
          SValue.fromValue(
            ValueGenMap(ImmArray(ValueText("a") -> ValueInt64(1), ValueText("c") -> ValueInt64(3))),
          ),
        )
      }

      "does nothing with non-existing key" in {
        eval(e"""$builtin @Text @Int64 "d" $map""") shouldEqual Right(
          SValue.fromValue(
            ValueGenMap(
              ImmArray(
                ValueText("a") -> ValueInt64(1),
                ValueText("b") -> ValueInt64(2),
                ValueText("c") -> ValueInt64(3),
              ),
            ),
          ),
        )
      }

      "crash on non Hashable key" in {
        val expr =
          e"""$builtin @(Int64 -> Int64) @Int64 (\(x: Int64) -> x) (GENMAP_EMPTY @(Int64 -> Int64) @Int64)"""
        eval(expr) shouldBe Left(SErrorCrash("function are not hashable"))
      }
    }

    "GENMAP_KEYS" - {

      "returns the keys in order" in {
        val words = List(
          "slant" -> 0,
          "visit" -> 1,
          "ranch" -> 2,
          "first" -> 3,
          "patch" -> 4,
          "trend" -> 5,
          "sweat" -> 6,
          "enter" -> 7,
          "cover" -> 8,
          "favor" -> 9,
        )

        eval(e"GENMAP_KEYS @Text @Int64 ${buildMap("Int64", words: _*)}") shouldEqual
          Right(SList(FrontStack(words.map { case (k, _) => SText(k) })))
      }
    }

    "GENMAP_VALUES" - {

      "returns the values in order" in {
        val words = List(
          "slant" -> 0,
          "visit" -> 1,
          "ranch" -> 2,
          "first" -> 3,
          "patch" -> 4,
          "trend" -> 5,
          "sweat" -> 6,
          "enter" -> 7,
          "cover" -> 8,
          "favor" -> 9,
        )

        eval(e"GENMAP_VALUES @Text @Int64 ${buildMap("Int64", words: _*)}") shouldEqual
          Right(SList(FrontStack(words.map { case (_, v) => SInt64(v.toLong) })))
      }
    }

    "TEXTMAP_SIZE" - {
      "returns 0 for empty Map" in {
        eval(e"GENMAP_SIZE @Text @Int64 (GENMAP_EMPTY @Text @Int64)") shouldEqual Right(SInt64(0))
      }

      "returns the expected size for non-empty Map" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)
        eval(e"GENMAP_SIZE @Int64 $map") shouldEqual Right(SInt64(3))
      }
    }

  }

  "Conversion operations" - {

    def almostZero(scale: Long) = BigDecimal(s"1E-$scale")

    "NUMERIC_TO_INT64" - {
      "throws exception in case of overflow" in {
        eval(e"NUMERIC_TO_INT64 @0 ${s(0, -BigDecimal(2).pow(63) - 1)}") shouldBe 'left
        eval(e"NUMERIC_TO_INT64 @3 ${s(3, -BigDecimal(2).pow(63) - 1 + almostZero(3))}") shouldEqual Right(
          SInt64(Long.MinValue),
        )
        eval(e"NUMERIC_TO_INT64 @7 ${s(7, -BigDecimal(2).pow(63))}") shouldEqual Right(
          SInt64(Long.MinValue),
        )
        eval(e"NUMERIC_TO_INT64 @11 ${s(11, BigDecimal(2).pow(63) - 1)}") shouldEqual Right(
          SInt64(Long.MaxValue),
        )
        eval(e"NUMERIC_TO_INT64 @13 ${s(13, BigDecimal(2).pow(63) - almostZero(13))}") shouldEqual Right(
          SInt64(Long.MaxValue),
        )
        eval(e"NUMERIC_TO_INT64 @17 ${s(17, BigDecimal(2).pow(63))}") shouldBe 'left
        eval(e"NUMERIC_TO_INT64 @13 ${s(13, "1E22")}") shouldBe 'left
      }

      "works as expected" in {
        val testCases = Table[Long, String, Long](
          ("scale", "Decimal", "Int64"),
          (7, s(7, almostZero(7)), 0),
          (2, "0.00", 0),
          (8, "1.00000000", 1),
          (10, "1.0000000001", 1),
          (37, "1." + "9" * 37, 1),
          (20, "123456789.12345678912345678912", 123456789),
        )

        forEvery(testCases) { (scale, decimal, int64) =>
          eval(e"NUMERIC_TO_INT64 @$scale $decimal") shouldEqual Right(SInt64(int64))
          eval(e"NUMERIC_TO_INT64 @$scale -$decimal") shouldEqual Right(SInt64(-int64))
        }
      }
    }

    "INT64_TO_NUMERIC" - {
      "work as expected" in {
        val testCases = Table[Long]("Int64", 167, 11, 2, 1, 0, -1, -2, -13, -113)

        forEvery(testCases) { int64 =>
          eval(e"INT64_TO_NUMERIC @10 $int64") shouldEqual Right(SNumeric(n(10, int64)))
        }
      }
    }

    "UNIX_MICROSECONDS_TO_TIMESTAMP" - {
      "throws an exception in case of overflow" in {
        val testCases = Table[Long, Symbol](
          "Int64" -> "overflows",
          Long.MinValue -> 'left,
          -62135596800000001L -> 'left,
          -62135596800000000L -> 'right,
          0L -> 'right,
          253402300799999999L -> 'right,
          253402300800000000L -> 'left,
          Long.MaxValue -> 'left,
        )

        forEvery(testCases) { (int64, overflows) =>
          eval(e"UNIX_MICROSECONDS_TO_TIMESTAMP $int64") shouldBe overflows
        }
      }
    }

    "TIMESTAMP_TO_UNIX_MICROSECONDS & UNIX_MICROSECONDS_TO_TIMESTAMP" - {
      "works as expected" in {

        val testCases = Table[String, Long](
          "Timestamp" -> "Int64",
          "0001-01-01T00:00:00.000000Z" -> -62135596800000000L,
          "1969-07-21T02:56:15.000000Z" -> -14159025000000L,
          "1970-01-01T00:00:00.000000Z" -> 0,
          "2000-12-31T23:00:00.000000Z" -> 978303600000000L,
          "9999-12-31T23:59:59.999999Z" -> 253402300799999999L,
        )

        forEvery(testCases) { (timestamp, int64) =>
          eval(e"TIMESTAMP_TO_UNIX_MICROSECONDS $timestamp") shouldEqual Right(SInt64(int64))
          eval(e"UNIX_MICROSECONDS_TO_TIMESTAMP $int64") shouldEqual Right(
            STimestamp(Time.Timestamp.assertFromLong(int64)),
          )
          eval(e"EQUAL @Timestamp (UNIX_MICROSECONDS_TO_TIMESTAMP $int64) $timestamp") shouldEqual Right(
            SBool(true),
          )
        }
      }
    }

    "UNIX_DAYS_TO_DATE" - {
      "throws an exception in case of overflow" in {
        val testCases = Table[Long, Symbol](
          "Int64" -> "overflows",
          Long.MinValue -> 'left,
          Int.MinValue.toLong -> 'left,
          -719163L -> 'left,
          -719162L -> 'right,
          0L -> 'right,
          2932896L -> 'right,
          2932897L -> 'left,
          Int.MinValue.toLong -> 'left,
          Long.MaxValue -> 'left,
        )

        forEvery(testCases) { (int64, overflows) =>
          eval(e"UNIX_DAYS_TO_DATE $int64") shouldBe overflows
        }
      }
    }

    "DATE_TO_UNIX_DAYS & UNIX_DAYS_TO_DATE" - {
      "works as expected" in {

        val testCases = Table[String, Long](
          "Date" -> "Int64",
          "1969-07-21" -> -164,
          "1970-01-01" -> 0,
          "2001-01-01" -> 11323,
        )

        forEvery(testCases) { (date, int) =>
          eval(e"DATE_TO_UNIX_DAYS $date") shouldEqual Right(SInt64(int))
          eval(e"UNIX_DAYS_TO_DATE $int") shouldEqual Time.Date
            .asInt(int)
            .map(i => SDate(Time.Date.assertFromDaysSinceEpoch(i)))
          eval(e"EQUAL @Date (UNIX_DAYS_TO_DATE $int) $date") shouldEqual Right(SBool(true))
        }
      }
    }

    "Text Operations" - {
      "TO_QUOTED_TEXT_PARTY single quotes" in {
        eval(e"TO_QUOTED_TEXT_PARTY 'alice'") shouldEqual Right(SText("'alice'"))
      }

      "TO_TEXT_PARTY does not single quote" in {
        eval(e"TO_TEXT_PARTY 'alice'") shouldEqual Right(SText("alice"))
      }

      "FROM_TEXT_PARTY" in {
        eval(e"""FROM_TEXT_PARTY "alice" """) shouldEqual Right(
          SOptional(Some(SParty(Ref.Party.assertFromString("alice")))),
        )
        eval(e"""FROM_TEXT_PARTY "bad%char" """) shouldEqual Right(SOptional(None))
      }

      "FROM_TEXT_INT64" in {
        val positiveTestCases =
          Table(
            "strings",
            "-9223372036854775808",
            "-9223372036854775807",
            "-22",
            "0",
            "42",
            "9223372036854775806",
            "9223372036854775807",
            "+9223372036854775807",
            "01",
            "0" * 20 + "42",
            "-003",
          )
        val negativeTestCases =
          Table(
            "strings",
            "pi",
            "0x11",
            "1.0",
            "2.",
            "1L",
            "+-1",
            "9223372036854775808",
            "9223372036854775809",
            "-9223372036854775809",
            "-9223372036854775810",
            "1" * 20,
          )

        forEvery(positiveTestCases) { s =>
          eval(e"""FROM_TEXT_INT64 "$s"""") shouldEqual Right(SOptional(Some(SInt64(s.toLong))))
        }
        forEvery(negativeTestCases) { s =>
          eval(e"""FROM_TEXT_INT64 "$s"""") shouldEqual Right(SOptional(None))
        }
      }

      "handle ridiculously huge strings" in {

        val testCases = Table(
          "input" -> "output",
          (() => "1" * 10000000) -> None,
          (() => "0" * 10000000 + "1") -> Some(SInt64(1)),
        )
        val builtin = e"""FROM_TEXT_INT64"""

        forEvery(testCases) { (input, output) =>
          eval(Ast.EApp(builtin, Ast.EPrimLit(PLText(input())))) shouldEqual Right(
            SOptional(output),
          )
        }

      }

    }

    "FROM_TEXT_NUMERIC" in {
      val positiveTestCases =
        Table(
          "strings" -> "canonical string",
          ("9" * 28 + "." + "9" * 10) -> ("9" * 28 + "." + "9" * 10),
          ("0" * 20 + "1" * 28) -> ("0" * 20 + "1" * 28),
          "161803398.87499" -> "161803398.87499",
          "3.1415926536" -> "3.1415926536",
          "2.7182818285" -> "2.7182818285",
          "0.0000000001" -> "0.0000000001",
          "0.0005" + "0" * 20 -> "0.0005",
          "+0.0" -> "0.0",
          "0.0" -> "0.0",
          "0" -> "0.0",
          "-0" -> "0.0",
          "42" -> "42.0",
          "-0.0005" + "0" * 20 -> "-0.0005",
          "-0.0000000001" -> "-0.0000000001",
          "-2.7182818285" -> "-2.7182818285",
          "-3.1415926536" -> "-3.1415926536",
          "-161803398.87499" -> "-161803398.87499",
          ("-" + "0" * 20 + "1" * 28) -> ("-" + "1" * 28),
          ("-" + "9" * 28 + "." + "9" * 10) -> ("-" + "9" * 28 + "." + "9" * 10),
        )
      val negativeTestCases =
        Table(
          "strings",
          "pi",
          "0x11",
          "1E10",
          "2.",
          "1L",
          "+-1",
          "1" * 29,
          "-" + "1" * 29,
          "+" + "1" * 29,
          "1" * 29,
          "0." + "0" * 10 + "1",
          "42" + "0" * 24 + "2019",
        )

      forEvery(positiveTestCases) { (input, expected) =>
        val e = e"""FROM_TEXT_NUMERIC @10 "$input""""
        eval(e) shouldEqual Right(SOptional(Some(SNumeric(n(10, expected)))))
      }
      forEvery(negativeTestCases) { input =>
        eval(e"""FROM_TEXT_NUMERIC @10 "$input"""") shouldEqual Right(SOptional(None))
      }
    }

    "handle ridiculously huge strings" ignore {

      val testCases = Table(
        "input" -> "output",
        (() => "1" * 10000000) -> None,
        (() => "1." + "0" * 10000000) -> Some(SNumeric(n(10, 1))),
        (() => "0" * 10000000 + "1.0") -> Some(SNumeric(n(10, 1))),
        (() => "+0" * 10000000 + "2.0") -> Some(SNumeric(n(10, 2))),
        (() => "-0" * 10000000 + "3.0") -> Some(SNumeric(n(10, -3))),
      )
      val builtin = e"""FROM_TEXT_NUMERIC @10"""

      forEvery(testCases) { (input, output) =>
        eval(Ast.EApp(builtin, Ast.EPrimLit(Ast.PLText(input())))) shouldEqual Right(
          SOptional(output),
        )
      }

    }

  }

  "EQUAL @TypeRep" - {

    val values = Table(
      "values",
      "(type_rep @Mod:T)",
      "(type_rep @Mod:R)",
      "(type_rep @Int64)",
      "(type_rep @(Mod:Tree (List Text)))",
      "(type_rep @((ContractId Mod:T) -> Mod:Color))",
    )

    "is reflexive" in {
      forEvery(values)(v => {
        val e = e"EQUAL @TypeRep $v $v"
        eval(e) shouldEqual Right(SBool(true))
      })
    }

    "works as expected" in {
      forEvery(values)(v1 =>
        forEvery(values)(v2 => eval(e"EQUAL @TypeRep $v1 $v2") shouldEqual Right(SBool(v1 == v2))),
      )
    }
  }

  "Debugging builtins" - {

    "TRACE" - {
      "is idempotent" in {
        val testCases = Table[String, SValue](
          "expression" -> "result",
          "1" -> SInt64(1),
          "1.00" -> SNumeric(n(2, 1)),
          "True" -> SBool(true),
          "()" -> SUnit,
          """ "text" """ -> SText("text"),
          " 'party' " -> SParty(Ref.Party.assertFromString("party")),
          intList(1, 2, 3) -> SList(FrontStack(SInt64(1), SInt64(2), SInt64(3))),
          " UNIX_DAYS_TO_DATE 1 " -> SDate(Time.Date.assertFromDaysSinceEpoch(1)),
          """ TRACE "another message" (ADD_INT64 1 1)""" -> SInt64(2),
        )

        forEvery(testCases) { (exp, result) =>
          eval(e"""TRACE "message" ($exp)""") shouldEqual Right(result)
        }
      }

      "throws an expression if its argument throws one" in {
        eval(e"""TRACE "message" 1""") shouldEqual Right(SInt64(1))
        eval(e"""TRACE "message" (ERROR "error")""") shouldBe 'left
        eval(e"""TRACE "message" (DIV_INT64 1 0)""") shouldBe 'left
      }
    }

  }

  "Error builtins" - {

    "ERROR" - {
      "throws an exception " in {
        eval(e"""ERROR "message" """) shouldBe 'left
      }
    }
  }

}

object SBuiltinTest {

  private def eval(e: Expr): Either[SError, SValue] = {
    val machine = Speedy.Machine.fromExpr(
      expr = e,
      checkSubmitterInMaintainers = true,
      compiledPackages = PureCompiledPackages(Map.empty).right.get,
      scenario = false,
    )
    final case class Goodbye(e: SError) extends RuntimeException("", null, false, false)
    try {
      while (!machine.isFinal) machine.step() match {
        case SResultContinue => ()
        case SResultError(err) => throw Goodbye(err)
        case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
      }

      Right(machine.toSValue)
    } catch { case Goodbye(err) => Left(err) }
  }

  def intList(xs: Long*): String =
    if (xs.isEmpty) "(Nil @Int64)"
    else xs.mkString(s"(Cons @Int64 [", ", ", s"] (Nil @Int64))")

  private val entryFields: Array[Ref.Name] =
    Ref.Name.Array(Ref.Name.assertFromString("key"), Ref.Name.assertFromString("value"))

  private def mapEntry(k: String, v: SValue) = {
    val args = new util.ArrayList[SValue](2)
    args.add(SText(k))
    args.add(v)
    SStruct(entryFields, args)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def resultEq: Equality[Either[SError, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def optionEq: Equality[Option[SValue]] = {
    case (Some(v1: SValue), Some(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (None, None) => true
    case _ => false
  }

}
