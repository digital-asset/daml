// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{Ref, _}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin.{
  SBCacheDisclosedContract,
  SBCheckTemplate,
  SBCheckTemplateKey,
  SBCrash,
  SBuildCachedContract,
}
import com.daml.lf.speedy.SError.{SError, SErrorCrash}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.speedy.Speedy.{CachedContract, OnLedger, SKeyWithMaintainers}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ValueArithmeticError, VersionedContractInstance}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.Inside

import java.util
import scala.language.implicitConversions
import scala.util.{Failure, Try}

class SBuiltinTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks with Inside {

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
    val aBigOddInt64: Long = 0x67890abcedf12345L

    val smallInt64s = Table[Long]("small integer values", 167, 11, 2, 1, 0, -1, -2, -11, -167)

    "ADD_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"ADD_INT64 $MaxInt64 -1") shouldBe Right(SInt64(MaxInt64 - 1))
        eval(e"ADD_INT64 $MaxInt64 1") shouldBe a[Left[_, _]]
        eval(e"ADD_INT64 $MinInt64 1") shouldBe Right(SInt64(MinInt64 + 1))
        eval(e"ADD_INT64 $MinInt64 -1") shouldBe a[Left[_, _]]
        eval(e"ADD_INT64 $aBigOddInt64 $aBigOddInt64") shouldBe a[Left[_, _]]
      }
    }

    "SUB_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"SUB_INT64 $MaxInt64 1") shouldBe Right(SInt64(MaxInt64 - 1))
        eval(e"SUB_INT64 $MaxInt64 -1") shouldBe a[Left[_, _]]
        eval(e"SUB_INT64 $MinInt64 -1") shouldBe Right(SInt64(MinInt64 + 1))
        eval(e"SUB_INT64 $MinInt64 1") shouldBe a[Left[_, _]]
        eval(e"SUB_INT64 -$aBigOddInt64 $aBigOddInt64") shouldBe a[Left[_, _]]
      }
    }

    "MUL_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"MUL_INT64 ${1L << 31} ${1L << 31}") shouldBe Right(SInt64(1L << 62))
        eval(e"MUL_INT64 ${1L << 32} ${1L << 31}") shouldBe a[Left[_, _]]
        eval(e"MUL_INT64 ${1L << 32} -${1L << 31}") shouldBe Right(SInt64(1L << 63))
        eval(e"MUL_INT64 ${1L << 32} -${1L << 32}") shouldBe a[Left[_, _]]
        eval(e"MUL_INT64 ${1L << 32} -${1L << 32}") shouldBe a[Left[_, _]]
        eval(e"MUL_INT64 $aBigOddInt64 42") shouldBe a[Left[_, _]]
      }
    }

    "DIV_INT64" - {
      "is symmetric, i.e. rounds towards 0" in {
        eval(e"DIV_INT64 10 3") shouldBe Right(SInt64(3))
        eval(e"DIV_INT64 10 -3") shouldBe Right(SInt64(-3))
        eval(e"DIV_INT64 -10 3") shouldBe Right(SInt64(-3))
        eval(e"DIV_INT64 -10 -3") shouldBe Right(SInt64(3))
      }

      "throws an exception if it overflows" in {
        eval(e"DIV_INT64 $MaxInt64 -1") shouldBe Right(SInt64(-MaxInt64))
        eval(e"DIV_INT64 $MinInt64 -1") shouldBe a[Left[_, _]]
      }

      "throws an exception when dividing by 0" in {
        eval(e"DIV_INT64 1 $MaxInt64") shouldBe Right(SInt64(0))
        eval(e"DIV_INT64 1 0") shouldBe a[Left[_, _]]
        eval(e"DIV_INT64 $aBigOddInt64 0") shouldBe a[Left[_, _]]
      }
    }

    "MOD_INT64" - {
      "is remainder with respect to DIV_INT64, i.e. b*(a/b) + (a%b) == a" in {
        eval(e"MOD_INT64 10 3") shouldBe Right(SInt64(1))
        eval(e"MOD_INT64 10 -3") shouldBe Right(SInt64(1))
        eval(e"MOD_INT64 -10 3") shouldBe Right(SInt64(-1))
        eval(e"MOD_INT64 -10 -3") shouldBe Right(SInt64(-1))
      }
    }

    "EXP_INT64" - {

      "throws an exception if the exponent is negative" in {
        eval(e"EXP_INT64 1 0") shouldBe Right(SInt64(1))
        eval(e"EXP_INT64 1 -1") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 0 -1") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 10 -1") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 10 -20") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 $aBigOddInt64 -42") shouldBe a[Left[_, _]]
      }

      "throws an exception if it overflows" in {
        eval(e"EXP_INT64 ${1L << 6} 9") shouldBe Right(SInt64(1L << 54))
        eval(e"EXP_INT64 ${1L << 7} 9") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 ${-(1L << 7)} 9") shouldBe Right(SInt64(1L << 63))
        eval(e"EXP_INT64 ${-(1L << 7)} 10") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 3 $aBigOddInt64") shouldBe a[Left[_, _]]
      }

      "accepts huge exponents for bases -1, 0 and, 1" in {
        eval(e"EXP_INT64 2 $aBigOddInt64") shouldBe a[Left[_, _]]
        eval(e"EXP_INT64 -1 $aBigOddInt64") shouldBe Right(SInt64(-1))
        eval(e"EXP_INT64 0 $aBigOddInt64") shouldBe Right(SInt64(0))
        eval(e"EXP_INT64 1 $aBigOddInt64") shouldBe Right(SInt64(1))
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
          assert(result == result.longValue)
          eval(e"EXP_INT64 $base $exponent") shouldBe Right(SInt64(result.longValue))
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
        ("LESS_EQ @Int64", (a, b) => Some(SBool(a <= b))),
        ("GREATER_EQ @Int64", (a, b) => Some(SBool(a >= b))),
        ("LESS @Int64", (a, b) => Some(SBool(a < b))),
        ("GREATER @Int64", (a, b) => Some(SBool(a > b))),
        ("EQUAL @Int64", (a, b) => Some(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(smallInt64s) { a =>
          forEvery(smallInt64s) { b =>
            eval(e"$builtin $a $b").toOption shouldBe ref(a, b)
          }
        }
      }
    }

    "INT64_TO_TEXT" - {
      "return proper results" in {
        forEvery(smallInt64s) { a =>
          eval(e"INT64_TO_TEXT $a") shouldBe Right(SText(a.toString))
        }
      }
    }

  }

  "Numeric operations" - {

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
        eval(e"$builtin @0 ${"9" * 38}. -1.") shouldBe a[Right[_, _]]
        eval(e"$builtin @0 ${"9" * 38}. 1.") shouldBe a[Left[_, _]]
        eval(e"$builtin @37 9.${"9" * 37} -0.${"0" * 36}1") shouldBe a[Right[_, _]]
        eval(e"$builtin @37 9.${"9" * 37} 0.${"0" * 36}1") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${s(10, bigBigDecimal)} ${s(10, two)}") shouldBe Right(
          SNumeric(n(10, bigBigDecimal + 2))
        )
        eval(e"$builtin @10 ${s(10, maxDecimal)} ${s(10, minPosDecimal)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${s(10, maxDecimal.negate)} ${s(10, -minPosDecimal)}") shouldBe a[
          Left[_, _]
        ]
        eval(e"$builtin @10 ${s(10, bigBigDecimal)} ${s(10, bigBigDecimal - 1)}") shouldBe a[
          Left[_, _]
        ]
      }
    }

    "SUB_NUMERIC" - {
      val builtin = "SUB_NUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @0 -${"9" * 38}. -1.") shouldBe a[Right[_, _]]
        eval(e"$builtin @0 -${"9" * 38}. 1.") shouldBe a[Left[_, _]]
        eval(e"$builtin @37 -9.${"9" * 37} -0.${"0" * 36}1") shouldBe a[Right[_, _]]
        eval(e"$builtin @37 -9.${"9" * 37} 0.${"0" * 36}1") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 $bigBigDecimal ${s(10, two)}") shouldBe Right(
          SNumeric(n(10, bigBigDecimal - 2))
        )
        eval(e"$builtin @10 ${s(10, maxDecimal)} -$minPosDecimal") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${maxDecimal.negate} ${s(10, minPosDecimal)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${-bigBigDecimal} ${s(10, bigBigDecimal)}") shouldBe a[Left[_, _]]
      }
    }

    "MUL_NUMERIC" - {
      val builtin = "MUL_NUMERIC"
      val underSqrtOfTen = "3.1622776601683793319988935444327185337"
      val overSqrtOfTen = "3.1622776601683793319988935444327185338"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @0 @0 @0 1${"0" * 18}. 1${"0" * 19}.") shouldBe a[Right[_, _]]
        eval(e"$builtin @0 @0 @0 1${"0" * 19}.  1${"0" * 19}.") shouldBe a[Left[_, _]]
        eval(e"$builtin @37 @37 @37 $underSqrtOfTen $underSqrtOfTen") shouldBe a[Right[_, _]]
        eval(e"$builtin @37 @37 @37 $overSqrtOfTen $underSqrtOfTen") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 1.1000000000 2.2000000000") shouldBe Right(
          SNumeric(n(10, 2.42))
        )
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(13)} ${tenPowerOf(14)}") shouldBe Right(
          SNumeric(n(10, "1E27"))
        )
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(14)} ${tenPowerOf(14)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 ${s(10, bigBigDecimal)} ${bigBigDecimal - 1}") shouldBe a[
          Left[_, _]
        ]
      }
    }

    "DIV_NUMERIC" - {
      val builtin = "DIV_NUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @37 @37 @37 ${s(37, "1E-18")} ${s(37, "-1E-18")}") shouldBe a[Right[_, _]]
        eval(e"$builtin @37 @37 @37 ${s(37, "1E-18")} ${s(37, "-1E-19")}") shouldBe a[Left[_, _]]
        eval(e"$builtin @1 @1 @1 ${s(1, "1E36")} 0.2") shouldBe a[Right[_, _]]
        eval(e"$builtin @1 @1 @1 ${s(1, "1E36")} 0.1") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 1.1000000000 2.2000000000") shouldBe Right(
          SNumeric(n(10, 0.5))
        )
        eval(e"$builtin @10 @10 @10 ${s(10, bigBigDecimal)} ${tenPowerOf(-10)}") shouldBe a[
          Left[_, _]
        ]
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(17)} ${tenPowerOf(-10)}") shouldBe Right(
          SNumeric(n(10, "1E27"))
        )
        eval(e"$builtin @10 @10 @10 ${tenPowerOf(18)} ${tenPowerOf(-10)}") shouldBe a[Left[_, _]]
      }

      "throws an exception when divided by 0" in {
        eval(e"$builtin @10 @10 @10 ${s(10, one)} ${tenPowerOf(-10)}") shouldBe Right(
          SNumeric(n(10, tenPowerOf(10)))
        )
        eval(e"$builtin @10 @10 @10 ${s(10, one)} ${s(10, zero)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 ${s(10, bigBigDecimal)} ${s(10, zero)}") shouldBe a[Left[_, _]]
      }
    }

    "ROUND_NUMERIC" - {
      "throws an exception if second argument is not between -27 and 10 exclusive" in {
        val testCases = Table("rounding", 100 :: -100 :: List.range(-30, 13): _*)

        forEvery(testCases) { i =>
          eval(e"ROUND_NUMERIC @10 $i ${s(10, bigBigDecimal)}") shouldBe (
            if (-27 <= i && i <= 10) Symbol("right") else Symbol("left")
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
          eval(e"ROUND_NUMERIC @10 $rounding ${n(10, input)}") shouldBe Right(
            SNumeric(n(10, result))
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
        ("LESS_EQ @(Numeric 10)", (a, b) => Some(SBool(BigDecimal(a) <= BigDecimal(b)))),
        ("GREATER_EQ @(Numeric 10)", (a, b) => Some(SBool(BigDecimal(a) >= BigDecimal(b)))),
        ("LESS @(Numeric 10)", (a, b) => Some(SBool(BigDecimal(a) < BigDecimal(b)))),
        ("GREATER @(Numeric 10)", (a, b) => Some(SBool(BigDecimal(a) > BigDecimal(b)))),
        ("EQUAL @(Numeric 10)", (a, b) => Some(SBool(BigDecimal(a) == BigDecimal(b)))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(decimals) { a =>
          forEvery(decimals) { b =>
            eval(e"$builtin ${s(10, a)} ${s(10, b)}").toOption shouldBe
              ref(n(10, a), n(10, b))
          }
        }
      }
    }

    "NUMERIC_TO_TEXT" - {
      "returns proper results" in {
        forEvery(decimals) { a =>
          eval(e"NUMERIC_TO_TEXT @10 ${s(10, a)}") shouldBe Right(SText(a))
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
          eval(e"CAST_NUMERIC @$inputScale @$outputScale $x") shouldBe a[Left[_, _]]
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
          eval(e"CAST_NUMERIC @$inputScale @$outputScale $x") shouldBe a[Left[_, _]]
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
          eval(e"CAST_NUMERIC @$inputScale @$outputScale $x") shouldBe Right(
            SNumeric(n(outputScale, x))
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
          eval(e"SHIFT_NUMERIC @$inputScale @$outputScale $input") shouldBe Right(
            SNumeric(n(outputScale, output))
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
        eval(e"""EXPLODE_TEXT "a¶‱😂"""") shouldBe Right(
          SList(
            FrontStack(
              SText("a"),
              SText("¶"),
              SText("‱"),
              SText("😂"),
            )
          )
        )
      }
    }

    "IMPLODE_TEXT" - {
      "works properly" in {
        eval(e"""IMPLODE_TEXT (Cons @Text ["", "", ""] (Nil @Text)) """) shouldBe Right(
          SText("")
        )
        eval(e"""IMPLODE_TEXT (Cons @Text ["a", "¶", "‱", "😂"] (Nil @Text)) """) shouldBe
          Right(SText("a¶‱😂"))
        eval(
          e"""IMPLODE_TEXT Cons @Text ["IMPLODE_TEXT", " ", "works", " ", "properly"] Nil @Text """
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
          eval(e"""SHA256_TEXT "$input"""") shouldBe Right(SText(output))
        }

      }
    }

    "TEXT_TO_TEXT" - {
      "is idempotent" in {
        forEvery(strings) { s =>
          eval(e""" TEXT_TO_TEXT "$s" """) shouldBe Right(SText(s))
        }
      }
    }

    "Text binary operations computes proper results" in {

      val testCases = Table[String, (String, String) => Either[SError, SValue]](
        ("builtin", "reference"),
        ("APPEND_TEXT", (a, b) => Right(SText(a + b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(strings) { a =>
          forEvery(strings) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldBe ref(a, b)
          }
        }
      }
    }

    "CODE_POINTS_TO_TEXT" - {

      "accepts legal code points" in {
        val testCases = Table(
          "codePoints",
          0x000000, // smallest code point
          0x000061,
          0x00007f, // biggest ASCII code point
          0x000080, // smallest non-ASCII code point
          0x0000e9,
          0x008ee0,
          0x00d7ff, // smallest surrogate - 1
          0x00e000, // biggest surrogate + 1
          0x00e568,
          0x00ffff, // biggest code point of the Basic Multilingual Plan
          0x010000, // smallest code point of the Supplementary Plan 1
          0x01d81a,
          0x01ffff, // biggest code point of the Supplementary Plan 1
          0x020000, // smallest code point of the Supplementary Plan 2
          0x0245ad,
          0x02ffff, // biggest code point of the Supplementary Plan 2
          0x030000, // smallest code point of the Supplementary Plan 3
          0x03ae2d,
          0x03ffff, // biggest code point of the Supplementary Plan 3
          0x040000, // smallest code point of the Supplementary Plans 4-13
          0x09ea6d,
          0x0dffff, // biggest code point of the Supplementary Plans 4-13
          0x0e0000, // smallest code point of the Supplementary Plan 14
          0x0eae2d,
          0x0effff, // biggest code point of the Supplementary Plan 14
          0x0f0000, // smallest code point of the Supplementary Plans 15-16
          0x10ae2d,
          0x10ffff, // biggest code point of the Supplementary Plans 15-16
        )

        forEvery(testCases)(cp =>
          eval(
            e"""CODE_POINTS_TO_TEXT ${intList('\''.toLong, cp.toLong, '\''.toLong)}"""
          ) shouldBe Right(
            SText("'" + new String(Character.toChars(cp)) + "'")
          )
        )
      }

      "rejects surrogate code points " in {
        val testCases = Table(
          "surrogate",
          0x00d800, // smallest surrogate
          0x00d924,
          0x00dbff, // biggest high surrogate
          0x00dc00, // smallest low surrogate
          0x00dde0,
          0x00dfff, // biggest surrogate
        )

        forEvery(testCases)(cp =>
          eval(
            e"""CODE_POINTS_TO_TEXT ${intList('\''.toLong, cp.toLong, '\''.toLong)}"""
          ) shouldBe a[Left[_, _]]
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
          eval(e"""CODE_POINTS_TO_TEXT ${intList('\''.toLong, cp, '\''.toLong)}""") shouldBe a[
            Left[_, _]
          ]
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
        ("LESS_EQ @Timestamp", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ @Timestamp", (a, b) => Right(SBool(a >= b))),
        ("LESS @Timestamp", (a, b) => Right(SBool(a < b))),
        ("GREATER @Timestamp", (a, b) => Right(SBool(a > b))),
        ("EQUAL @Timestamp", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(timeStamp) { a =>
          forEvery(timeStamp) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldBe ref(a, b)
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
          eval(e"TEXT_TO_TEXT $s") shouldBe Right(SText(s))
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
        ("LESS_EQ @Date", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ @Date", (a, b) => Right(SBool(a >= b))),
        ("LESS @Date", (a, b) => Right(SBool(a < b))),
        ("GREATER @Date", (a, b) => Right(SBool(a > b))),
        ("EQUAL @Date", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(dates) { a =>
          forEvery(dates) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldBe ref(a, b)
          }
        }
      }
    }

    "DATE_TO_TEXT" - {
      "works as expected" in {
        eval(e"DATE_TO_TEXT  1879-03-14").left.map(_ => ()) shouldBe Right(SText("1879-03-14"))
      }
    }
  }

  "ContractId operations" - {

    "EQUAL @ContractId" - {
      "works as expected" in {
        val cid1 = SContractId(Value.ContractId.V1(Hash.hashPrivateKey("#contract1")))
        val cid2 = SContractId(Value.ContractId.V1(Hash.hashPrivateKey("#contract2")))
        evalApp(e"EQUAL @(ContractId Mod:T)", Array(cid1, cid1)) shouldBe Right(SBool(true))
        evalApp(e"EQUAL @(ContractId Mod:T)", Array(cid1, cid2)) shouldBe Right(SBool(false))
      }
    }

  }

  "List operations" - {

    val f = """(\ (x:Int64) (y:Int64) ->  ADD_INT64 3 (MUL_INT64 x y))"""
    val g = """(\ (x:Int64) -> let z:Int64 = 3 in \ (y:Int64) -> ADD_INT64 z (MUL_INT64 x y))"""

    "FOLDL" - {
      "works as expected" in {
        eval(e"FOLDL @Int64 @Int64 $f 5 ${intList()}") shouldBe Right(SInt64(5))
        eval(e"FOLDL @Int64 @Int64 $f 5 ${intList(7, 11, 13)}") shouldBe Right(SInt64(5476))
      }
    }

    "FOLDR" - {
      "works as expected" in {
        eval(e"FOLDR @Int64 @Int64 $f 5 ${intList()}") shouldBe Right(SInt64(5))
        eval(e"FOLDR @Int64 @Int64 $f 5 ${intList(7, 11, 13)}") shouldBe Right(SInt64(5260))
      }
      "works as expected when step function takes one argument" in {
        eval(e"FOLDR @Int64 @Int64 $g 5 ${intList()}") shouldBe Right(SInt64(5))
        eval(e"FOLDR @Int64 @Int64 $g 5 ${intList(7, 11, 13)}") shouldBe Right(SInt64(5260))
      }
    }

    "EQUAL_LIST" - {
      "works as expected" in {
        val sameParity =
          """(\ (x:Int64) (y:Int64) -> EQUAL @Int64 (MOD_INT64 x 2) (MOD_INT64 y 2))"""

        eval(e"EQUAL_LIST @Int64 $sameParity ${intList()} ${intList()}") shouldBe Right(
          SBool(true)
        )
        eval(
          e"EQUAL_LIST @Int64 $sameParity ${intList(1, 2, 3)} ${intList(5, 6, 7)}"
        ) shouldBe Right(
          SBool(true)
        )
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList()} ${intList(1)}") shouldBe Right(
          SBool(false)
        )
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1)} ${intList(1, 2)}") shouldBe Right(
          SBool(false)
        )
        eval(
          e"EQUAL_LIST @Int64 $sameParity ${intList(1, 2, 3)} ${intList(5, 6, 4)}"
        ) shouldBe Right(
          SBool(false)
        )
      }
    }
  }

  "TextMap operations" - {

    def buildMap[X](typ: String, l: (String, X)*) =
      (l foldLeft "TEXTMAP_EMPTY @Int64") { case (acc, (k, v)) =>
        s"""(TEXTMAP_INSERT @$typ "$k" $v $acc)"""
      }

    "TEXTMAP_EMPTY" - {
      "produces a map" in {
        eval(e"TEXTMAP_EMPTY @Int64") shouldBe Right(SValue.SValue.EmptyTextMap)
      }
    }

    "TEXTMAP_INSERT" - {

      "inserts as expected" in {
        eval(e"${buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)}") shouldBe
          Right(
            SMap(true, SText("a") -> SInt64(1), SText("b") -> SInt64(2), SText("c") -> SInt64(3))
          )
      }

      "replaces already present key" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

        eval(e"$map") shouldBe
          Right(
            SMap(true, SText("a") -> SInt64(1), SText("b") -> SInt64(2), SText("c") -> SInt64(3))
          )
        eval(e"""TEXTMAP_INSERT @Int64 "b" 4 $map""") shouldBe Right(
          SMap(true, SText("a") -> SInt64(1), SText("b") -> SInt64(4), SText("c") -> SInt64(3))
        )
      }
    }

    "TEXTMAP_LOOKUP" - {
      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "finds existing key" in {
        for {
          x <- List("a" -> 1L, "b" -> 2L, "c" -> 3L)
          (k, v) = x
        } eval(e"""TEXTMAP_LOOKUP @Int64 "$k" $map""") shouldBe Right(SOptional(Some(SInt64(v))))
      }
      "not finds non-existing key" in {
        eval(e"""TEXTMAP_LOOKUP @Int64 "d" $map""") shouldBe Right(SOptional(None))
      }
    }

    "TEXTMAP_DELETE" - {
      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "deletes existing key" in {
        eval(e"""TEXTMAP_DELETE @Int64 "a" $map""") shouldBe Right(
          SMap(true, SText("b") -> SInt64(2), SText("c") -> SInt64(3))
        )
        eval(e"""TEXTMAP_DELETE @Int64 "b" $map""") shouldBe Right(
          SMap(true, SText("a") -> SInt64(1), SText("c") -> SInt64(3))
        )
      }
      "does nothing with non-existing key" in {
        eval(e"""TEXTMAP_DELETE @Int64 "d" $map""") shouldBe Right(
          SMap(true, SText("a") -> SInt64(1), SText("b") -> SInt64(2), SText("c") -> SInt64(3))
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

        eval(e"TEXTMAP_TO_LIST @Int64 ${buildMap("Int64", words: _*)}") shouldBe
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
              )
            )
          )
      }
    }

    "TEXTMAP_SIZE" - {
      "returns 0 for empty Map" in {
        eval(e"TEXTMAP_SIZE @Int64 (TEXTMAP_EMPTY @Int64)") shouldBe Right(SInt64(0))
      }

      "returns the expected size for non-empty Map" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)
        eval(e"TEXTMAP_SIZE @Int64 $map") shouldBe Right(SInt64(3))
      }
    }

  }

  // FIXME: https://github.com/digital-asset/daml/pull/3260
  // test output order of GENMAP_KEYS and GENMAP_VALUES
  "GenMap operations" - {

    def buildMap[X](typ: String, l: (String, X)*) =
      (l foldLeft "GENMAP_EMPTY @Text @Int64") { case (acc, (k, v)) =>
        s"""(GENMAP_INSERT @Text @$typ "$k" $v $acc)"""
      }

    val funT = "Int64 -> Int64"
    val eitherT = s"Mod:Either ($funT) Int64"
    val funV = """\(x: Int64) -> x"""
    val leftV = s"""Mod:Either:Left @($funT) @Int64 ($funV)"""
    val rightV = s"Mod:Either:Right @($funT) @Int64 1"
    val emptyMapV = s"GENMAP_EMPTY @($eitherT) @Int64"
    val nonEmptyMapV = s"GENMAP_INSERT @($eitherT) @Int64 ($rightV) 0 ($emptyMapV)"

    eval(e"$nonEmptyMapV") shouldBe a[Right[_, _]]

    "GENMAP_EMPTY" - {
      "produces an empty GenMap" in {
        eval(e"GENMAP_EMPTY @Text @Int64") shouldBe Right(SMap(false))
      }
    }

    val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

    "GENMAP_INSERT" - {

      val builtin = "GENMAP_INSERT"

      "inserts as expected" in {
        val e = e"$map"
        eval(e) shouldBe Right(
          SMap(false, SText("a") -> SInt64(1), SText("b") -> SInt64(2), SText("c") -> SInt64(3))
        )
      }

      "replaces already present key" in {
        eval(e"""$builtin @Text @Int64 "b" 4 $map""") shouldBe Right(
          SMap(false, SText("a") -> SInt64(1), SText("b") -> SInt64(4), SText("c") -> SInt64(3))
        )
      }

      "crash when comparing non comparable keys" in {
        val expr1 = e"""$builtin @($eitherT) @Int64 ($leftV) 0 ($emptyMapV)"""
        val expr2 = e"""$builtin @($eitherT) @Int64 ($leftV) 1 ($nonEmptyMapV)"""
        eval(expr1) shouldBe Left(
          SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        )
        eval(expr2) shouldBe Left(
          SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        )
      }
    }

    "GENMAP_LOOKUP" - {

      val builtin = "GENMAP_LOOKUP"

      "finds existing key" in {
        for {
          x <- List("a" -> 1L, "b" -> 2L, "c" -> 3L)
          (k, v) = x
        } eval(e"""$builtin @Text @Int64 "$k" $map""") shouldBe Right(SOptional(Some(SInt64(v))))
      }
      "not finds non-existing key" in {
        eval(e"""$builtin @Text @Int64 "d" $map""") shouldBe Right(SOptional(None))
      }

      "crash when comparing non comparable keys" in {
        val expr1 = e"""$builtin @($eitherT) @Int64 ($leftV) ($emptyMapV)"""
        val expr2 = e"""$builtin @($eitherT) @Int64 ($leftV) ($nonEmptyMapV)"""
        eval(expr1) shouldBe Left(
          SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        )
        eval(expr2) shouldBe Left(
          SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        )
      }
    }

    "GENMAP_DELETE" - {

      val builtin = "GENMAP_DELETE"

      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "deletes existing key" in {
        eval(e"""$builtin @Text @Int64 "a" $map""") shouldBe Right(
          SMap(false, SText("b") -> SInt64(2), SText("c") -> SInt64(3))
        )
        eval(e"""$builtin @Text @Int64 "b" $map""") shouldBe Right(
          SMap(false, SText("a") -> SInt64(1), SText("c") -> SInt64(3))
        )
      }

      "does nothing with non-existing key" in {
        eval(e"""$builtin @Text @Int64 "d" $map""") shouldBe Right(
          SMap(
            false,
            SText("a") -> SInt64(1),
            SText("b") -> SInt64(2),
            SText("c") -> SInt64(3),
          )
        )
      }

      "crash when comparing non comparable keys" in {
        val expr1 = e"""$builtin @($eitherT) @Int64 ($leftV) ($emptyMapV)"""
        val expr2 = e"""$builtin @($eitherT) @Int64 ($leftV) ($nonEmptyMapV)"""
        eval(expr1) shouldBe Left(
          SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        )
        eval(expr2) shouldBe Left(
          SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        )
      }
    }

    val words = List(
      "slant",
      "visit",
      "ranch",
      "first",
      "patch",
      "trend",
      "sweat",
      "enter",
      "cover",
      "favor",
    ).zipWithIndex

    val sortedWords = words.sortBy(_._1)

    "GENMAP_KEYS" - {

      "returns the keys in order" in {

        eval(e"GENMAP_KEYS @Text @Int64 ${buildMap("Int64", words: _*)}") shouldBe
          Right(SList(sortedWords.map { case (k, _) => SText(k) }.to(FrontStack)))
      }
    }

    "GENMAP_VALUES" - {

      "returns the values in order" in {
        eval(e"GENMAP_VALUES @Text @Int64 ${buildMap("Int64", words: _*)}") shouldBe
          Right(SList(sortedWords.map { case (_, v) => SInt64(v.toLong) }.to(FrontStack)))
      }
    }

    "TEXTMAP_SIZE" - {
      "returns 0 for empty Map" in {
        eval(e"GENMAP_SIZE @Text @Int64 (GENMAP_EMPTY @Text @Int64)") shouldBe Right(SInt64(0))
      }

      "returns the expected size for non-empty Map" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)
        eval(e"GENMAP_SIZE @Int64 $map") shouldBe Right(SInt64(3))
      }
    }

  }

  "Conversion operations" - {

    def almostZero(scale: Long) = BigDecimal(s"1E-$scale")

    "NUMERIC_TO_INT64" - {
      "throws exception in case of overflow" in {
        eval(e"NUMERIC_TO_INT64 @0 ${s(0, -BigDecimal(2).pow(63) - 1)}") shouldBe a[Left[_, _]]
        eval(
          e"NUMERIC_TO_INT64 @3 ${s(3, -BigDecimal(2).pow(63) - 1 + almostZero(3))}"
        ) shouldBe Right(
          SInt64(Long.MinValue)
        )
        eval(e"NUMERIC_TO_INT64 @7 ${s(7, -BigDecimal(2).pow(63))}") shouldBe Right(
          SInt64(Long.MinValue)
        )
        eval(e"NUMERIC_TO_INT64 @11 ${s(11, BigDecimal(2).pow(63) - 1)}") shouldBe Right(
          SInt64(Long.MaxValue)
        )
        eval(
          e"NUMERIC_TO_INT64 @13 ${s(13, BigDecimal(2).pow(63) - almostZero(13))}"
        ) shouldBe Right(
          SInt64(Long.MaxValue)
        )
        eval(e"NUMERIC_TO_INT64 @17 ${s(17, BigDecimal(2).pow(63))}") shouldBe a[Left[_, _]]
        eval(e"NUMERIC_TO_INT64 @13 ${s(13, "1E22")}") shouldBe a[Left[_, _]]
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
          eval(e"NUMERIC_TO_INT64 @$scale $decimal") shouldBe Right(SInt64(int64))
          eval(e"NUMERIC_TO_INT64 @$scale -$decimal") shouldBe Right(SInt64(-int64))
        }
      }
    }

    "INT64_TO_NUMERIC" - {
      "work as expected" in {
        val testCases = Table[Long]("Int64", 167, 11, 2, 1, 0, -1, -2, -13, -113)

        forEvery(testCases) { int64 =>
          eval(e"INT64_TO_NUMERIC @10 $int64") shouldBe Right(SNumeric(n(10, int64)))
        }
      }
    }

    "UNIX_MICROSECONDS_TO_TIMESTAMP" - {
      "throws an exception in case of overflow" in {
        val testCases = Table[Long, Symbol](
          "Int64" -> "overflows",
          Long.MinValue -> Symbol("left"),
          -62135596800000001L -> Symbol("left"),
          -62135596800000000L -> Symbol("right"),
          0L -> Symbol("right"),
          253402300799999999L -> Symbol("right"),
          253402300800000000L -> Symbol("left"),
          Long.MaxValue -> Symbol("left"),
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
          eval(e"TIMESTAMP_TO_UNIX_MICROSECONDS $timestamp") shouldBe Right(SInt64(int64))
          eval(e"UNIX_MICROSECONDS_TO_TIMESTAMP $int64") shouldBe Right(
            STimestamp(Time.Timestamp.assertFromLong(int64))
          )
          eval(
            e"EQUAL @Timestamp (UNIX_MICROSECONDS_TO_TIMESTAMP $int64) $timestamp"
          ) shouldBe Right(
            SBool(true)
          )
        }
      }
    }

    "UNIX_DAYS_TO_DATE" - {
      "throws an exception in case of overflow" in {
        val testCases = Table[Long, Symbol](
          "Int64" -> "overflows",
          Long.MinValue -> Symbol("left"),
          Int.MinValue.toLong -> Symbol("left"),
          -719163L -> Symbol("left"),
          -719162L -> Symbol("right"),
          0L -> Symbol("right"),
          2932896L -> Symbol("right"),
          2932897L -> Symbol("left"),
          Int.MinValue.toLong -> Symbol("left"),
          Long.MaxValue -> Symbol("left"),
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
          eval(e"DATE_TO_UNIX_DAYS $date") shouldBe Right(SInt64(int))
          eval(e"UNIX_DAYS_TO_DATE $int") shouldBe Time.Date
            .asInt(int)
            .map(i => SDate(Time.Date.assertFromDaysSinceEpoch(i)))
          eval(e"EQUAL @Date (UNIX_DAYS_TO_DATE $int) $date") shouldBe Right(SBool(true))
        }
      }
    }

    "Text Operations" - {
      "PARTY_TO_QUOTED_TEXT single quotes" in {
        evalApp(
          e"PARTY_TO_QUOTED_TEXT",
          Array(SParty(Ref.Party.assertFromString("alice"))),
        ) shouldBe Right(SText("'alice'"))
      }

      "PARTY_TO_TEXT does not single quote" in {
        evalApp(
          e"PARTY_TO_TEXT",
          Array(SParty(Ref.Party.assertFromString("alice"))),
        ) shouldBe Right(SText("alice"))
      }

      "TEXT_TO_PARTY" - {
        "should convert correct string" in {
          eval(e"""TEXT_TO_PARTY "alice" """) shouldBe Right(
            SOptional(Some(SParty(Ref.Party.assertFromString("alice"))))
          )
        }
        "should not convert string with incorrect char" in {
          eval(e"""TEXT_TO_PARTY "bad%char" """) shouldBe Right(SOptional(None))
        }

        "should not convert too long string" in {
          val party255 = "p" * 255
          val party256 = party255 + "p"
          eval(e"""TEXT_TO_PARTY "$party255" """) shouldBe Right(
            SOptional(Some(SParty(Ref.Party.assertFromString(party255))))
          )
          eval(e"""TEXT_TO_PARTY "$party256" """) shouldBe Right(SOptional(None))
        }

        "should not convert empty string" in {
          eval(e"""TEXT_TO_PARTY "p" """) shouldBe Right(
            SOptional(Some(SParty(Ref.Party.assertFromString("p"))))
          )
          eval(e"""TEXT_TO_PARTY "" """) shouldBe Right(SOptional(None))
        }

      }

      "TEXT_TO_INT64" in {
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
          eval(e"""TEXT_TO_INT64 "$s"""") shouldBe Right(SOptional(Some(SInt64(s.toLong))))
        }
        forEvery(negativeTestCases) { s =>
          eval(e"""TEXT_TO_INT64 "$s"""") shouldBe Right(SOptional(None))
        }
      }

      "CONTRACT_ID_TO_TEXT" - {
        "returns None on-ledger" in {
          val f = """(\(c:(ContractId Mod:T)) -> CONTRACT_ID_TO_TEXT @Mod:T c)"""
          val cid = Value.ContractId.V1(Hash.hashPrivateKey("abc"))
          evalApp(
            e"$f",
            Array(SContractId(cid)),
            onLedger = true,
          ) shouldBe Right(SOptional(None))
        }
        "returns Some(abc) off-ledger" in {
          val f = """(\(c:(ContractId Mod:T)) -> CONTRACT_ID_TO_TEXT @Mod:T c)"""
          val cid = Value.ContractId.V1(Hash.hashPrivateKey("abc"))
          evalApp(
            e"$f",
            Array(SContractId(cid)),
            onLedger = false,
          ) shouldBe Right(SOptional(Some(SText(cid.coid))))
        }
      }

      "handle ridiculously huge strings" in {

        val testCases = Table(
          "input" -> "output",
          (() => "1" * 10000000) -> None,
          (() => "0" * 10000000 + "1") -> Some(SInt64(1)),
        )
        val builtin = e"""TEXT_TO_INT64"""

        forEvery(testCases) { (input, output) =>
          eval(EApp(builtin, EPrimLit(PLText(input())))) shouldBe Right(
            SOptional(output)
          )
        }

      }

    }

    "TEXT_TO_NUMERIC" in {
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
        val e = e"""TEXT_TO_NUMERIC @10 "$input""""
        eval(e) shouldBe Right(SOptional(Some(SNumeric(n(10, expected)))))
      }
      forEvery(negativeTestCases) { input =>
        eval(e"""TEXT_TO_NUMERIC @10 "$input"""") shouldBe Right(SOptional(None))
      }
    }

    "handle ridiculously huge strings" in {

      val testCases = Table(
        "input" -> "output",
        (() => "1" * 10000000) -> None,
        (() => "1." + "0" * 10000000) -> Some(SNumeric(n(10, 1))),
        (() => "0" * 10000000 + "1.0") -> Some(SNumeric(n(10, 1))),
        (() => "+" + "0" * 10000000 + "2.0") -> Some(SNumeric(n(10, 2))),
        (() => "-" + "0" * 10000000 + "3.0") -> Some(SNumeric(n(10, -3))),
      )
      val builtin = e"""TEXT_TO_NUMERIC @10"""

      forEvery(testCases) { (input, output) =>
        eval(EApp(builtin, EPrimLit(PLText(input())))) shouldBe Right(SOptional(output))
      }
    }

  }

  "ArithmeticBuiltins" - {

    "throw DamlArithmeticException with proper name and argument" in {

      import SBuiltin._
      import Numeric.Scale.{MinValue => MinScale, MaxValue => MaxScale}
      import java.math.BigDecimal

      val TMinScale = STNat(MinScale)

      val MaxNumeric0 = SNumeric(Numeric.maxValue(MinScale))
      val MinNumeric0 = SNumeric(Numeric.minValue(MinScale))
      val TwoNumeric0 = SNumeric(data.assertRight(Numeric.fromLong(MinScale, 2L)))

      val VeryBigBigNumericA =
        SBigNumeric.assertFromBigDecimal(
          BigDecimal.valueOf(8).scaleByPowerOfTen(SBigNumeric.MaxScale - 1)
        )
      val VeryBigBigNumericB =
        SBigNumeric.assertFromBigDecimal(
          BigDecimal.valueOf(7).scaleByPowerOfTen(SBigNumeric.MaxScale - 1)
        )
      val VeryBigNegativeBigNumeric =
        SBigNumeric.assertFromBigDecimal(
          BigDecimal.valueOf(-7).scaleByPowerOfTen(SBigNumeric.MaxScale - 1)
        )

      val ZeroInt64 = SInt64(0L)
      val TwoInt64 = SInt64(2L)
      val MaxInt64 = SInt64(Long.MaxValue)

      val cases = Table[SBuiltinArithmetic, Seq[SValue], String](
        ("builtin", "args", "name"),
        (SBAddInt64, List[SValue](MaxInt64, TwoInt64), "ADD_INT64"),
        (SBSubInt64, List[SValue](SInt64(-2L), MaxInt64), "SUB_INT64"),
        (SBMulInt64, List[SValue](MaxInt64, TwoInt64), "MUL_INT64"),
        (SBDivInt64, List[SValue](MaxInt64, ZeroInt64), "DIV_INT64"),
        (SBModInt64, List[SValue](MaxInt64, ZeroInt64), "MOD_INT64"),
        (SBExpInt64, List[SValue](TwoInt64, MaxInt64), "EXP_INT64"),
        (SBAddNumeric, List[SValue](TMinScale, MaxNumeric0, TwoNumeric0), "ADD_NUMERIC"),
        (SBSubNumeric, List[SValue](TMinScale, MinNumeric0, TwoNumeric0), "SUB_NUMERIC"),
        (
          SBMulNumeric,
          List[SValue](TMinScale, TMinScale, TMinScale, MaxNumeric0, MaxNumeric0),
          "MUL_NUMERIC",
        ),
        (
          SBDivNumeric,
          List[SValue](
            TMinScale,
            TMinScale,
            TMinScale,
            TwoNumeric0,
            SNumeric(Numeric.assertFromString("0.")),
          ),
          "DIV_NUMERIC",
        ),
        (
          SBRoundNumeric,
          List[SValue](STNat(MinScale), SInt64(MaxScale.toLong), MaxNumeric0),
          "ROUND_NUMERIC",
        ),
        (
          SBCastNumeric,
          List[SValue](STNat(MinScale), STNat(MaxScale), MaxNumeric0),
          "CAST_NUMERIC",
        ),
        (SBInt64ToNumeric, List[SValue](STNat(MaxScale), SInt64(10)), "INT64_TO_NUMERIC"),
        (SBNumericToInt64, List[SValue](STNat(MinScale), MaxNumeric0), "NUMERIC_TO_INT64"),
        (SBUnixDaysToDate, List[SValue](MaxInt64), "UNIX_DAYS_TO_DATE"),
        (SBUnixMicrosecondsToTimestamp, List(MaxInt64), "UNIX_MICROSECONDS_TO_TIMESTAMP"),
        (SBAddBigNumeric, List[SValue](VeryBigBigNumericA, VeryBigBigNumericB), "ADD_BIGNUMERIC"),
        (
          SBSubBigNumeric,
          List[SValue](VeryBigBigNumericA, VeryBigNegativeBigNumeric),
          "SUB_BIGNUMERIC",
        ),
        (SBMulBigNumeric, List[SValue](VeryBigBigNumericA, VeryBigBigNumericB), "MUL_BIGNUMERIC"),
        (
          SBDivBigNumeric,
          List[SValue](
            SInt64(0),
            SInt64(0),
            VeryBigBigNumericA,
            SBigNumeric.assertFromBigDecimal(BigDecimal.ZERO),
          ),
          "DIV_BIGNUMERIC",
        ),
        (
          SBShiftRightBigNumeric,
          List[SValue](SInt64(-1L), VeryBigBigNumericA),
          "SHIFT_RIGHT_BIGNUMERIC",
        ),
        (
          SBBigNumericToNumeric,
          List[SValue](TMinScale, VeryBigBigNumericA),
          "BIGNUMERIC_TO_NUMERIC",
        ),
      )

      forAll(cases) { (builtin, args, name) =>
        inside(
          evalSExpr(SEAppAtomicSaturatedBuiltin(builtin, args.map(SEValue(_)).toArray), false)
        ) {
          case Left(
                SError.SErrorDamlException(
                  IE.UnhandledException(ValueArithmeticError.typ, ValueArithmeticError(msg))
                )
              )
              if msg == s"ArithmeticError while evaluating ($name ${args.iterator.map(lit2string).mkString(" ")})." =>
        }
      }
    }
  }

  "Error builtins" - {

    "ERROR" - {
      "throws an exception" in {
        eval(e"""ERROR "message" """) shouldBe a[Left[_, _]]
      }
    }
  }

  "SBCrash" - {
    "throws an exception" in {
      val result = evalSExpr(
        SEApp(SEBuiltin(SBCrash("test message")), Array(SUnit)),
        onLedger = false,
      )

      inside(result) { case Left(SErrorCrash(_, message)) =>
        message should endWith("test message")
      }
    }
  }

  "AnyExceptionMessage" - {
    "request unknown packageId" in {
      eval(
        e"""ANY_EXCEPTION_MESSAGE (to_any_exception @Mod:Exception (Mod:Exception {}))"""
      ) shouldBe Right(SText("some nice error message"))
      eval(
        e"""ANY_EXCEPTION_MESSAGE (to_any_exception @Mod:ExceptionAppend (Mod:ExceptionAppend { front = "Hello", back = "world"}))"""
      ) shouldBe Right(SText("Helloworld"))
      inside(
        Try(
          eval(
            e"""ANY_EXCEPTION_MESSAGE (to_any_exception @'-unknown-package-':Mod:Exception ('-unknown-package-':Mod:Exception {}))"""
          )
        )
      ) { case Failure(error) =>
        error.getMessage should include("unknown package '-unknown-package-'")
      }
    }

    "should not request package for ArithmeticError" in {
      eval(
        e"""ANY_EXCEPTION_MESSAGE (to_any_exception @'cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a':DA.Exception.ArithmeticError:ArithmeticError ('cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a':DA.Exception.ArithmeticError:ArithmeticError { message = "Arithmetic error" }))"""
      ) shouldBe Right(SText("Arithmetic error"))
    }

  }

  "To/FromAnyException" - {
    val testCases = Table[String, String](
      ("expression", "string-result"),
      ("Mod:from1 Mod:A1", "1"),
      ("Mod:from2 Mod:A2", "2"),
      ("Mod:from3 Mod:A3", "3"),
      ("Mod:from1 Mod:A2", "NONE"),
      ("Mod:from1 Mod:A3", "NONE"),
      ("Mod:from2 Mod:A1", "NONE"),
      ("Mod:from2 Mod:A3", "NONE"),
      ("Mod:from3 Mod:A1", "NONE"),
      ("Mod:from3 Mod:A2", "NONE"),
    )

    forEvery(testCases) { (exp: String, res: String) =>
      s"""eval[$exp] --> "$res"""" in {
        val expected = Right(SValue.SText(res))
        eval(e"$exp") shouldBe expected
      }
    }
  }

  "Interfaces" - {
    val iouTypeRep = Ref.TypeConName.assertFromString("-pkgId-:Mod:Iou")
    val alice = Ref.Party.assertFromString("alice")
    val bob = Ref.Party.assertFromString("bob")

    val testCases = Table[String, SValue](
      "expression" -> "string-result",
      "interface_template_type_rep @Mod:Iface Mod:aliceOwesBobIface" -> STypeRep(
        TTyCon(iouTypeRep)
      ),
      "signatory_interface @Mod:Iface Mod:aliceOwesBobIface" -> SList(FrontStack(SParty(alice))),
      "observer_interface @Mod:Iface Mod:aliceOwesBobIface" -> SList(FrontStack(SParty(bob))),
    )

    forEvery(testCases) { (exp, res) =>
      s"""eval[$exp] --> "$res"""" in {
        eval(e"$exp") shouldBe Right(res)
      }
    }
  }

  "SBCheckTemplate" - {
    "detects templates that exist" in {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:Iou")

      evalSExpr(
        SEApp(SEBuiltin(SBCheckTemplate(templateId)), Array(SUnit)),
        onLedger = false,
      ) shouldBe Right(SBool(true))
    }

    "detects non-existent templates" in {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:NonExistent")

      evalSExpr(
        SEApp(SEBuiltin(SBCheckTemplate(templateId)), Array(SUnit)),
        onLedger = false,
      ) shouldBe Right(SBool(false))
    }
  }

  "SBCheckTemplateKey" - {
    "detects keys that exist for the template" in {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:IouWithKey")

      evalSExpr(
        SEApp(SEBuiltin(SBCheckTemplateKey(templateId)), Array(SUnit)),
        onLedger = false,
      ) shouldBe Right(SBool(true))
    }

    "detects non-existent template keys" in {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:Iou")

      evalSExpr(
        SEApp(SEBuiltin(SBCheckTemplateKey(templateId)), Array(SUnit)),
        onLedger = false,
      ) shouldBe Right(SBool(false))
    }
  }

  "SBCacheDisclosedContract" - {
    "updates on ledger cached contract map" - {
      val version = TransactionVersion.minExplicitDisclosure
      val contractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))

      "when no template key is defined" in {
        val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:Iou")
        val (disclosedContract, None) =
          buildDisclosedContract(contractId, alice, alice, templateId, withKey = false)
        val cachedContract = CachedContract(
          templateId,
          disclosedContract.argument,
          Set(alice),
          Set.empty,
          None,
        )
        val cachedContractSExpr = SBuildCachedContract(
          SEValue(STypeRep(TTyCon(templateId))),
          SEValue(disclosedContract.argument),
          SEValue(SList(FrontStack(SParty(alice)))),
          SEValue(SList(FrontStack.Empty)),
          SEValue(SOptional(None)),
        )

        inside(
          evalSExpr(
            SELet1(
              cachedContractSExpr,
              SEAppAtomic(SEBuiltin(SBCacheDisclosedContract(contractId)), Array(SELocS(1))),
            ),
            getContract = Map(
              contractId -> VersionedContractInstance(
                version,
                templateId,
                disclosedContract.argument.toUnnormalizedValue,
                "Agreement",
              )
            ),
            onLedger = true,
          )
        ) { case Right((SUnit, contractCache, disclosedContractKeys)) =>
          contractCache shouldBe Map(
            contractId -> cachedContract
          )
          disclosedContractKeys shouldBe Map.empty
        }
      }

      "when template key is defined" in {
        val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:IouWithKey")
        val (disclosedContract, Some((key, keyWithMaintainers, keyHash))) =
          buildDisclosedContract(contractId, alice, alice, templateId, withKey = true)
        val optionalKey = Some(SKeyWithMaintainers(key, Set(alice)))
        val cachedContract = CachedContract(
          templateId,
          disclosedContract.argument,
          Set(alice),
          Set.empty,
          optionalKey,
        )
        val cachedContractSExpr = SBuildCachedContract(
          SEValue(STypeRep(TTyCon(templateId))),
          SEValue(disclosedContract.argument),
          SEValue(SList(FrontStack(SParty(alice)))),
          SEValue(SList(FrontStack.Empty)),
          SEValue(SOptional(Some(keyWithMaintainers))),
        )

        inside(
          evalSExpr(
            SELet1(
              cachedContractSExpr,
              SEAppAtomic(SEBuiltin(SBCacheDisclosedContract(contractId)), Array(SELocS(1))),
            ),
            getContract = Map(
              contractId -> VersionedContractInstance(
                version,
                templateId,
                disclosedContract.argument.toUnnormalizedValue,
                "Agreement",
              )
            ),
            onLedger = true,
          )
        ) { case Right((SUnit, contractCache, disclosedContractKeys)) =>
          contractCache shouldBe Map(
            contractId -> cachedContract
          )
          disclosedContractKeys shouldBe Map(keyHash -> SValue.SContractId(contractId))
        }
      }
    }
  }
}

object SBuiltinTest {

  import SpeedyTestLib.loggingContext

  private lazy val pkg =
    p"""
        module Mod {
          variant Either a b = Left : a | Right : b ;
          record @serializable MyUnit = { };
          record Tuple a b = { fst: a, snd: b };
          enum Color = Red | Green | Blue;
          record @serializable Exception = {} ;
          exception Exception = {
              message \(e: Mod:Exception) -> "some nice error message"
          } ;

         record @serializable ExceptionAppend = { front: Text, back: Text } ;
         exception ExceptionAppend = {
           message \(e: Mod:ExceptionAppend) -> APPEND_TEXT (Mod:ExceptionAppend {front} e) (Mod:ExceptionAppend {back} e)
         };

          record @serializable Ex1 = { message: Text } ;
          exception Ex1 = {
            message \(e: Mod:Ex1) -> Mod:Ex1 {message} e
          };
          record @serializable Ex2 = { message: Text } ;
          exception Ex2 = {
            message \(e: Mod:Ex2) -> Mod:Ex2 {message} e
          };
          record @serializable Ex3 = { message: Text } ;
          exception Ex3 = {
            message \(e: Mod:Ex3) -> Mod:Ex3 {message} e
          };
          val A1 : AnyException = to_any_exception @Mod:Ex1 (Mod:Ex1 { message = "1" });
          val A2 : AnyException = to_any_exception @Mod:Ex2 (Mod:Ex2 { message = "2" });
          val A3 : AnyException = to_any_exception @Mod:Ex3 (Mod:Ex3 { message = "3" });
          val from1 : AnyException -> Text = \(e:AnyException) -> case from_any_exception @Mod:Ex1 e of None -> "NONE" | Some x -> Mod:Ex1 { message} x;
          val from2 : AnyException -> Text = \(e:AnyException) -> case from_any_exception @Mod:Ex2 e of None -> "NONE" | Some x -> Mod:Ex2 { message} x;
          val from3 : AnyException -> Text = \(e:AnyException) -> case from_any_exception @Mod:Ex3 e of None -> "NONE" | Some x -> Mod:Ex3 { message} x;

          interface (this : Iface) = { viewtype Mod:MyUnit; };

          record @serializable Iou = { i: Party, u: Party, name: Text };
          template (this: Iou) = {
            precondition True;
            signatories Cons @Party [Mod:Iou {i} this] (Nil @Party);
            observers Cons @Party [Mod:Iou {u} this] (Nil @Party);
            agreement "Agreement";
            implements Mod:Iface { view = Mod:MyUnit {}; };
          };

          record @serializable Key = { label: Text, maintainers: List Party };

          record @serializable IouWithKey = { i: Party, u: Party, name: Text, k: Party };
          template (this: IouWithKey) = {
            precondition True;
            signatories Cons @Party [Mod:IouWithKey {i} this] (Nil @Party);
            observers Cons @Party [Mod:IouWithKey {u} this] (Nil @Party);
            agreement "Agreement";
            implements Mod:Iface { view = Mod:MyUnit {}; };
            key @Mod:Key
              (Mod:Key { label = "test-key", maintainers = (Cons @Party [Mod:IouWithKey {k} this] (Nil @Party)) })
              (\(key: Mod:Key) -> (Mod:Key {maintainers} key));
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";
          val bob : Party = Mod:mkParty "bob";

          val aliceOwesBob : Mod:Iou = Mod:Iou { i = Mod:alice, u = Mod:bob, name = "alice owes bob" };
          val aliceOwesBobIface : Mod:Iface = to_interface @Mod:Iface @Mod:Iou Mod:aliceOwesBob;
        }

    """

  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(Map(defaultParserParameters.defaultPackageId -> pkg))

  private def eval(e: Expr, onLedger: Boolean = true): Either[SError, SValue] =
    evalSExpr(compiledPackages.compiler.unsafeCompile(e), onLedger)

  private def evalApp(
      e: Expr,
      args: Array[SValue],
      onLedger: Boolean = true,
  ): Either[SError, SValue] =
    evalSExpr(SEApp(compiledPackages.compiler.unsafeCompile(e), args), onLedger)

  val alice: Party = Ref.Party.assertFromString("Alice")
  val committers: Set[Party] = Set(alice)

  private def evalSExpr(sexpr: SExpr, onLedger: Boolean): Either[SError, SValue] = {
    evalSExpr(sexpr, PartialFunction.empty, onLedger).map(_._1)
  }

  private def evalSExpr(
      sexpr: SExpr,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      onLedger: Boolean,
  ): Either[
    SError,
    (SValue, Map[ContractId, CachedContract], Map[crypto.Hash, SValue.SContractId]),
  ] = {
    val machine =
      if (onLedger) {
        Speedy.Machine.fromUpdateSExpr(
          compiledPackages,
          transactionSeed = crypto.Hash.hashPrivateKey("SBuiltinTest"),
          updateSE = SELet1(sexpr, SEMakeClo(Array(SELocS(1)), 1, SELocF(0))),
          committers = committers,
        )
      } else {
        Speedy.Machine.fromPureSExpr(compiledPackages, sexpr)
      }

    SpeedyTestLib.run(machine, getContract = getContract).map { value =>
      machine.ledgerMode match {
        case onLedger: OnLedger =>
          (value, onLedger.getCachedContracts, onLedger.disclosureKeyTable.toMap)

        case _ =>
          (value, Map.empty, Map.empty)
      }
    }
  }

  def intList(xs: Long*): String =
    if (xs.isEmpty) "(Nil @Int64)"
    else xs.mkString(s"(Cons @Int64 [", ", ", s"] (Nil @Int64))")

  private val entryFields = Struct.assertFromNameSeq(List(keyFieldName, valueFieldName))

  private def mapEntry(k: String, v: SValue) = SStruct(entryFields, ArrayList(SText(k), v))

  private def lit2string(x: SValue): String =
    x match {
      case SBool(b) => b.toString
      case SInt64(i) => i.toString
      case STimestamp(t) => t.toString
      case SDate(date) => date.toString
      case SBigNumeric(x) => Numeric.toUnscaledString(x)
      case SNumeric(x) => Numeric.toUnscaledString(x)
      case STNat(n) => s"@$n"
      case _ => sys.error(s"litToText: unexpected $x")
    }

  def buildDisclosedContract(
      contractId: ContractId,
      owner: Party,
      maintainer: Party,
      templateId: Ref.Identifier,
      withKey: Boolean,
  ): (DisclosedContract, Option[(SValue, SValue, crypto.Hash)]) = {
    val key = SValue.SRecord(
      templateId,
      ImmArray(
        Ref.Name.assertFromString("label"),
        Ref.Name.assertFromString("maintainers"),
      ),
      ArrayList(
        SValue.SText("test-key"),
        SValue.SList(FrontStack(SValue.SParty(maintainer))),
      ),
    )
    val keyWithMaintainers = SStruct(
      Struct.assertFromNameSeq(
        Seq(Ref.Name.assertFromString("key"), Ref.Name.assertFromString("maintainers"))
      ),
      ArrayList(
        key,
        SValue.SList(FrontStack(SValue.SParty(maintainer))),
      ),
    )
    val globalKey =
      if (withKey) {
        Some(
          GlobalKeyWithMaintainers(
            GlobalKey(templateId, key.toUnnormalizedValue),
            Set(maintainer),
          )
        )
      } else {
        None
      }
    val keyHash = globalKey.map(_.globalKey.hash)
    val fields = if (withKey) ImmArray("i", "u", "name", "k") else ImmArray("i", "u", "name")
    val values: util.ArrayList[SValue] =
      if (withKey) {
        ArrayList(
          SValue.SParty(owner),
          SValue.SParty(maintainer),
          SValue.SText("x"),
          SValue.SParty(maintainer),
        )
      } else {
        ArrayList(SValue.SParty(owner), SValue.SParty(maintainer), SValue.SText("y"))
      }
    val disclosedContract = DisclosedContract(
      templateId,
      SContractId(contractId),
      SValue.SRecord(
        templateId,
        fields.map(Ref.Name.assertFromString),
        values,
      ),
      ContractMetadata(Time.Timestamp.now(), keyHash, ImmArray.Empty),
    )

    (disclosedContract, if (withKey) Some((key, keyWithMaintainers, keyHash.get)) else None)
  }
}
