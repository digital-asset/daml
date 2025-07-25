// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.crypto.MessageSignaturePrototypeUtil
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion}
import com.digitalasset.daml.lf.speedy.SBuiltinFun.{
  SBCrash,
  SBImportInputContract,
  SBuildContractInfoStruct,
}
import com.digitalasset.daml.lf.speedy.SError.{SError, SErrorCrash}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue.{SValue => _, _}
import com.digitalasset.daml.lf.speedy.Speedy.{CachedKey, ContractInfo, Machine}
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.security.{KeyPairGenerator, PrivateKey}
import java.util
import scala.language.implicitConversions
import scala.util.{Failure, Try}

class SBuiltinTestV2 extends SBuiltinTest(LanguageMajorVersion.V2)

class SBuiltinTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  val helpers = new SBuiltinTestHelpers(majorLanguageVersion)
  import helpers.{parserParameters => _, _}

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  implicit def toScale(i: Int): Numeric.Scale = Numeric.Scale.assertFromInt(i)

  def n(scale: Int, x: BigDecimal): Numeric = Numeric.assertFromBigDecimal(scale, x)
  def n(scale: Int, str: String): Numeric = n(scale, BigDecimal(str))
  def s(scale: Int, x: BigDecimal): String = Numeric.toString(n(scale, x))
  def s(scale: Int, str: String): String = s(scale, BigDecimal(str))
  def w(scale: Int): String = s(scale, 1)

  def tenPowerOf(i: Int, scale: Int = 10) =
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

    val maxNumeric = Numeric.maxValue(Numeric.Scale.assertFromInt(10))

    val minPosNumeric = BigDecimal("0000000000000000000000000000.0000000001")
    val bigBigDecimal = BigDecimal("8765432109876543210987654321.0987654321")
    val zero = BigDecimal("0.0000000000")
    val one = BigDecimal("1.0000000000")
    val two = BigDecimal("2.0000000000")

    val numerics = Table[String](
      "Numerics",
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
        eval(e"$builtin @10 ${s(10, maxNumeric)} ${s(10, minPosNumeric)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${s(10, maxNumeric.negate)} ${s(10, -minPosNumeric)}") shouldBe a[
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
        eval(e"$builtin @10 ${s(10, maxNumeric)} -$minPosNumeric") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${maxNumeric.negate} ${s(10, minPosNumeric)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 ${-bigBigDecimal} ${s(10, bigBigDecimal)}") shouldBe a[Left[_, _]]
      }
    }

    "MUL_NUMERIC" - {
      val builtin = "MUL_NUMERIC"
      val underSqrtOfTen = "3.1622776601683793319988935444327185337"
      val overSqrtOfTen = "3.1622776601683793319988935444327185338"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @0 @0 @0 ${w(0)} 1${"0" * 18}. 1${"0" * 19}.") shouldBe a[Right[_, _]]
        eval(e"$builtin @0 @0 @0 ${w(0)} 1${"0" * 19}.  1${"0" * 19}.") shouldBe a[Left[_, _]]
        eval(e"$builtin @37 @37 @37 ${w(37)} $underSqrtOfTen $underSqrtOfTen") shouldBe a[
          Right[_, _]
        ]
        eval(e"$builtin @37 @37 @37 ${w(37)} $overSqrtOfTen $underSqrtOfTen") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 ${w(10)} 1.1000000000 2.2000000000") shouldBe Right(
          SNumeric(n(10, 2.42))
        )
        eval(e"$builtin @10 @10 @10 ${w(10)} ${tenPowerOf(13)} ${tenPowerOf(14)}") shouldBe Right(
          SNumeric(n(10, "1E27"))
        )
        eval(e"$builtin @10 @10 @10 ${w(10)}  ${tenPowerOf(14)} ${tenPowerOf(14)}") shouldBe a[
          Left[_, _]
        ]
        eval(
          e"$builtin @10 @10 @10 ${w(10)}  ${s(10, bigBigDecimal)} ${bigBigDecimal - 1}"
        ) shouldBe a[
          Left[_, _]
        ]
      }
    }

    "DIV_NUMERIC" - {
      val builtin = "DIV_NUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin @37 @37 @37 ${w(37)} ${s(37, "1E-18")} ${s(37, "-1E-18")}") shouldBe a[
          Right[_, _]
        ]
        eval(e"$builtin @37 @37 @37 ${w(37)} ${s(37, "1E-18")} ${s(37, "-1E-19")}") shouldBe a[
          Left[_, _]
        ]
        eval(e"$builtin @1 @1 @1 ${w(1)} ${s(1, "1E36")} 0.2") shouldBe a[Right[_, _]]
        eval(e"$builtin @1 @1 @1 ${w(1)} ${s(1, "1E36")} 0.1") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 ${w(10)} 1.1000000000 2.2000000000") shouldBe Right(
          SNumeric(n(10, 0.5))
        )
        eval(
          e"$builtin @10 @10 @10 ${w(10)} ${s(10, bigBigDecimal)} ${tenPowerOf(-10)}"
        ) shouldBe a[
          Left[_, _]
        ]
        eval(e"$builtin @10 @10 @10 ${w(10)} ${tenPowerOf(17)} ${tenPowerOf(-10)}") shouldBe Right(
          SNumeric(n(10, "1E27"))
        )
        eval(e"$builtin @10 @10 @10 ${w(10)} ${tenPowerOf(18)} ${tenPowerOf(-10)}") shouldBe a[
          Left[_, _]
        ]
      }

      "throws an exception when divided by 0" in {
        eval(e"$builtin @10 @10 @10 ${w(10)} ${s(10, one)} ${tenPowerOf(-10)}") shouldBe Right(
          SNumeric(n(10, tenPowerOf(10)))
        )
        eval(e"$builtin @10 @10 @10 ${w(10)} ${s(10, one)} ${s(10, zero)}") shouldBe a[Left[_, _]]
        eval(e"$builtin @10 @10 @10 ${w(10)} ${s(10, bigBigDecimal)} ${s(10, zero)}") shouldBe a[
          Left[_, _]
        ]
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
          ("rounding", "numeric", "result"),
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

    "Numeric binary operations compute proper results" in {

      def round(x: BigDecimal) = n(10, x.setScale(10, BigDecimal.RoundingMode.HALF_EVEN))

      val testCases = Table[String, (Numeric, Numeric) => Option[SValue]](
        ("builtin", "reference"),
        ("ADD_NUMERIC @10", (a, b) => Some(SNumeric(n(10, a add b)))),
        ("SUB_NUMERIC @10", (a, b) => Some(SNumeric(n(10, a subtract b)))),
        (s"MUL_NUMERIC @10 @10 @10 ${w(10)}", (a, b) => Some(SNumeric(round(a multiply b)))),
        (
          s"DIV_NUMERIC @10 @10 @10 ${w(10)}",
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
        forEvery(numerics) { a =>
          forEvery(numerics) { b =>
            eval(e"$builtin ${s(10, a)} ${s(10, b)}").toOption shouldBe
              ref(n(10, a), n(10, b))
          }
        }
      }
    }

    "NUMERIC_TO_TEXT" - {
      "returns proper results" in {
        forEvery(numerics) { a =>
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
          eval(e"CAST_NUMERIC @$inputScale @$outputScale ${w(outputScale)} $x") shouldBe a[
            Left[_, _]
          ]
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
          eval(e"CAST_NUMERIC @$inputScale @$outputScale ${w(outputScale)} $x") shouldBe a[
            Left[_, _]
          ]
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
          eval(e"CAST_NUMERIC @$inputScale @$outputScale ${w(outputScale)} $x") shouldBe Right(
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
          eval(e"SHIFT_NUMERIC @$inputScale @$outputScale ${w(outputScale)} $input") shouldBe Right(
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
    val relativeSuffix = Bytes.fromByteArray(Array(0, 1, 2, 3))
    val absoluteSuffix = Bytes.fromByteArray(Array(-1, 0, 1, 2))

    val absoluteCidV1 = ContractId.V1(Hash.hashPrivateKey("#contract1"), absoluteSuffix)
    val localCidV1 = ContractId.V1(Hash.hashPrivateKey("#contract2"))

    def cidV2WithSuffix(discriminator: String, suffix: Bytes): ContractId =
      ContractId.V2
        .suffixed(Time.Timestamp.Epoch, Hash.hashPrivateKey(discriminator), suffix)
        .toOption
        .get

    val absoluteCidV2 = cidV2WithSuffix("#contract3", absoluteSuffix)
    val relativeCidV2 = cidV2WithSuffix("#contract4", relativeSuffix)
    val localCidV2 =
      ContractId.V2.unsuffixed(Time.Timestamp.Epoch, Hash.hashPrivateKey("#contract5"))

    "EQUAL @ContractId" - {
      "works as expected" in {
        evalApp(
          e"EQUAL @(ContractId Mod:T)",
          Array(SContractId(absoluteCidV1), SContractId(absoluteCidV1)),
        ) shouldBe Right(SBool(true))
        evalApp(
          e"EQUAL @(ContractId Mod:T)",
          Array(SContractId(absoluteCidV1), SContractId(localCidV1)),
        ) shouldBe Right(SBool(false))
      }
    }

    "TEXT_TO_CONTRACT-ID" - {
      "should parse absolute contract ID" in {
        val absoluteCidV1AsHex = absoluteCidV1.toBytes.toHexString
        val absoluteCidV2AsHex = absoluteCidV2.toBytes.toHexString
        evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(absoluteCidV1AsHex))) shouldBe Right(
          SContractId(absoluteCidV1)
        )
        evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(absoluteCidV2AsHex))) shouldBe Right(
          SContractId(absoluteCidV2)
        )
      }

      "should fail to parse relative contract id" in {
        val relativeCidV2AsHex = relativeCidV2.toBytes.toHexString
        inside {
          evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(relativeCidV2AsHex)))
        } { case Left(SError.SErrorDamlException(IE.Crypto(error))) =>
          error shouldBe IE.Crypto.MalformedContractId(relativeCidV2AsHex)
        }
      }

      "should fail to parse local contract id" in {
        val localCidV1AsHex = localCidV1.toBytes.toHexString
        val localCidV2AsHex = localCidV2.toBytes.toHexString
        inside {
          evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(localCidV1AsHex)))
        } { case Left(SError.SErrorDamlException(IE.Crypto(error))) =>
          error shouldBe IE.Crypto.MalformedContractId(localCidV1AsHex)
        }
        inside {
          evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(localCidV2AsHex)))
        } { case Left(SError.SErrorDamlException(IE.Crypto(error))) =>
          error shouldBe IE.Crypto.MalformedContractId(localCidV2AsHex)
        }
      }

      "should fail on malformed contract id" in {
        val invalidHexString = "invalid"
        val tooShortHexString = "0" * 64
        val tooLongHexString = "0" * 256

        inside(evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(invalidHexString)))) {
          case Left(SError.SErrorDamlException(IE.Crypto(error))) =>
            error shouldBe IE.Crypto.MalformedContractId(invalidHexString)
        }
        inside(evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(tooShortHexString)))) {
          case Left(SError.SErrorDamlException(IE.Crypto(error))) =>
            error shouldBe IE.Crypto.MalformedContractId(tooShortHexString)
        }
        inside(evalApp(e"TEXT_TO_CONTRACT_ID", Array(SText(tooLongHexString)))) {
          case Left(SError.SErrorDamlException(IE.Crypto(error))) =>
            error shouldBe IE.Crypto.MalformedContractId(tooLongHexString)
        }
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
          SError.SErrorDamlException(IE.NonComparableValues)
        )
        eval(expr2) shouldBe Left(
          SError.SErrorDamlException(IE.NonComparableValues)
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
          SError.SErrorDamlException(IE.NonComparableValues)
        )
        eval(expr2) shouldBe Left(
          SError.SErrorDamlException(IE.NonComparableValues)
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
          SError.SErrorDamlException(IE.NonComparableValues)
        )
        eval(expr2) shouldBe Left(
          SError.SErrorDamlException(IE.NonComparableValues)
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
          ("scale", "Numeric", "Int64"),
          (7, s(7, almostZero(7)), 0),
          (2, "0.00", 0),
          (8, "1.00000000", 1),
          (10, "1.0000000001", 1),
          (37, "1." + "9" * 37, 1),
          (20, "123456789.12345678912345678912", 123456789),
        )

        forEvery(testCases) { (scale, numeric, int64) =>
          eval(e"NUMERIC_TO_INT64 @$scale $numeric") shouldBe Right(SInt64(int64))
          eval(e"NUMERIC_TO_INT64 @$scale -$numeric") shouldBe Right(SInt64(-int64))
        }
      }
    }

    "INT64_TO_NUMERIC" - {
      "work as expected" in {
        val testCases = Table[Long]("Int64", 167, 11, 2, 1, 0, -1, -2, -13, -113)

        forEvery(testCases) { int64 =>
          eval(e"INT64_TO_NUMERIC @10 ${w(10)} $int64") shouldBe Right(SNumeric(n(10, int64)))
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
          val cid = ContractId.V1(Hash.hashPrivateKey("abc"))
          evalAppOnLedger(e"$f", Array(SContractId(cid))) shouldBe Right(SOptional(None))
        }
        "returns Some(abc) off-ledger" in {
          val f = """(\(c:(ContractId Mod:T)) -> CONTRACT_ID_TO_TEXT @Mod:T c)"""
          val cid = ContractId.V1(Hash.hashPrivateKey("abc"))
          evalApp(e"$f", Array(SContractId(cid))) shouldBe Right(SOptional(Some(SText(cid.coid))))
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
          eval(EApp(builtin, EBuiltinLit(BLText(input())))) shouldBe Right(
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
        val e = e"""TEXT_TO_NUMERIC @10 ${w(10)}"$input""""
        eval(e) shouldBe Right(SOptional(Some(SNumeric(n(10, expected)))))
      }
      forEvery(negativeTestCases) { input =>
        eval(e"""TEXT_TO_NUMERIC @10 ${w(10)}"$input"""") shouldBe Right(SOptional(None))
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
      val builtin = e"""TEXT_TO_NUMERIC @10 ${w(10)}"""

      forEvery(testCases) { (input, output) =>
        eval(EApp(builtin, EBuiltinLit(BLText(input())))) shouldBe Right(SOptional(output))
      }
    }

  }

  "ArithmeticBuiltins" - {

    "throw DamlArithmeticException with proper name and argument" in {

      import SBuiltinFun._
      import Numeric.Scale.{MinValue => MinScale, MaxValue => MaxScale}
      import java.math.BigDecimal

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
        (SBAddNumeric, List[SValue](MaxNumeric0, TwoNumeric0), "ADD_NUMERIC"),
        (SBSubNumeric, List[SValue](MinNumeric0, TwoNumeric0), "SUB_NUMERIC"),
        (
          SBMulNumeric,
          List[SValue](
            witness(MinScale),
            MaxNumeric0,
            MaxNumeric0,
          ),
          "MUL_NUMERIC",
        ),
        (
          SBDivNumeric,
          List[SValue](
            witness(MinScale),
            TwoNumeric0,
            SNumeric(Numeric.assertFromString("0.")),
          ),
          "DIV_NUMERIC",
        ),
        (
          SBRoundNumeric,
          List[SValue](SInt64(MaxScale.toLong), MaxNumeric0),
          "ROUND_NUMERIC",
        ),
        (
          SBCastNumeric,
          List[SValue](witness(MaxScale), MaxNumeric0),
          "CAST_NUMERIC",
        ),
        (SBInt64ToNumeric, List[SValue](witness(MaxScale), SInt64(10)), "INT64_TO_NUMERIC"),
        (SBNumericToInt64, List[SValue](MaxNumeric0), "NUMERIC_TO_INT64"),
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
          List[SValue](witness(MinScale), VeryBigBigNumericA),
          "BIGNUMERIC_TO_NUMERIC",
        ),
      )

      forAll(cases) { (builtin, args, name) =>
        inside(eval(SEAppAtomicSaturatedBuiltin(builtin, args.map(SEValue(_)).toArray))) {
          case Left(
                SError.SErrorDamlException(
                  IE.FailureStatus(
                    errorId,
                    _,
                    msg,
                    _,
                  )
                )
              ) =>
            msg shouldBe s"ArithmeticError while evaluating ($name ${args.iterator.map(lit2string).mkString(" ")})."
            errorId shouldBe "UNHANDLED_EXCEPTION/DA.Exception.ArithmeticError:ArithmeticError"
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
      inside(eval(SEApp(SEBuiltinFun(SBCrash("test message")), Array(SUnit)))) {
        case Left(SErrorCrash(_, message)) =>
          message should endWith("test message")
      }
    }
  }

  "AnyExceptionMessage" - {
    "request unknown packageId" in {
      evalOnLedger(
        e"""ANY_EXCEPTION_MESSAGE (to_any_exception @Mod:Exception (Mod:Exception {}))"""
      ) shouldBe Right(SText("some nice error message"))
      evalOnLedger(
        e"""ANY_EXCEPTION_MESSAGE (to_any_exception @Mod:ExceptionAppend (Mod:ExceptionAppend { front = "Hello", back = "world"}))"""
      ) shouldBe Right(SText("Helloworld"))
      inside(
        Try(
          evalOnLedger(
            e"""ANY_EXCEPTION_MESSAGE (to_any_exception @'-unknown-package-':Mod:Exception ('-unknown-package-':Mod:Exception {}))"""
          )
        )
      ) { case Failure(error) =>
        error.getMessage should include("unknown package '-unknown-package-'")
      }
    }

    "should not request package for ArithmeticError" in {
      val tyCon = StablePackages(majorLanguageVersion).ArithmeticError
      val prettyTyCon = s"'${tyCon.packageId}':${tyCon.qualifiedName}"
      eval(
        e"""ANY_EXCEPTION_MESSAGE (to_any_exception @$prettyTyCon ($prettyTyCon { message = "Arithmetic error" }))"""
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
    val iouTypeRep = Ref.TypeConId.assertFromString("-pkgId-:Mod:Iou")
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

  "SBCacheDisclosedContract" - {
    "updates on ledger cached contract map" - {
      val contractId = ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))

      "when no template key is defined" in {
        val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:Iou")
        val (disclosedContract, None) =
          buildDisclosedContract(contractId, alice, alice, templateId, withKey = false)
        val contractInfo = ContractInfo(
          version = txVersion,
          packageName = pkg.pkgName,
          templateId = templateId,
          value = disclosedContract.argument,
          signatories = Set(alice),
          observers = Set.empty,
          keyOpt = None,
        )
        val contractInfoSExpr = SBuildContractInfoStruct(
          SEValue(STypeRep(TTyCon(templateId))),
          SEValue(disclosedContract.argument),
          SEValue(SList(FrontStack(SParty(alice)))),
          SEValue(SList(FrontStack.Empty)),
          SEValue(SOptional(None)),
        )

        inside(
          evalOnLedger(
            SELet1(
              contractInfoSExpr,
              SEAppAtomic(
                SEBuiltinFun(SBImportInputContract(disclosedContract.contract, templateId)),
                Array(SELocS(1)),
              ),
            ),
            getContract = Map(
              contractId -> Versioned(
                version = txVersion,
                Value.ThinContractInstance(
                  packageName = pkg.pkgName,
                  template = templateId,
                  arg = disclosedContract.argument.toUnnormalizedValue,
                ),
              )
            ),
          )
        ) { case Right((SUnit, disclosedContracts, disclosedContractKeys)) =>
          disclosedContracts shouldBe Map(contractId -> contractInfo)
          disclosedContractKeys shouldBe empty
        }
      }

      "when template key is defined" in {
        val templateId = Ref.Identifier.assertFromString("-pkgId-:Mod:IouWithKey")
        val (disclosedContract, Some(key)) =
          buildDisclosedContract(contractId, alice, alice, templateId, withKey = true)
        val cachedKey = CachedKey(
          pkgName,
          GlobalKeyWithMaintainers
            .assertBuild(templateId, key.toUnnormalizedValue, Set(alice), pkg.pkgName),
          key,
        )
        val contractInfo = ContractInfo(
          version = txVersion,
          packageName = pkg.pkgName,
          templateId = templateId,
          value = disclosedContract.argument,
          signatories = Set(alice),
          observers = Set.empty,
          keyOpt = Some(cachedKey),
        )
        val contractInfoSExpr = SBuildContractInfoStruct(
          SEValue(STypeRep(TTyCon(templateId))),
          SEValue(disclosedContract.argument),
          SEValue(SList(FrontStack(SParty(alice)))),
          SEValue(SList(FrontStack.Empty)),
          SEValue(SOptional(Some(keyWithMaintainers(key, alice)))),
        )

        inside(
          evalOnLedger(
            SELet1(
              contractInfoSExpr,
              SEAppAtomic(
                SEBuiltinFun(SBImportInputContract(disclosedContract.contract, templateId)),
                Array(SELocS(1)),
              ),
            ),
            getContract = Map(
              contractId -> Versioned(
                version = txVersion,
                Value.ThinContractInstance(
                  template = templateId,
                  arg = disclosedContract.argument.toUnnormalizedValue,
                  packageName = pkg.pkgName,
                ),
              )
            ),
          )
        ) { case Right((SUnit, disclosedContracts, disclosedContractKeys)) =>
          disclosedContracts shouldBe Map(contractId -> contractInfo)
          disclosedContractKeys shouldBe Map(cachedKey.globalKey -> contractId)
        }
      }
    }
  }

  "ensure clause exception" - {
    "can be caught on when creating a contract" in {
      evalUpdateOnLedger(e"Mod:createFailingPreconditionAndCatchError") shouldBe Right(SUnit)
    }

    "cannot be caught when exercising a choice" in {
      inside {
        val cid = ContractId.V1(Hash.hashPrivateKey("abc"))
        evalUpdateAppOnLedger(
          e"Mod:exerciseFailingPreconditionAndCatchError",
          Array(SContractId(cid)),
          getContract = Map(
            cid -> Versioned(
              version = txVersion,
              Value.ThinContractInstance(
                packageName = pkg.pkgName,
                template = t"Mod:FailingPrecondition".asInstanceOf[Ast.TTyCon].tycon,
                arg = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
              ),
            )
          ),
        )
      } {
        case Left(
              SError.SErrorDamlException(
                IE.FailureStatus(_, _, msg, _)
              )
            ) =>
          msg shouldBe "failed precondition"
      }
    }
  }

  "Crypto" - {

    "KECCAK256_TEXT" - {
      "correctly digest hex strings" in {
        val testCases = Table(
          "" ->
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "00" -> "bc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a",
          "0000" -> "54a8c0ab653c15bfb48b47fd011ba2b9617af01cb45cab344acd57c924d56798",
          "deadbeef" -> "d4fd4e189132273036449fc9e11198c739161b4c0116a9a2dccdfa1c492006f1",
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ->
            "a332df1e58a99d8dcb0767dab2d23985ef337d59fadbe040f56e1c642b314973",
        )
        forEvery(testCases) { (input, output) =>
          eval(e"""KECCAK256_TEXT "$input"""") shouldBe Right(SText(output))
        }
      }

      "fail to digest non-hex strings" in {
        val testCases = Table(
          "0",
          "000",
          "0g",
          "DeadBeef",
          "843d0824-9133-4bc9-b0e8-7cb4e8487dd1",
          "input",
          """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
            |eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
            |minim veniam, quis nostrud exercitation ullamco laboris nisi ut
            |aliquip ex ea commodo consequat. Duis aute irure dolor in
            |reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
            |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
            |culpa qui officia deserunt mollit anim id est laborum..."""
            .replaceAll("\r", "")
            .stripMargin,
          "a¶‱😂",
        )
        forEvery(testCases) { input =>
          inside(eval(e"""KECCAK256_TEXT "$input"""")) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Crypto(IE.Crypto.MalformedByteEncoding(value, reason))
                  )
                ) =>
              value should be(input)
              reason should be("can not parse hex string")
          }
        }
      }
    }

    "SECP256K1_BOOL" - {
      val keyPair = support.crypto.MessageSignatureUtil.generateKeyPair

      "valid secp256k1 signature and public key" - {
        "correctly verify signed message" in {
          val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
          val privateKey = keyPair.getPrivate
          val message = Ref.HexString.assertFromString("deadbeef")
          val signature = support.crypto.MessageSignatureUtil.sign(message, privateKey)

          eval(e"""SECP256K1_BOOL "$signature" "$message" "$publicKey"""") shouldBe Right(
            SBool(true)
          )
        }

        "fail to verify unsigned messages" in {
          val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
          val privateKey = keyPair.getPrivate
          val message = Ref.HexString.assertFromString("deadbeef")
          val invalidMessage = Ref.HexString.assertFromString("deadbeefdeadbeef")
          val signature = support.crypto.MessageSignatureUtil.sign(message, privateKey)

          eval(e"""SECP256K1_BOOL "$signature" "$invalidMessage" "$publicKey"""") shouldBe Right(
            SBool(false)
          )
        }

        "throws with non-hex encoded messages" in {
          val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
          val privateKey = keyPair.getPrivate
          val message = Ref.HexString.assertFromString("deadbeef")
          val invalidMessage = "DeadBeef"
          val signature = support.crypto.MessageSignatureUtil.sign(message, privateKey)

          inside(eval(e"""SECP256K1_BOOL "$signature" "$invalidMessage" "$publicKey"""")) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Crypto(IE.Crypto.MalformedByteEncoding(value, reason))
                  )
                ) =>
              value should be(invalidMessage)
              reason should startWith(
                "can not parse message hex string"
              )
          }
        }
      }

      "valid signature and valid message" - {
        "fails with incorrect secp256k1 public key" in {
          val incorrectPublicKey =
            Bytes
              .fromByteArray(
                support.crypto.MessageSignatureUtil.generateKeyPair.getPublic.getEncoded
              )
              .toHexString
          val privateKey = keyPair.getPrivate
          val message = Ref.HexString.assertFromString("deadbeef")
          val signature = support.crypto.MessageSignatureUtil.sign(message, privateKey)

          eval(e"""SECP256K1_BOOL "$signature" "$message" "$incorrectPublicKey"""") shouldBe Right(
            SBool(false)
          )
        }

        "throws with invalid public key" in {
          val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
          invalidKeyPairGen.initialize(1024)
          val invalidPublicKey = Bytes
            .fromByteArray(invalidKeyPairGen.generateKeyPair().getPublic.getEncoded)
            .toHexString
          val privateKey = keyPair.getPrivate
          val message = Ref.HexString.assertFromString("deadbeef")
          val signature = support.crypto.MessageSignatureUtil.sign(message, privateKey)

          inside(eval(e"""SECP256K1_BOOL "$signature" "$message" "$invalidPublicKey"""")) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Crypto(IE.Crypto.MalformedKey(`invalidPublicKey`, reason))
                  )
                ) =>
              reason should startWith("java.security.InvalidKeyException")
          }
        }

        "throws with non-hex encoded public key" in {
          val invalidPublicKey = keyPair.getPublic
          val privateKey = keyPair.getPrivate
          val message = Ref.HexString.assertFromString("deadbeef")
          val signature = support.crypto.MessageSignatureUtil.sign(message, privateKey)

          inside(eval(e"""SECP256K1_BOOL "$signature" "$message" "$invalidPublicKey"""")) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Crypto(IE.Crypto.MalformedByteEncoding(value, reason))
                  )
                ) =>
              value should be(s"$invalidPublicKey")
              reason should be("can not parse DER encoded public key hex string")
          }
        }
      }

      "valid message and secp256k1 public key" - {
        "throws with invalid signature" in {
          def rsaSign(msg: Ref.HexString, pk: PrivateKey): Ref.HexString = {
            val signer = new MessageSignaturePrototypeUtil("SHA256withRSA")

            Bytes.fromByteArray(signer.sign(Bytes.fromHexString(msg).toByteArray, pk)).toHexString
          }

          val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
          invalidKeyPairGen.initialize(1024)
          val invalidPrivateKey = invalidKeyPairGen.generateKeyPair().getPrivate
          val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
          val message = Ref.HexString.assertFromString("deadbeef")
          val invalidSignature = rsaSign(message, invalidPrivateKey)

          inside(eval(e"""SECP256K1_BOOL "$invalidSignature" "$message" "$publicKey"""")) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Crypto(IE.Crypto.MalformedSignature(`invalidSignature`, reason))
                  )
                ) =>
              reason should be("error decoding signature bytes.")
          }
        }

        "throws with non-hex encoded signature" in {
          for (invalidSignature <- List("DeadBeef", "non-hex encoded")) {
            val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
            val message = Ref.HexString.assertFromString("deadbeef")

            inside(eval(e"""SECP256K1_BOOL "$invalidSignature" "$message" "$publicKey"""")) {
              case Left(
                    SError.SErrorDamlException(
                      IE.Crypto(IE.Crypto.MalformedByteEncoding(value, reason))
                    )
                  ) =>
                value should be(invalidSignature)
                reason should be("can not parse signature hex string")
            }
          }
        }
      }
    }

    "HEX_TO_TEXT" - {
      "correctly decode a hex string as text" in {
        val testCases = Table(
          "" -> "",
          "Hello world!" -> "48656c6c6f20776f726c6421",
          "DeadBeef" -> "4465616442656566",
          "843d0824-9133-4bc9-b0e8-7cb4e8487dd1" -> "38343364303832342d393133332d346263392d623065382d376362346538343837646431",
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ->
            "65336230633434323938666331633134396166626634633839393666623932343237616534316534363439623933346361343935393931623738353262383535",
          """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
            |eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
            |minim veniam, quis nostrud exercitation ullamco laboris nisi ut
            |aliquip ex ea commodo consequat. Duis aute irure dolor in
            |reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
            |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
            |culpa qui officia deserunt mollit anim id est laborum..."""
            .replaceAll("\r", "") ->
            "4c6f72656d20697073756d20646f6c6f722073697420616d65742c20636f6e73656374657475722061646970697363696e6720656c69742c2073656420646f0a2020202020202020202020207c656975736d6f642074656d706f7220696e6369646964756e74207574206c61626f726520657420646f6c6f7265206d61676e6120616c697175612e20557420656e696d2061640a2020202020202020202020207c6d696e696d2076656e69616d2c2071756973206e6f737472756420657865726369746174696f6e20756c6c616d636f206c61626f726973206e6973692075740a2020202020202020202020207c616c697175697020657820656120636f6d6d6f646f20636f6e7365717561742e2044756973206175746520697275726520646f6c6f7220696e0a2020202020202020202020207c726570726568656e646572697420696e20766f6c7570746174652076656c697420657373652063696c6c756d20646f6c6f726520657520667567696174206e756c6c610a2020202020202020202020207c70617269617475722e204578636570746575722073696e74206f6363616563617420637570696461746174206e6f6e2070726f6964656e742c2073756e7420696e0a2020202020202020202020207c63756c706120717569206f666669636961206465736572756e74206d6f6c6c697420616e696d20696420657374206c61626f72756d2e2e2e",
          "a¶‱😂" ->
            "61c2b6e280b1f09f9882",
        )
        forEvery(testCases) { (output, input) =>
          eval(e"""HEX_TO_TEXT "$input"""") shouldBe Right(SText(output))
        }
      }

      "fail to decode non-hex strings" in {
        val testCases = Table(
          "0",
          "000",
          "0g",
          "843d0824-9133-4bc9-b0e8-7cb4e8487dd1",
          "input",
          """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
            |eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
            |minim veniam, quis nostrud exercitation ullamco laboris nisi ut
            |aliquip ex ea commodo consequat. Duis aute irure dolor in
            |reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
            |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
            |culpa qui officia deserunt mollit anim id est laborum..."""
            .replaceAll("\r", "")
            .stripMargin,
          "a¶‱😂",
        )
        forEvery(testCases) { input =>
          inside(eval(e"""HEX_TO_TEXT "$input"""")) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Crypto(IE.Crypto.MalformedByteEncoding(value, reason))
                  )
                ) =>
              value should be(input)
              reason should be("can not parse hex string argument")
          }
        }
      }
    }

    "TEXT_TO_HEX" - {
      "correctly encode text as a hex string" in {
        val testCases = Table(
          "" -> "",
          "Hello world!" -> "48656c6c6f20776f726c6421",
          "DeadBeef" -> "4465616442656566",
          "843d0824-9133-4bc9-b0e8-7cb4e8487dd1" -> "38343364303832342d393133332d346263392d623065382d376362346538343837646431",
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ->
            "65336230633434323938666331633134396166626634633839393666623932343237616534316534363439623933346361343935393931623738353262383535",
          """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
            |eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
            |minim veniam, quis nostrud exercitation ullamco laboris nisi ut
            |aliquip ex ea commodo consequat. Duis aute irure dolor in
            |reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
            |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
            |culpa qui officia deserunt mollit anim id est laborum..."""
            .replaceAll("\r", "") ->
            "4c6f72656d20697073756d20646f6c6f722073697420616d65742c20636f6e73656374657475722061646970697363696e6720656c69742c2073656420646f0a2020202020202020202020207c656975736d6f642074656d706f7220696e6369646964756e74207574206c61626f726520657420646f6c6f7265206d61676e6120616c697175612e20557420656e696d2061640a2020202020202020202020207c6d696e696d2076656e69616d2c2071756973206e6f737472756420657865726369746174696f6e20756c6c616d636f206c61626f726973206e6973692075740a2020202020202020202020207c616c697175697020657820656120636f6d6d6f646f20636f6e7365717561742e2044756973206175746520697275726520646f6c6f7220696e0a2020202020202020202020207c726570726568656e646572697420696e20766f6c7570746174652076656c697420657373652063696c6c756d20646f6c6f726520657520667567696174206e756c6c610a2020202020202020202020207c70617269617475722e204578636570746575722073696e74206f6363616563617420637570696461746174206e6f6e2070726f6964656e742c2073756e7420696e0a2020202020202020202020207c63756c706120717569206f666669636961206465736572756e74206d6f6c6c697420616e696d20696420657374206c61626f72756d2e2e2e",
          "a¶‱😂" ->
            "61c2b6e280b1f09f9882",
        )
        forEvery(testCases) { (input, output) =>
          eval(e"""TEXT_TO_HEX "$input"""") shouldBe Right(SText(output))
        }
      }
    }
  }
}

final class SBuiltinTestHelpers(majorLanguageVersion: LanguageMajorVersion) {

  import SpeedyTestLib.loggingContext

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)

  lazy val pkg =
    p"""  metadata ( '-sbuiltin-test-' : '1.0.0' )
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
            implements Mod:Iface { view = Mod:MyUnit {}; };
          };

          record @serializable Key = { label: Text, maintainers: List Party };

          record @serializable IouWithKey = { i: Party, u: Party, name: Text, k: Party };
          template (this: IouWithKey) = {
            precondition True;
            signatories Cons @Party [Mod:IouWithKey {i} this] (Nil @Party);
            observers Cons @Party [Mod:IouWithKey {u} this] (Nil @Party);
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

          // a template whose precondition always evaluates to false
          record @serializable FailingPrecondition = { p: Party };
          template (this: FailingPrecondition) = {
            // Damlc will trhow DA.Exception.PreconditionFailed:PreconditionFailed but the engine expects no particular
            // exception type so we can throw Ex1 without modifying the outcome of the tests.
            precondition throw @Bool @Mod:Ex1 (Mod:Ex1 {message = "failed precondition"});
            signatories Cons @Party [Mod:FailingPrecondition {p} this] (Nil @Party);
            observers Nil @Party;

            choice @nonConsuming SomeChoice (self) (u: Unit): Text
              , controllers (Nil @Party)
              , observers (Nil @Party)
              to upure @Text "SomeChoice was called";
          };

          // checks that the FailedPrecondition error thrown when creating a FailingPrecondition instance can be caught
          val createFailingPreconditionAndCatchError: Update Unit =
            try @Unit
              ubind _:(ContractId Mod:FailingPrecondition) <-
                  create @Mod:FailingPrecondition (Mod:FailingPrecondition { p = Mod:alice })
              in upure @Unit ()
            catch
              e -> Some @(Update Unit) (upure @Unit ());

          // Tries to catch the error throw by the ensure clause of FailingPrecondition when exercising a choice on it,
          // should fail to do so.
          val exerciseFailingPreconditionAndCatchError: (ContractId Mod:FailingPrecondition) -> Update Text =
            \(cid: ContractId Mod:FailingPrecondition) ->
              try @Text
                exercise @Mod:FailingPrecondition SomeChoice cid ()
              catch
                e -> Some @(Update Text) (upure @Text "some exception was caught");
        }
    """

  val txVersion = pkg.languageVersion
  val pkgName = Ref.PackageName.assertFromString("-sbuiltin-test-")

  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(
      Map(parserParameters.defaultPackageId -> pkg),
      Compiler.Config.Default(majorLanguageVersion),
    )

  val stablePackages = StablePackages(majorLanguageVersion)

  def eval(e: Expr): Either[SError, SValue] =
    Machine.runPureExpr(e, compiledPackages)

  def evalApp(
      e: Expr,
      args: Array[SValue],
  ): Either[SError, SValue] =
    eval(SEApp(compiledPackages.compiler.unsafeCompile(e), args))

  val alice: Party = Ref.Party.assertFromString("Alice")
  val committers: Set[Party] = Set(alice)

  def eval(sexpr: SExpr): Either[SError, SValue] =
    Machine.runPureSExpr(sexpr, compiledPackages)

  def evalAppOnLedger(
      e: Expr,
      args: Array[SValue],
      getContract: PartialFunction[ContractId, Value.VersionedThinContractInstance] = Map.empty,
  ): Either[SError, SValue] =
    evalOnLedger(SEApp(compiledPackages.compiler.unsafeCompile(e), args), getContract).map(_._1)

  def evalOnLedger(
      e: Expr,
      getContract: PartialFunction[ContractId, Value.VersionedThinContractInstance] = Map.empty,
  ): Either[SError, SValue] =
    evalOnLedger(compiledPackages.compiler.unsafeCompile(e), getContract).map(_._1)

  def evalOnLedger(
      sexpr: SExpr,
      getContract: PartialFunction[ContractId, Value.VersionedThinContractInstance],
  ): Either[
    SError,
    (
        SValue,
        Map[ContractId, ContractInfo],
        Map[GlobalKey, ContractId],
    ),
  ] = evalUpdateOnLedger(SELet1(sexpr, SEMakeClo(Array(SELocS(1)), 1, SELocF(0))), getContract)

  def evalUpdateOnLedger(
      e: Expr,
      getContract: PartialFunction[ContractId, Value.VersionedThinContractInstance] = Map.empty,
  ): Either[SError, SValue] =
    evalUpdateOnLedger(compiledPackages.compiler.unsafeCompile(e), getContract).map(_._1)

  def evalUpdateAppOnLedger(
      e: Expr,
      args: Array[SValue],
      getContract: PartialFunction[ContractId, Value.VersionedThinContractInstance] = Map.empty,
  ): Either[SError, SValue] =
    evalUpdateOnLedger(SEApp(compiledPackages.compiler.unsafeCompile(e), args), getContract)
      .map(_._1)

  def evalUpdateOnLedger(
      sexpr: SExpr,
      getContract: PartialFunction[ContractId, Value.VersionedThinContractInstance],
  ): Either[
    SError,
    (
        SValue,
        Map[ContractId, ContractInfo],
        Map[GlobalKey, ContractId],
    ),
  ] = {
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        compiledPackages,
        transactionSeed = crypto.Hash.hashPrivateKey("SBuiltinTest"),
        updateSE = sexpr,
        committers = committers,
      )

    SpeedyTestLib
      .run(machine, getContract = getContract)
      .map(
        (_, machine.disclosedContracts, machine.disclosedContractKeys)
      )
  }

  def intList(xs: Long*): String =
    if (xs.isEmpty) "(Nil @Int64)"
    else xs.mkString(s"(Cons @Int64 [", ", ", s"] (Nil @Int64))")

  val entryFields = Struct.assertFromNameSeq(List(keyFieldName, valueFieldName))

  def mapEntry(k: String, v: SValue) = SStruct(entryFields, ArrayList(SText(k), v))

  def lit2string(x: SValue): String =
    x match {
      case SBool(b) => b.toString
      case SInt64(i) => i.toString
      case STimestamp(t) => t.toString
      case SDate(date) => date.toString
      case SBigNumeric(x) => Numeric.toUnscaledString(x)
      case SNumeric(x) => Numeric.toUnscaledString(x)
      case _ => sys.error(s"litToText: unexpected $x")
    }

  def keyWithMaintainers(key: SValue, maintainer: Party) = SStruct(
    Struct.assertFromNameSeq(
      Seq(Ref.Name.assertFromString("key"), Ref.Name.assertFromString("maintainers"))
    ),
    ArrayList(
      key,
      SValue.SList(FrontStack(SValue.SParty(maintainer))),
    ),
  )

  def buildDisclosedContract(
      contractId: ContractId,
      owner: Party,
      maintainer: Party,
      templateId: Ref.Identifier,
      withKey: Boolean,
  ): (DisclosedContract, Option[SValue]) = {
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
    val fields = if (withKey) ImmArray("i", "u", "name", "k") else ImmArray("i", "u", "name")
    val svalues: util.ArrayList[SValue] =
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
    val sarg = SValue.SRecord(
      templateId,
      fields.map(Ref.Name.assertFromString),
      svalues,
    )
    val txVersion = pkg.languageVersion
    val keyOpt =
      if (withKey)
        Some(
          GlobalKeyWithMaintainers
            .assertBuild(templateId, key.toNormalizedValue(txVersion), Set(maintainer), pkgName)
        )
      else
        None
    val disclosedContract = DisclosedContract(
      FatContractInstance.fromCreateNode(
        Node.Create(
          coid = contractId,
          packageName = pkg.pkgName,
          templateId = templateId,
          arg = sarg.toNormalizedValue(txVersion),
          signatories = Set(maintainer),
          stakeholders = Set(maintainer),
          keyOpt = keyOpt,
          version = txVersion,
        ),
        createTime = CreationTime.CreatedAt(Time.Timestamp.now()),
        cantonData = Bytes.Empty,
      ),
      sarg,
    )

    (disclosedContract, keyOpt.map(_ => key))
  }

  val witness = Numeric.Scale.values.map(n => SNumeric(Numeric.assertFromBigDecimal(n, 1)))

}
