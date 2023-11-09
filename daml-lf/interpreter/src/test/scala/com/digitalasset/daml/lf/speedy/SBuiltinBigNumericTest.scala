// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.testing.parser.Implicits.{SyntaxHelper}
import com.daml.lf.testing.parser.ParserParameters
import org.scalatest.Inside.inside
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec

import scala.language.implicitConversions

class SBuiltinBigNumericTestV1 extends SBuiltinBigNumericTest(LanguageMajorVersion.V1)
class SBuiltinBigNumericTestV2 extends SBuiltinBigNumericTest(LanguageMajorVersion.V2)

class SBuiltinBigNumericTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks {

  val helpers = new SBuiltinBigNumericTestHelpers(majorLanguageVersion)
  import helpers.{parserParameters => _, _}

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  private implicit def toScale(i: Int): Numeric.Scale = Numeric.Scale.assertFromInt(i)

  private def n(scale: Int, x: BigDecimal): Numeric = Numeric.assertFromBigDecimal(scale, x)
  private def n(scale: Int, str: String): Numeric = n(scale, BigDecimal(str))
  private def s(scale: Int, x: BigDecimal): String = Numeric.toString(n(scale, x))
  private def s(scale: Int, str: String): String = s(scale, BigDecimal(str))

  private def tenPowerOf(i: Int, scale: Int) =
    if (i == 0)
      "1." + "0" * scale
    else if (i > 0)
      "1" + "0" * i + "." + "0" * scale
    else
      "0." + "0" * (-i - 1) + "1" + "0" * (scale + i)

  private val decimals = Table[String](
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

  "BigNumeric operations" - {
    import java.math.BigDecimal
    import SValue.SBigNumeric.{MaxScale, MinScale}

    "BigNumeric binary operations compute proper results" in {
      import scala.math.{BigDecimal => BigDec}
      import SBigNumeric.assertFromBigDecimal

      val testCases = Table[String, (BigDecimal, BigDecimal) => Option[SValue]](
        ("builtin", "reference"),
        ("ADD_BIGNUMERIC", (a, b) => Some(assertFromBigDecimal(a add b))),
        ("SUB_BIGNUMERIC", (a, b) => Some(assertFromBigDecimal(a subtract b))),
        ("MUL_BIGNUMERIC ", (a, b) => Some(assertFromBigDecimal(a multiply b))),
        (
          "DIV_BIGNUMERIC 10 ROUNDING_HALF_EVEN",
          {
            case (a, b) if b.signum != 0 =>
              Some(assertFromBigDecimal(a.divide(b, 10, java.math.RoundingMode.HALF_EVEN)))
            case _ => None
          },
        ),
        ("LESS_EQ @BigNumeric", (a, b) => Some(SBool(BigDec(a) <= BigDec(b)))),
        ("GREATER_EQ @BigNumeric", (a, b) => Some(SBool(BigDec(a) >= BigDec(b)))),
        ("LESS @BigNumeric", (a, b) => Some(SBool(BigDec(a) < BigDec(b)))),
        ("GREATER @BigNumeric", (a, b) => Some(SBool(BigDec(a) > BigDec(b)))),
        ("EQUAL @BigNumeric", (a, b) => Some(SBool(BigDec(a) == BigDec(b)))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(decimals) { a =>
          forEvery(decimals) { b =>
            val actualResult = eval(
              e"$builtin (NUMERIC_TO_BIGNUMERIC @10 ${s(10, a)}) (NUMERIC_TO_BIGNUMERIC @10 ${s(10, b)})"
            )

            actualResult.toOption shouldBe ref(n(10, a), n(10, b))
          }
        }
      }
    }

    "BIGNUMERIC_TO_TEXT" - {
      "return proper result" in {
        val testCases = Table(
          "expression" -> "regex",
          "BigNumeric:zero" -> "0\\.0",
          "BigNumeric:one" -> "1\\.0",
          "BigNumeric:minusOne" -> "-1\\.0",
          "BigNumeric:minPositive" -> s"0\\.(0{${SBigNumeric.MaxScale - 1}})1",
          "BigNumeric:maxPositive" -> s"(9{${SBigNumeric.MaxScale}})\\.(9{${SBigNumeric.MaxScale}})",
          "BigNumeric:maxNegative" -> s"-0\\.(0{${SBigNumeric.MaxScale - 1}})1",
          "BigNumeric:minNegative" -> s"-(9{${SBigNumeric.MaxScale}})\\.(9{${SBigNumeric.MaxScale}})",
        )

        forEvery(testCases) { (exp, regexp) =>
          val p = regexp.r.pattern
          inside(eval(e"BIGNUMERIC_TO_TEXT $exp")) {
            case Right(SText(x)) if p.matcher(x).find() =>
          }
        }
      }
    }

    "ADD_BIGNUMERIC" - {
      val builtin = "ADD_BIGNUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin BigNumeric:maxPositive BigNumeric:maxNegative") shouldBe a[Right[_, _]]
        eval(e"$builtin BigNumeric:maxPositive BigNumeric:minPositive") shouldBe a[Left[_, _]]
        eval(e"$builtin BigNumeric:minNegative BigNumeric:minPositive") shouldBe a[Right[_, _]]
        eval(e"$builtin BigNumeric:minNegative BigNumeric:maxNegative") shouldBe a[Left[_, _]]
        eval(e"$builtin BigNumeric:x BigNumeric:almostX") shouldBe a[Right[_, _]]
        eval(e"$builtin BigNumeric:x BigNumeric:x") shouldBe a[Left[_, _]]
      }
    }

    "SUB_BIGNUMERIC" - {
      val builtin = "SUB_BIGNUMERIC"

      "throws an exception in case of overflow" in {
        eval(e"$builtin BigNumeric:minNegative BigNumeric:maxNegative") shouldBe a[Right[_, _]]
        eval(e"$builtin BigNumeric:minNegative BigNumeric:minPositive") shouldBe a[Left[_, _]]
        eval(e"$builtin BigNumeric:maxPositive BigNumeric:minPositive") shouldBe a[Right[_, _]]
        eval(e"$builtin BigNumeric:maxPositive BigNumeric:maxNegative") shouldBe a[Left[_, _]]
        eval(e"$builtin BigNumeric:minusX BigNumeric:almostX") shouldBe a[Right[_, _]]
        eval(e"$builtin BigNumeric:minusX BigNumeric:x") shouldBe a[Left[_, _]]
      }
    }

    "MUL_BIGNUMERIC" - {
      "throws an exception in case of overflow" in {

        val testCases = Table(
          "arguments" -> "success",
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale} BigNumeric:one) BigNumeric:one" -> true,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale} BigNumeric:one) BigNumeric:ten" -> false,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2} BigNumeric:one)" -> true,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one)" -> false,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MaxScale} BigNumeric:one) BigNumeric:one" -> true,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MaxScale} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC 1 BigNumeric:one)" -> false,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2} BigNumeric:one)" -> true,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one)" -> false,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2} BigNumeric:underSqrtOfTen) (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:underSqrtOfTen)" -> true,
          s"(SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2} BigNumeric:overSqrtOfTen) (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:overSqrtOfTen)" -> false,
        )

        forEvery(testCases)((args, success) =>
          eval(e"MUL_BIGNUMERIC $args") shouldBe (if (success) a[Right[_, _]] else a[Left[_, _]])
        )
      }
    }

    "DIV_BIGNUMERIC" - {
      "throws an exception in case of overflow" in {
        val testCases = Table(
          "arguments" -> "success",
          s"-1000 ROUNDING_DOWN (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC ${-(MinScale / 2 - 1)} BigNumeric:one)" -> true,
          s"-1000 ROUNDING_DOWN (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC ${-(MinScale / 2 - 1)} BigNumeric:one)" -> false,
          s"-1000 ROUNDING_DOWN (SHIFT_RIGHT_BIGNUMERIC ${MinScale} BigNumeric:one) BigNumeric:one" -> true,
          s"-1000 ROUNDING_DOWN (SHIFT_RIGHT_BIGNUMERIC ${MinScale} BigNumeric:one) (SHIFT_RIGHT_BIGNUMERIC 1 BigNumeric:one)" -> false,
          s"-1000 ROUNDING_DOWN (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:underSqrtOfTen) (SHIFT_RIGHT_BIGNUMERIC ${-(MinScale / 2 - 1)} BigNumeric:overSqrtOfTen)" -> true,
          s"-1000 ROUNDING_DOWN (SHIFT_RIGHT_BIGNUMERIC ${MinScale / 2 - 1} BigNumeric:overSqrtOfTen) (SHIFT_RIGHT_BIGNUMERIC ${-(MinScale / 2 - 1)} BigNumeric:overSqrtOfTen)" -> false,
          s"${MinScale} ROUNDING_UP BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> false,
          s"${MinScale} ROUNDING_DOWN BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> true,
          s"${MinScale} ROUNDING_CEILING BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> false,
          s"${MinScale} ROUNDING_FLOOR BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> true,
          s"${MinScale} ROUNDING_HALF_UP BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> false,
          s"${MinScale} ROUNDING_HALF_DOWN BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> true,
          s"${MinScale} ROUNDING_HALF_EVEN BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:two)" -> false,
          s"${MinScale} ROUNDING_UP BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_DOWN BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_CEILING BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_FLOOR BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_HALF_UP BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_HALF_DOWN BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_HALF_EVEN BigNumeric:twentyEight (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_UP BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_DOWN BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_CEILING BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_FLOOR BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> true,
          s"${MinScale} ROUNDING_HALF_UP BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_HALF_DOWN BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_HALF_EVEN BigNumeric:twentyNine (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:three)" -> false,
          s"${MinScale} ROUNDING_UP BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> false,
          s"${MinScale} ROUNDING_DOWN BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> true,
          s"${MinScale} ROUNDING_CEILING BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> true,
          s"${MinScale} ROUNDING_FLOOR BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> false,
          s"${MinScale} ROUNDING_HALF_UP BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> false,
          s"${MinScale} ROUNDING_HALF_DOWN BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> true,
          s"${MinScale} ROUNDING_HALF_EVEN BigNumeric:nineteen (SHIFT_RIGHT_BIGNUMERIC ${-MinScale} BigNumeric:minusTwo)" -> false,
        )
        forEvery(testCases)((args, success) =>
          eval(e"DIV_BIGNUMERIC $args") shouldBe (if (success) a[Right[_, _]] else a[Left[_, _]])
        )
      }

      "crash if cannot compute the result without rounding" in {
        val testCases = Table(
          "arguments" -> "success",
          s"${MaxScale} ROUNDING_UNNECESSARY BigNumeric:one BigNumeric:two" -> true,
          s"${MaxScale} ROUNDING_UNNECESSARY BigNumeric:one BigNumeric:three" -> false,
          s"${MaxScale / 2} ROUNDING_UNNECESSARY BigNumeric:one BigNumeric:three" -> false,
          "1 ROUNDING_UNNECESSARY BigNumeric:one BigNumeric:two" -> true,
          "0 ROUNDING_UNNECESSARY BigNumeric:one BigNumeric:two" -> false,
        )

        forEvery(testCases)((args, success) =>
          eval(e"DIV_BIGNUMERIC $args") shouldBe (if (success) a[Right[_, _]] else a[Left[_, _]])
        )
      }

      "round as expected" in {

        val testCases = Table(
          "arguments" -> "success",
          "0 ROUNDING_UP BigNumeric:nineteen BigNumeric:two" -> "10",
          "0 ROUNDING_DOWN BigNumeric:nineteen BigNumeric:two" -> "9",
          "0 ROUNDING_CEILING BigNumeric:nineteen BigNumeric:two" -> "10",
          "0 ROUNDING_FLOOR BigNumeric:nineteen BigNumeric:two" -> "9",
          "0 ROUNDING_HALF_UP BigNumeric:nineteen BigNumeric:two" -> "10",
          "0 ROUNDING_HALF_DOWN BigNumeric:nineteen BigNumeric:two" -> "9",
          "0 ROUNDING_HALF_EVEN BigNumeric:nineteen BigNumeric:two" -> "10",
          "1 ROUNDING_UP BigNumeric:twentyEight BigNumeric:three" -> "9.4",
          "1 ROUNDING_DOWN BigNumeric:twentyEight BigNumeric:three" -> "9.3",
          "1 ROUNDING_CEILING BigNumeric:twentyEight BigNumeric:three" -> "9.4",
          "1 ROUNDING_FLOOR BigNumeric:twentyEight BigNumeric:three" -> "9.3",
          "1 ROUNDING_HALF_UP BigNumeric:twentyEight BigNumeric:three" -> "9.3",
          "1 ROUNDING_HALF_DOWN BigNumeric:twentyEight BigNumeric:three" -> "9.3",
          "1 ROUNDING_HALF_EVEN BigNumeric:twentyEight BigNumeric:three" -> "9.3",
          "2 ROUNDING_UP BigNumeric:twentyNine BigNumeric:three" -> "9.67",
          "2 ROUNDING_DOWN BigNumeric:twentyNine BigNumeric:three" -> "9.66",
          "2 ROUNDING_CEILING BigNumeric:twentyNine BigNumeric:three" -> "9.67",
          "2 ROUNDING_FLOOR BigNumeric:twentyNine BigNumeric:three" -> "9.66",
          "2 ROUNDING_HALF_UP BigNumeric:twentyNine BigNumeric:three" -> "9.67",
          "2 ROUNDING_HALF_DOWN BigNumeric:twentyNine BigNumeric:three" -> "9.67",
          "2 ROUNDING_HALF_EVEN BigNumeric:twentyNine BigNumeric:three" -> "9.67",
          "0 ROUNDING_UP BigNumeric:nineteen BigNumeric:minusTwo" -> "-10",
          "0 ROUNDING_DOWN BigNumeric:nineteen BigNumeric:minusTwo" -> "-9",
          "0 ROUNDING_CEILING BigNumeric:nineteen BigNumeric:minusTwo" -> "-9",
          "0 ROUNDING_FLOOR BigNumeric:nineteen BigNumeric:minusTwo" -> "-10",
          "0 ROUNDING_HALF_UP BigNumeric:nineteen BigNumeric:minusTwo" -> "-10",
          "0 ROUNDING_HALF_DOWN BigNumeric:nineteen BigNumeric:minusTwo" -> "-9",
          "0 ROUNDING_HALF_EVEN BigNumeric:nineteen BigNumeric:minusTwo" -> "-10",
        )
        forEvery(testCases)((args, result) =>
          inside(eval(e"DIV_BIGNUMERIC $args")) { case Right(SBigNumeric(x)) =>
            assert(x == new BigDecimal(result).stripTrailingZeros())
          }
        )
      }

    }

    "SHIFT_RIGHT_BIGNUMERIC" - {
      import java.math.BigDecimal
      import SBigNumeric.assertFromBigDecimal

      "returns proper result" in {
        val testCases = Table[Int, Int, String, String](
          ("input scale", "output scale", "input", "output"),
          (0, 1, s(0, Numeric.maxValue(0)), s(1, Numeric.maxValue(1))),
          (0, 37, tenPowerOf(1, 0), tenPowerOf(-36, 37)),
          (20, 10, tenPowerOf(15, 20), tenPowerOf(5, 30)),
          (20, -10, tenPowerOf(15, 20), tenPowerOf(25, 10)),
          (10, 10, tenPowerOf(-5, 10), tenPowerOf(-15, 20)),
          (20, -10, tenPowerOf(-5, 20), tenPowerOf(5, 10)),
          (10, 10, tenPowerOf(10, 10), tenPowerOf(0, 20)),
          (20, -10, tenPowerOf(10, 20), tenPowerOf(20, 10)),
        )
        forEvery(testCases) { (inputScale, shifting, input, output) =>
          eval(
            e"SHIFT_RIGHT_BIGNUMERIC $shifting (NUMERIC_TO_BIGNUMERIC @$inputScale $input)"
          ) shouldBe Right(assertFromBigDecimal(new BigDecimal(output)))
        }
      }

      "throws an exception in case of underflow/overflow" in {
        val testCases = Table(
          "arguments" -> "success",
          "1 BigNumeric:maxPositive" -> false,
          "-1 BigNumeric:maxPositive" -> false,
          s"${MaxScale} BigNumeric:one" -> true,
          s"${MaxScale + 1} BigNumeric:one" -> false,
          s"${MinScale} BigNumeric:one" -> true,
          s"${MinScale - 1} BigNumeric:one" -> false,
        )

        forEvery(testCases)((args, success) =>
          eval(e"SHIFT_RIGHT_BIGNUMERIC  $args") shouldBe (if (success) a[Right[_, _]]
                                                           else a[Left[_, _]])
        )
      }

      "never throw an exception when trying to shifting 0" in {
        val testCases = Table(
          "argument",
          Long.MinValue,
          Int.MinValue.toLong,
          MinScale.toLong - 1,
          MinScale.toLong,
          -1L,
          0L,
          1L,
          MaxScale.toLong,
          MaxScale.toLong + 1,
          Int.MaxValue.toLong,
          Long.MaxValue,
        )
        forEvery(testCases)(arg =>
          eval(e"SHIFT_RIGHT_BIGNUMERIC $arg BigNumeric:zero") shouldBe Right(SBigNumeric.Zero)
        )
      }

    }
  }

}

final class SBuiltinBigNumericTestHelpers(majorLanguageVersion: LanguageMajorVersion) {

  import SpeedyTestLib.loggingContext

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  private val pkg =
    p"""
        module BigNumeric {

          val maxScale: Int64 = ${SValue.SBigNumeric.MaxScale};
          val minScale: Int64 = SUB_INT64 1 BigNumeric:maxScale;

          val zero: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 0. ;
          val one: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 1. ;
          val two: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 2. ;
          val three: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 3. ;
          val minusOne: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 -1. ;
          val minusTwo: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 -2. ;
          val ten: BigNumeric = NUMERIC_TO_BIGNUMERIC @0 10. ;
          val underSqrtOfTen: BigNumeric =
            NUMERIC_TO_BIGNUMERIC @37 3.1622776601683793319988935444327185337;
          val overSqrtOfTen: BigNumeric =
            NUMERIC_TO_BIGNUMERIC @37 3.1622776601683793319988935444327185338;
          val nineteen: BigNumeric =
            NUMERIC_TO_BIGNUMERIC @0 19.;
          val twentyEight: BigNumeric =
            NUMERIC_TO_BIGNUMERIC @0 28.;
          val twentyNine: BigNumeric =
            NUMERIC_TO_BIGNUMERIC @0 29.;
          val minPositive: BigNumeric =
            SHIFT_RIGHT_BIGNUMERIC BigNumeric:maxScale BigNumeric:one;
          val maxPositive: BigNumeric =
            let x: BigNumeric = SUB_BIGNUMERIC BigNumeric:one BigNumeric:minPositive in
            ADD_BIGNUMERIC x (SHIFT_RIGHT_BIGNUMERIC (SUB_INT64 0 BigNumeric:maxScale) x);
          val maxNegative: BigNumeric =
            SHIFT_RIGHT_BIGNUMERIC BigNumeric:maxScale BigNumeric:minusOne;
          val minNegative: BigNumeric =
            let x: BigNumeric = ADD_BIGNUMERIC BigNumeric:minusOne BigNumeric:minPositive in
              ADD_BIGNUMERIC x (SHIFT_RIGHT_BIGNUMERIC (SUB_INT64 0 BigNumeric:maxScale) x);

          val tenPower: Int64 -> BigNumeric = \(n: Int64) ->
           SHIFT_RIGHT_BIGNUMERIC (SUB_INT64 0 n) BigNumeric:one;
          val x: BigNumeric = SHIFT_RIGHT_BIGNUMERIC ${SValue.SBigNumeric.MinScale} (NUMERIC_TO_BIGNUMERIC @0 5.);
          val almostX: BigNumeric = SUB_BIGNUMERIC BigNumeric:x BigNumeric:minPositive;
          val minusX: BigNumeric = SUB_BIGNUMERIC BigNumeric:zero BigNumeric:x;
        }
    """

  val compiledPackages =
    PureCompiledPackages.assertBuild(
      Map(parserParameters.defaultPackageId -> pkg),
      Compiler.Config.Default(majorLanguageVersion),
    )

  def eval(e: Expr): Either[SError.SError, SValue] =
    Speedy.Machine.runPureExpr(e, compiledPackages)
}
