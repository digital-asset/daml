// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import java.util

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.data.Ref.{Party, SimpleString}
import com.digitalasset.daml.lf.data.{Decimal, FrontStack, Time}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.speedy.SResult.{SResultContinue, SResultError}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.testing.parser.Implicits._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.immutable.HashMap

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SBuiltinTest extends FreeSpec with Matchers with TableDrivenPropertyChecks {

  import SBuiltinTest._

  "Integer operations" - {

    val MaxInt64 = Long.MaxValue
    val MinInt64 = Long.MinValue
    val aBigOddInt64: Long = 0X67890ABCEDF12345L

    val smallInt64s = Table[Long]("small integer values", 167, 11, 2, 1, 0, -1, -2, -11, -167)

    "ADD_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"ADD_INT64 $MaxInt64 -1") shouldBe Right(SInt64(MaxInt64 - 1))
        eval(e"ADD_INT64 $MaxInt64 1") shouldBe 'left
        eval(e"ADD_INT64 $MinInt64 1") shouldBe Right(SInt64(MinInt64 + 1))
        eval(e"ADD_INT64 $MinInt64 -1") shouldBe 'left
        eval(e"ADD_INT64 $aBigOddInt64 $aBigOddInt64") shouldBe 'left
      }
    }

    "SUB_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"SUB_INT64 $MaxInt64 1") shouldBe Right(SInt64(MaxInt64 - 1))
        eval(e"SUB_INT64 $MaxInt64 -1") shouldBe 'left
        eval(e"SUB_INT64 $MinInt64 -1") shouldBe Right(SInt64(MinInt64 + 1))
        eval(e"SUB_INT64 $MinInt64 1") shouldBe 'left
        eval(e"SUB_INT64 -$aBigOddInt64 $aBigOddInt64") shouldBe 'left
      }
    }

    "MUL_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"MUL_INT64 ${1L << 31} ${1L << 31}") shouldBe Right(SInt64(1L << 62))
        eval(e"MUL_INT64 ${1L << 32} ${1L << 31}") shouldBe 'left
        eval(e"MUL_INT64 ${1L << 32} -${1L << 31}") shouldBe Right(SInt64(1L << 63))
        eval(e"MUL_INT64 ${1L << 32} -${1L << 32}") shouldBe 'left
        eval(e"MUL_INT64 ${1L << 32} -${1L << 32}") shouldBe 'left
        eval(e"MUL_INT64 $aBigOddInt64 10") shouldBe 'left
      }
    }

    "DIV_INT64" - {
      "throws an exception if it overflows" in {
        eval(e"DIV_INT64 $MaxInt64 -1") shouldBe Right(SInt64(-MaxInt64))
        eval(e"DIV_INT64 $MinInt64 -1") shouldBe 'left
      }
    }

    "EXP_INT64" - {

      "throws an exception if the exponent is negative" in {
        eval(e"EXP_INT64 1 0") shouldBe Right(SInt64(1))
        eval(e"EXP_INT64 1 -1") shouldBe 'left
        eval(e"EXP_INT64 0 -1") shouldBe 'left
        eval(e"EXP_INT64 10 -1") shouldBe 'left
        eval(e"EXP_INT64 10 -20") shouldBe 'left
        eval(e"EXP_INT64 $aBigOddInt64 -10") shouldBe 'left
      }

      "throws an exception if it overflows" in {
        eval(e"EXP_INT64 ${1L << 6} 9") shouldBe Right(SInt64(1L << 54))
        eval(e"EXP_INT64 ${1L << 7} 9") shouldBe 'left
        eval(e"EXP_INT64 ${-(1L << 7)} 9") shouldBe Right(SInt64(1L << 63))
        eval(e"EXP_INT64 ${-(1L << 7)} 10") shouldBe 'left
        eval(e"EXP_INT64 3 $aBigOddInt64") shouldBe 'left
      }

      "accepts huge exponents for bases -1, 0 and, 1" in {
        eval(e"EXP_INT64 2 $aBigOddInt64") shouldBe 'left
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
          assert(result == result.longValue())
          eval(e"EXP_INT64 $base $exponent") shouldBe Right(SInt64(result.longValue()))
        }
      }
    }

    "Integer binary operations computes proper results" in {
      // EXP_INT64 is tested independently

      val testCases = Table[String, (Long, Long) => Either[Any, SValue]](
        ("builtin", "reference"),
        ("ADD_INT64", (a, b) => Right(SInt64(a + b))),
        ("SUB_INT64", (a, b) => Right(SInt64(a - b))),
        ("MUL_INT64", (a, b) => Right(SInt64(a * b))),
        ("DIV_INT64", (a, b) => if (b == 0) Left(()) else Right(SInt64(a / b))),
        ("MOD_INT64", (a, b) => if (b == 0) Left(()) else Right(SInt64(a % b))),
        ("LESS_EQ_INT64", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ_INT64", (a, b) => Right(SBool(a >= b))),
        ("LESS_INT64", (a, b) => Right(SBool(a < b))),
        ("GREATER_INT64", (a, b) => Right(SBool(a > b))),
        ("EQUAL_INT64", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(smallInt64s) { a =>
          forEvery(smallInt64s) { b =>
            eval(e"$builtin $a $b").left.map(_ => ()) shouldBe ref(a, b)
          }
        }
      }
    }

    "TO_TEXT_INT64" - {
      "return proper results" in {
        forEvery(smallInt64s) { a =>
          eval(e"TO_TEXT_INT64 $a") shouldBe Right(SText(a.toString))
        }
      }
    }

  }

  "Decimal operations" - {

    val maxDecimal = Decimal.max

    val minPosDecimal = BigDecimal("0000000000000000000000000000.0000000001")
    val bigBigDecimal = BigDecimal("8765432109876543210987654321.0987654321")

    val decimals = Table[String](
      "Decimals",
      "161803398.87499",
      "3.1415926536",
      "2.7182818285",
      "0.0000000001",
      "0.0",
      "-0.0000000001",
      "-2.7182818285",
      "-3.1415926536",
      "-161803398.87499",
    )

    "ADD_DECIMAL" - {
      "throw exception in case of overflow" in {
        eval(e"ADD_DECIMAL $bigBigDecimal 2.") shouldBe Right(SDecimal(bigBigDecimal + 2))
        eval(e"ADD_DECIMAL $maxDecimal $minPosDecimal") shouldBe 'left
        eval(e"ADD_DECIMAL ${-maxDecimal} ${-minPosDecimal}") shouldBe 'left
        eval(e"ADD_DECIMAL $bigBigDecimal $bigBigDecimal") shouldBe 'left
      }
    }

    "SUB_DECIMAL" - {
      "throws exception in case of overflow" in {
        eval(e"SUB_DECIMAL $bigBigDecimal 2.") shouldBe Right(SDecimal(bigBigDecimal - 2))
        eval(e"SUB_DECIMAL $maxDecimal -$minPosDecimal") shouldBe 'left
        eval(e"SUB_DECIMAL ${-maxDecimal} $minPosDecimal") shouldBe 'left
        eval(e"SUB_DECIMAL ${-bigBigDecimal} $bigBigDecimal") shouldBe 'left
      }
    }

    "MUL_DECIMAL" - {
      "throws exception in case of overflow" in {
        eval(e"MUL_DECIMAL 1.1 2.2") shouldBe Right(SDecimal(2.42))
        eval(e"MUL_DECIMAL $bigBigDecimal $bigBigDecimal") shouldBe 'left
        eval(e"MUL_DECIMAL ${1E13} ${1E14}") shouldBe Right(SDecimal(1E27))
        eval(e"MUL_DECIMAL ${1E14} ${1E14}") shouldBe 'left
      }
    }

    "ROUND_DECIMAL" - {
      "throws an exception if second argument is not between -27 and 10 exclusive" in {
        val testCases = Table("rounding", 100 :: -100 :: List.range(-30, 13): _*)

        forEvery(testCases) { i =>
          eval(e"ROUND_DECIMAL $i $bigBigDecimal") shouldBe (
            if (-27 <= i && i <= 10) 'right else 'left
          )
        }
      }

      "returns proper result" in {
        val d = "8765432109876543210987654321.0987654321"
        val testCases = Table[Long, String, String](
          ("rounding", "decimal", "result"),
          (-27, d, "9000000000000000000000000000.0000000000"),
          (-1, "45.", "40."),
          (-1, "55.", "60."),
          (10, d, d)
        )

        forEvery(testCases) { (rounding, decimal, result) =>
          eval(e"ROUND_DECIMAL $rounding $decimal") shouldBe Right(SDecimal(BigDecimal(result)))
        }
      }
    }

    "Decimal binary operations computes proper results" in {

      val testCases = Table[String, (BigDecimal, BigDecimal) => Either[Any, SValue]](
        ("builtin", "reference"),
        ("ADD_DECIMAL", (a, b) => Decimal.check(a + b).map(SDecimal)),
        ("SUB_DECIMAL", (a, b) => Decimal.check(a - b).map(SDecimal)),
        ("MUL_DECIMAL", (a, b) => Decimal.check(a * b).map(SDecimal)),
        ("DIV_DECIMAL", (a, b) => if (b == 0) Left(()) else Decimal.check(a / b).map(SDecimal)),
        ("LESS_EQ_DECIMAL", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ_DECIMAL", (a, b) => Right(SBool(a >= b))),
        ("LESS_DECIMAL", (a, b) => Right(SBool(a < b))),
        ("GREATER_DECIMAL", (a, b) => Right(SBool(a > b))),
        ("EQUAL_DECIMAL", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(decimals) { a =>
          forEvery(decimals) { b =>
            eval(e"$builtin $a $b").left.map(_ => ()) shouldBe ref(BigDecimal(a), BigDecimal(b))
          }
        }
      }
    }

    "TO_TEXT_DECIMAL" - {
      "returns proper results" in {
        forEvery(decimals) { a =>
          eval(e"TO_TEXT_INT64 $a") shouldBe Right(SText(a))
        }
      }
    }

  }

  "Text operations" - {

    val strings =
      Table("string", "", "aa", "ab", "b", "a¶‱😂", "a¶‱😃", "a", "¶", "‱", "😂", "😃", "｡")

    "EXPLODE_TEXT" - {
      "works on full unicode" in {
        eval(e"""EXPLODE_TEXT "a¶‱😂"""") shouldBe Right(
          SList(
            FrontStack(
              SText("a"),
              SText("¶"),
              SText("‱"),
              SText("😂"),
            )))
      }
    }

    "IMPLODE_TEXT" - {
      "works properly" in {
        eval(e"""IMPLODE_TEXT (Cons @TEXT ["", "", ""] (Nil @TEXT)) """) shouldBe Right(SText(""))
        eval(e"""IMPLODE_TEXT (Cons @TEXT ["a", "¶", "‱", "😂"] (Nil @TEXT)) """) shouldBe
          Right(SText("a¶‱😂"))
        eval(
          e"""IMPLODE_TEXT Cons @TEXT ["IMPLODE_TEXT", " ", "works", " ", "properly"] Nil @TEXT """) shouldBe
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
            |culpa qui officia deserunt mollit anim id est laborum...""".stripMargin ->
            "c045064089460b634bb47e71d2457cd0e8dbc1327aaf9439c275c9796c073620",
          "a¶‱😂" ->
            "8f1cc14a85321115abcd2854e34f9ca004f4f199d367c3c9a84a355f287cec2e"
        )
        forEvery(testCases) { (input, output) =>
          eval(e"""SHA256_TEXT "$input"""") shouldBe Right(SText(output))
        }

      }
    }

    "TEXT_TO_TEXT" - {
      "is idempotent" in {
        forEvery(strings) { s =>
          eval(e""" TO_TEXT_TEXT "$s" """) shouldBe Right(SText(s))
        }
      }
    }

    "Text binary operations computes proper results" in {

      // a naive Unicode ordering for string
      val unicodeOrdering =
        Ordering.by((s: String) => s.codePoints().toArray.toIterable)

      assert(Ordering.String.gt("｡", "😂"))
      assert(unicodeOrdering.lt("｡", "😂"))

      val testCases = Table[String, (String, String) => Either[Any, SValue]](
        ("builtin", "reference"),
        ("APPEND_TEXT", (a, b) => Right(SText(a + b))),
        ("LESS_EQ_TEXT", (a, b) => Right(SBool(unicodeOrdering.lteq(a, b)))),
        ("GREATER_EQ_TEXT", (a, b) => Right(SBool(unicodeOrdering.gteq(a, b)))),
        ("LESS_TEXT", (a, b) => Right(SBool(unicodeOrdering.lt(a, b)))),
        ("GREATER_TEXT", (a, b) => Right(SBool(unicodeOrdering.gt(a, b)))),
        ("EQUAL_TEXT", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(strings) { a =>
          forEvery(strings) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldBe ref(a, b)
          }
        }
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
        "2000-12-31T23:00:00.000000Z")

      val testCases = Table[String, (String, String) => Either[Any, SValue]](
        ("builtin", "reference"),
        ("LESS_EQ_TIMESTAMP", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ_TIMESTAMP", (a, b) => Right(SBool(a >= b))),
        ("LESS_TIMESTAMP", (a, b) => Right(SBool(a < b))),
        ("GREATER_TIMESTAMP", (a, b) => Right(SBool(a > b))),
        ("EQUAL_TIMESTAMP", (a, b) => Right(SBool(a == b))),
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
          eval(e"TO_TEXT_TEXT $s") shouldBe Right(SText(s))
        }
      }
    }

  }

  "Date operations" - {

    "Date comparison operations compute proper results" in {

      // Here lexicographical order of string representation corresponds to chronological order

      val timeStamp = Table[String]("timestamp", "1969-07-21", "1970-01-01", "2001-01-01")

      val testCases = Table[String, (String, String) => Either[Any, SValue]](
        ("builtin", "reference"),
        ("LESS_EQ_DATE", (a, b) => Right(SBool(a <= b))),
        ("GREATER_EQ_DATE", (a, b) => Right(SBool(a >= b))),
        ("LESS_DATE", (a, b) => Right(SBool(a < b))),
        ("GREATER_DATE", (a, b) => Right(SBool(a > b))),
        ("EQUAL_DATE", (a, b) => Right(SBool(a == b))),
      )

      forEvery(testCases) { (builtin, ref) =>
        forEvery(timeStamp) { a =>
          forEvery(timeStamp) { b =>
            eval(e""" $builtin "$a" "$b" """).left.map(_ => ()) shouldBe ref(a, b)
          }
        }
      }
    }

    "TEXT_TO_DATE" - {
      "works as expected" in {
        eval(e"TO_TEXT_TEXT  1879-03-14").left.map(_ => ()) shouldBe Right(SText("1879-03-14"))
      }
    }
  }

  "ContractId operations" - {

    "EQUAL_CONTRACT_ID" - {
      "works as expected" in {
        eval(e"EQUAL_CONTRACT_ID @Mod:T 'contract1' 'contract1'") shouldBe Right(SBool(true))
        eval(e"EQUAL_CONTRACT_ID @Mod:T 'contract1' 'contract2'") shouldBe Right(SBool(false))
      }
    }

  }

  "List operations" - {

    val f = """(\ (x:Int64) (y:Int64) ->  ADD_INT64 3 (MUL_INT64 x y))"""

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
    }

    "EQUAL_LIST" - {
      "works as expected" in {
        val sameParity =
          """(\ (x:Int64) (y:Int64) -> EQUAL_INT64 (MOD_INT64 x 2) (MOD_INT64 y 2))"""

        eval(e"EQUAL_LIST @Int64 $sameParity ${intList()} ${intList()}") shouldBe Right(SBool(true))
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1, 2, 3)} ${intList(5, 6, 7)}") shouldBe Right(
          SBool(true))
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList()} ${intList(1)}") shouldBe Right(
          SBool(false))
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1)} ${intList(1, 2)}") shouldBe Right(
          SBool(false))
        eval(e"EQUAL_LIST @Int64 $sameParity ${intList(1, 2, 3)} ${intList(5, 6, 4)}") shouldBe Right(
          SBool(false))
      }
    }
  }

  "Map operations" - {

    def buildMap[X](typ: String, l: (String, X)*) =
      ("MAP_EMPTY @Int64" /: l) { case (acc, (k, v)) => s"""(MAP_INSERT @$typ "$k" $v $acc)""" }

    "MAP_EMPTY" - {
      "produces a map" in {
        eval(e"MAP_EMPTY @Int64") shouldBe Right(SMap(HashMap.empty))
      }
    }

    "MAP_INSERT" - {

      "inserts as expected" in {
        eval(e"${buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)}") shouldBe
          Right(SMap(HashMap("a" -> SInt64(1), "b" -> SInt64(2), "c" -> SInt64(3))))
      }

      "replaces already present key" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

        eval(e"$map") shouldBe
          Right(SMap(HashMap("a" -> SInt64(1), "b" -> SInt64(2), "c" -> SInt64(3))))
        eval(e"""MAP_INSERT @Int64 "b" 4 $map""") shouldBe Right(
          SMap(HashMap("a" -> SInt64(1), "b" -> SInt64(4), "c" -> SInt64(3))))
      }
    }

    "MAP_LOOKUP" - {
      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "finds existing key" in {
        for {
          x <- List("a" -> 1L, "b" -> 2L, "c" -> 3L)
          (k, v) = x
        } eval(e"""MAP_LOOKUP @Int64 "$k" $map""") shouldBe Right(SOptional(Some(SInt64(v))))
      }
      "not finds non-existing key" in {
        eval(e"""MAP_LOOKUP @Int64 "d" $map""") shouldBe Right(SOptional(None))
      }
    }

    "MAP_DELETE" - {
      val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)

      "deletes existing key" in {
        eval(e"""MAP_DELETE @Int64 "a" $map""") shouldBe Right(
          SMap(HashMap("b" -> SInt64(2), "c" -> SInt64(3))))
        eval(e"""MAP_DELETE @Int64 "b" $map""") shouldBe Right(
          SMap(HashMap("a" -> SInt64(1), "c" -> SInt64(3))))
      }
      "does nothing with non-existing key" in {
        eval(e"""MAP_DELETE @Int64 "d" $map""") shouldBe Right(
          SMap(HashMap("a" -> SInt64(1), "b" -> SInt64(2), "c" -> SInt64(3))))
      }
    }

    "MAP_TO_LIST" - {

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

        eval(e"MAP_TO_LIST @Int64 ${buildMap("Int64", words: _*)}") shouldBe
          Right(
            SList(FrontStack(
              mapEntry("cover", SInt64(8)),
              mapEntry("enter", SInt64(7)),
              mapEntry("favor", SInt64(9)),
              mapEntry("first", SInt64(3)),
              mapEntry("patch", SInt64(4)),
              mapEntry("ranch", SInt64(2)),
              mapEntry("slant", SInt64(0)),
              mapEntry("sweat", SInt64(6)),
              mapEntry("trend", SInt64(5)),
              mapEntry("visit", SInt64(1))
            )))
      }
    }

    "MAP_SIZE" - {
      "returns 0 for empty Map" in {
        eval(e"MAP_SIZE @Int64 (MAP_EMPTY @Int64)") shouldBe Right(SInt64(0))
      }

      "returns the expected size for non-empty Map" in {
        val map = buildMap("Int64", "a" -> 1, "b" -> 2, "c" -> 3)
        eval(e"MAP_SIZE @Int64 $map") shouldBe Right(SInt64(3))
      }
    }

  }

  "Conversion operations" - {

    val almostZero = BigDecimal("0.0000000001")

    "DECIMAL_TO_INT64" - {
      "throws exception in case of overflow" in {
        eval(e"DECIMAL_TO_INT64 ${-BigDecimal(2).pow(63) - 1}") shouldBe 'left
        eval(e"DECIMAL_TO_INT64 ${-BigDecimal(2).pow(63) - 1 + almostZero}") shouldBe Right(
          SInt64(Long.MinValue))
        eval(e"DECIMAL_TO_INT64 ${-BigDecimal(2).pow(63)}") shouldBe Right(SInt64(Long.MinValue))
        eval(e"DECIMAL_TO_INT64 ${BigDecimal(2).pow(63) - 1}") shouldBe Right(SInt64(Long.MaxValue))
        eval(e"DECIMAL_TO_INT64 ${BigDecimal(2).pow(63) - almostZero}") shouldBe Right(
          SInt64(Long.MaxValue))
        eval(e"DECIMAL_TO_INT64 ${BigDecimal(2).pow(63)}") shouldBe 'left
        eval(e"DECIMAL_TO_INT64 1000000000000000000000.0") shouldBe 'left
      }

      "works as expected" in {
        val testCases = Table[BigDecimal, Long](
          "Decimal" -> "Int64",
          almostZero -> 0,
          BigDecimal(0.0) -> 0,
          BigDecimal(1.0) -> 1,
          BigDecimal(1.0000000001) -> 1,
          BigDecimal(1.9999999999) -> 1,
          BigDecimal(123456789.123456789) -> 123456789
        )

        forEvery(testCases) { (decimal, int64) =>
          eval(e"DECIMAL_TO_INT64 $decimal") shouldBe Right(SInt64(int64))
          eval(e"DECIMAL_TO_INT64 ${-decimal}") shouldBe Right(SInt64(-int64))
        }
      }
    }

    "INT64_TO_DECIMAL" - {
      "work as expected" in {
        val testCases = Table[Long]("Int64", 167, 11, 2, 1, 0, -1, -2, -13, -113)

        forEvery(testCases) { int64 =>
          eval(e"INT64_TO_DECIMAL $int64") shouldBe Right(SDecimal(int64))
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
          eval(e"TIMESTAMP_TO_UNIX_MICROSECONDS $timestamp") shouldBe Right(SInt64(int64))
          eval(e"UNIX_MICROSECONDS_TO_TIMESTAMP $int64") shouldBe Right(
            STimestamp(Time.Timestamp.assertFromLong(int64)))
          eval(e"EQUAL_TIMESTAMP (UNIX_MICROSECONDS_TO_TIMESTAMP $int64) $timestamp") shouldBe Right(
            SBool(true))
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
          "2001-01-01" -> 11323
        )

        forEvery(testCases) { (date, int) =>
          eval(e"DATE_TO_UNIX_DAYS $date") shouldBe Right(SInt64(int))
          eval(e"UNIX_DAYS_TO_DATE $int") shouldBe Time.Date
            .asInt(int)
            .map(i => SDate(Time.Date.assertFromDaysSinceEpoch(i)))
          eval(e"EQUAL_DATE (UNIX_DAYS_TO_DATE $int) $date") shouldBe Right(SBool(true))
        }
      }
    }

    "TO_TEXT_PARTY & TO_QUOTED_TEXT_PARTY & FROM_TEXT_PARTY" - {
      "TO_QUOTED_TEXT_PARTY single quotes" in {
        eval(e"TO_QUOTED_TEXT_PARTY 'alice'") shouldBe Right(SText("'alice'"))
      }

      "TO_TEXT_PARTY does not single quote" in {
        eval(e"TO_TEXT_PARTY 'alice'") shouldBe Right(SText("alice"))
      }

      "FROM_TEXT_PARTY" in {
        eval(e"""FROM_TEXT_PARTY "alice" """) shouldBe Right(
          SOptional(Some(SParty(Party.assertFromString("alice")))))
        eval(e"""FROM_TEXT_PARTY "bad%char" """) shouldBe Right(SOptional(None))
      }
    }
  }

  "Debugging builtins" - {

    "TRACE" - {
      "is idempotent" in {
        val testCases = Table[String, SValue](
          "expression" -> "result",
          "1" -> SInt64(1),
          "1." -> SDecimal(1),
          "True" -> SBool(true),
          "()" -> SUnit(()),
          """ "text" """ -> SText("text"),
          " 'party' " -> SParty(SimpleString.assertFromString("party")),
          intList(1, 2, 3) -> SList(FrontStack(SInt64(1), SInt64(2), SInt64(3))),
          " UNIX_DAYS_TO_DATE 1 " -> SDate(Time.Date.assertFromDaysSinceEpoch(1)),
          """ TRACE "another message" (ADD_INT64 1 1)""" -> SInt64(2)
        )

        forEvery(testCases) { (exp, result) =>
          eval(e"""TRACE "message" ($exp)""") shouldBe Right(result)
        }
      }

      "throws an expression if its argument throws one" in {
        eval(e"""TRACE "message" 1""") shouldBe Right(SInt64(1))
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
    val machine = Speedy.Machine.fromExpr(e, PureCompiledPackages(Map.empty).right.get, false)
    while (!machine.isFinal) machine.step() match {
      case SResultContinue => ()
      case SResultError(err) => return Left(err)
      case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
    }

    Right(machine.toSValue())
  }

  def intList(xs: Long*): String =
    if (xs.isEmpty) "(Nil @Int64)"
    else xs.mkString(s"(Cons @Int64 [", ", ", s"] (Nil @Int64))")

  private val entryFields = Array("key", "value")

  private def mapEntry(k: String, v: SValue) = {
    val args = new util.ArrayList[SValue](2)
    args.add(SText(k))
    args.add(v)
    STuple(entryFields, args)
  }
}
