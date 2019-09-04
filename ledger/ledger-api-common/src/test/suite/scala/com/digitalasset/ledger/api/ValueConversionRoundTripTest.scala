// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api

import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{
  List => ApiList,
  Map => ApiMap,
  Optional => ApiOptional,
  _
}
import com.digitalasset.ledger.api.validation.{ValueValidator, ValidatorTestUtils}
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.google.protobuf.empty.Empty
import org.scalatest.WordSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}

class ValueConversionRoundTripTest
    extends WordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks {

  private val recordId =
    Identifier(packageId, moduleName = "Mod", entityName = "Record")
  private val emptyRecordId =
    Identifier(packageId, moduleName = "Mod", entityName = "EmptyRecord")
  private val variantId =
    Identifier(packageId, moduleName = "Mod", entityName = "Variant")

  private val label: String = "label"

  private def record(values: Value*): Value =
    Value(Sum.Record(Record(Some(recordId), values.zipWithIndex.map {
      case (v, i) => RecordField(label + "1")
    })))

  private val constructor: String = "constructor"

  private def variant(value: Value): Value =
    Value(Sum.Variant(Variant(Some(recordId), constructor, Some(value))))

  private val pkg = p"""
         module Mod {
           record EmptyRecord = {};
           record Record = { label1: Int64, label2: Int64, label0: Int64 };
           variant Variant = constructor: Unit;
         }
         """

  private def roundTrip(v: Value): Either[String, Value] =
    for {
      lfValue <- ValueValidator.validateValue(v).left.map(_.getMessage)
      apiValue <- LfEngineToApi.lfValueToApiValue(true, lfValue)
    } yield apiValue

  "round trip" should {
    "be idempotent on value that do not contain non empty maps, nor signed decimals" in {

      val testCases: TableFor1[Sum] = Table(
        "values",
        Sum.ContractId("coid"),
        DomainMocks.values.validApiParty.sum,
        Sum.Int64(Long.MinValue),
        Sum.Int64(0),
        Sum.Int64(Long.MaxValue),
        Sum.Text("string"),
        Sum.Text(""),
        Sum.Text("a ¶ ‱ 😂 😃"),
        Sum.Timestamp(Time.Timestamp.MinValue.micros),
        Sum.Timestamp(0),
        Sum.Timestamp(Time.Timestamp.MaxValue.micros),
        Sum.Date(Time.Date.MinValue.days),
        Sum.Date(0),
        Sum.Date(Time.Date.MaxValue.days),
        Sum.Bool(true),
        Sum.Bool(false),
        Sum.Unit(Empty()),
        Sum.List(ApiList(List.empty)),
        Sum.List(ApiList((0 to 10).map(i => Value(Sum.Int64(i.toLong))))),
        Sum.Optional(ApiOptional(None)),
        Sum.Optional(ApiOptional(Some(DomainMocks.values.validApiParty))),
        Sum.Map(ApiMap(List.empty)),
        Sum.Record(
          Record(
            Some(recordId),
            Seq(
              RecordField("label1", Some(Value(Sum.Int64(1)))),
              RecordField("label2", Some(Value(Sum.Int64(2)))),
              RecordField("label0", Some(Value(Sum.Int64(3)))),
            )
          )),
        Sum.Variant(Variant(Some(recordId), constructor, Some(DomainMocks.values.validApiParty)))
      )

      forEvery(testCases) { testCase =>
        val input = Value(testCase)
        roundTrip(input) shouldEqual Right(input)
      }
    }

    "should sort the entries of a map" in {
      val entries = List("‱", "1", "😂", "😃", "a").zipWithIndex.map {
        case (k, v) => ApiMap.Entry(k, Some(Value(Sum.Int64(v.toLong))))
      }
      val sortedEntries = entries.sortBy(_.key)

      // just to be sure we did not write the entries sorted
      assert(entries != sortedEntries)

      val input = Value(Sum.Map(ApiMap(entries)))
      val expected = Value(Sum.Map(ApiMap(sortedEntries)))

      roundTrip(input) shouldNot equal(Right(input))
      roundTrip(input) shouldEqual Right(expected)
    }

    "should write the positive decimal in canonical form" in {

      val testCases = Table(
        "input/output",
        "0" -> "0.",
        "0.0" -> "0.",
        "3.1415926536" -> "3.1415926536",
        ("1" + "0" * 27) -> ("1" + "0" * 27 + "."),
        ("1" + "0" * 27 + "." + "0" * 9 + "1") -> ("1" + "0" * 27 + "." + "0" * 9 + "1"),
        ("0." + "0" * 9 + "1") -> ("0." + "0" * 9 + "1"),
        ("0" * 10 + "42") -> "42.",
        ("0" * 10 + "42." + "0" * 10) -> "42."
      )

      roundTrip(Value(Sum.Numeric("0"))) shouldNot equal(Value(Sum.Numeric("0")))
      roundTrip(Value(Sum.Numeric("+1.0"))) shouldNot equal(Value(Sum.Numeric("+1.0")))

      forEvery(testCases) {
        case (input, expected) =>
          roundTrip(Value(Sum.Numeric(input))) shouldEqual Right(Value(Sum.Numeric(expected)))
          roundTrip(Value(Sum.Numeric("+" + input))) shouldEqual Right(Value(Sum.Numeric(expected)))
      }
    }

    "should write the negative decimal in canonical form" in {

      val testCases = Table(
        "input/output",
        "-0" -> "0.",
        "-0.0" -> "0.",
        "-3.1415926536" -> "-3.1415926536",
        ("-1" + "0" * 27) -> ("-1" + "0" * 27 + "."),
        ("-1" + "0" * 27 + "." + "0" * 9 + "1") -> ("-1" + "0" * 27 + "." + "0" * 9 + "1"),
        ("-0." + "0" * 9 + "1") -> ("-0." + "0" * 9 + "1"),
        ("-" + "0" * 10 + "42") -> "-42.",
        ("-" + "0" * 10 + "42." + "0" * 10) -> "-42."
      )

      roundTrip(Value(Sum.Numeric("-0"))) shouldNot equal(Value(Sum.Numeric("-0")))

      forEvery(testCases) {
        case (input, expected) =>
          roundTrip(Value(Sum.Numeric(input))) shouldEqual Right(Value(Sum.Numeric(expected)))
      }
    }
  }

}
