// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.value as api
import com.daml.ledger.api.v2.value.Value.Sum
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.ledger.api.validation.{ValidatorTestUtils, ValueValidator}
import com.digitalasset.canton.logging.NoLogging
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.empty.Empty
import org.mockito.MockitoSugar
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import org.scalatest.wordspec.AnyWordSpec

class ValueConversionRoundTripTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks
    with MockitoSugar {

  private val recordId =
    api.Identifier(packageId, moduleName = "Mod", entityName = "Record")

  private val constructor: String = "constructor"

  private def roundTrip(v: api.Value): Either[String, api.Value] =
    for {
      lfValue <- ValueValidator.validateValue(v)(NoLogging).left.map(_.getMessage)
      apiValue <- LfEngineToApi.lfValueToApiValue(true, lfValue)
    } yield apiValue

  "round trip" should {
    "be idempotent on value that do not contain non empty text maps, nor signed decimals" in {

      val testCases: TableFor1[Sum] = Table(
        "values",
        Sum.ContractId(ContractId.V1(Hash.hashPrivateKey("#coid")).coid),
        ApiMocks.values.validApiParty.sum,
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
        Sum.List(api.List(List.empty)),
        Sum.List(api.List((0 to 10).map(i => api.Value(Sum.Int64(i.toLong))))),
        Sum.Optional(api.Optional(None)),
        Sum.Optional(api.Optional(Some(ApiMocks.values.validApiParty))),
        Sum.TextMap(api.TextMap(List.empty)),
        Sum.GenMap(api.GenMap(List.empty)),
        Sum.GenMap(
          api.GenMap(
            List(
              api.GenMap.Entry(Some(api.Value(Sum.Text("key1"))), Some(api.Value(Sum.Int64(1)))),
              api.GenMap.Entry(Some(api.Value(Sum.Text("key3"))), Some(api.Value(Sum.Int64(3)))),
              api.GenMap.Entry(Some(api.Value(Sum.Text("key2"))), Some(api.Value(Sum.Int64(2)))),
              api.GenMap.Entry(Some(api.Value(Sum.Text("key1"))), Some(api.Value(Sum.Int64(0)))),
            )
          )
        ),
        Sum.Record(
          api.Record(
            Some(recordId),
            Seq(
              api.RecordField("label1", Some(api.Value(Sum.Int64(1)))),
              api.RecordField("label2", Some(api.Value(Sum.Int64(2)))),
              api.RecordField("label0", Some(api.Value(Sum.Int64(3)))),
            ),
          )
        ),
        Sum.Variant(
          api.Variant(Some(recordId), constructor, Some(ApiMocks.values.validApiParty))
        ),
      )

      forEvery(testCases) { testCase =>
        val input = api.Value(testCase)
        roundTrip(input) shouldEqual Right(input)
      }
    }

    "should sort the entries of a map" in {
      val entries = List("‱", "1", "😂", "😃", "a").zipWithIndex.map { case (k, v) =>
        api.TextMap.Entry(k, Some(api.Value(Sum.Int64(v.toLong))))
      }
      val sortedEntries = entries.sortBy(_.key)

      // just to be sure we did not write the entries sorted
      assert(entries != sortedEntries)

      val input = api.Value(Sum.TextMap(api.TextMap(entries)))
      val expected = api.Value(Sum.TextMap(api.TextMap(sortedEntries)))

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
        ("0" * 10 + "42." + "0" * 10) -> "42.",
      )

      roundTrip(api.Value(Sum.Numeric("0"))) shouldNot equal(api.Value(Sum.Numeric("0")))
      roundTrip(api.Value(Sum.Numeric("+1.0"))) shouldNot equal(api.Value(Sum.Numeric("+1.0")))

      forEvery(testCases) { case (input, expected) =>
        roundTrip(api.Value(Sum.Numeric(input))) shouldEqual Right(api.Value(Sum.Numeric(expected)))
        roundTrip(api.Value(Sum.Numeric("+" + input))) shouldEqual Right(
          api.Value(Sum.Numeric(expected))
        )
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
        ("-" + "0" * 10 + "42." + "0" * 10) -> "-42.",
      )

      roundTrip(api.Value(Sum.Numeric("-0"))) shouldNot equal(api.Value(Sum.Numeric("-0")))

      forEvery(testCases) { case (input, expected) =>
        roundTrip(api.Value(Sum.Numeric(input))) shouldEqual Right(api.Value(Sum.Numeric(expected)))
      }
    }

    "drop trailing Nones in LF -> API conversion" in {
      def intValue(valO: Option[Long]): Option[api.Value] =
        Some(
          api.Value(Sum.Optional(api.Optional(valO.map(v => api.Value(api.Value.Sum.Int64(v))))))
        )
      val testCases = Table[Seq[api.RecordField], Seq[api.RecordField]](
        "input" -> "expected",
        // Single None dropped
        Seq(
          api.RecordField("label1", intValue(None))
        ) -> Seq(),
        // Single Some kept
        Seq(
          api.RecordField("label1", intValue(Some(1L)))
        ) -> Seq(
          api.RecordField("label1", intValue(Some(1L)))
        ),
        // None in head position kept
        Seq(
          api.RecordField("label1", intValue(None)),
          api.RecordField("label2", intValue(Some(1L))),
        ) -> Seq(
          api.RecordField("label1", intValue(None)),
          api.RecordField("label2", intValue(Some(1L))),
        ),
        // None in tail position dropped
        Seq(
          api.RecordField("label1", intValue(None)),
          api.RecordField("label2", intValue(Some(1L))),
          api.RecordField("label3", intValue(None)),
        ) -> Seq(
          api.RecordField("label1", intValue(None)),
          api.RecordField("label2", intValue(Some(1L))),
        ),
        // Two nones in tail position dropped
        Seq(
          api.RecordField("label1", intValue(Some(1L))),
          api.RecordField("label2", intValue(None)),
          api.RecordField("label3", intValue(Some(3L))),
          api.RecordField("label4", intValue(None)),
          api.RecordField("label5", intValue(None)),
        ) -> Seq(
          api.RecordField("label1", intValue(Some(1L))),
          api.RecordField("label2", intValue(None)),
          api.RecordField("label3", intValue(Some(3L))),
        ),
        // Trailing None in nested record dropped
        Seq(
          api.RecordField(
            "label1",
            Some(
              api.Value(
                Sum.Record(
                  api.Record(
                    Some(recordId.copy(entityName = "nestedEntity")),
                    Seq(
                      api.RecordField("nestedLabel1", intValue(Some(1L))),
                      api.RecordField("nestedLabel2", intValue(None)),
                    ),
                  )
                )
              )
            ),
          )
        ) -> Seq(
          api.RecordField(
            "label1",
            Some(
              api.Value(
                Sum.Record(
                  api.Record(
                    Some(recordId.copy(entityName = "nestedEntity")),
                    Seq(
                      api.RecordField("nestedLabel1", intValue(Some(1L)))
                    ),
                  )
                )
              )
            ),
          )
        ),
      )

      forEvery(testCases) { case (inputVals, expectedVals) =>
        val input = api.Value(Sum.Record(api.Record(Some(recordId), inputVals)))
        val expected = api.Value(Sum.Record(api.Record(Some(recordId), expectedVals)))

        roundTrip(input) shouldEqual Right(expected)
        roundTrip(expected) shouldEqual Right(expected)
      }
    }
  }
}
