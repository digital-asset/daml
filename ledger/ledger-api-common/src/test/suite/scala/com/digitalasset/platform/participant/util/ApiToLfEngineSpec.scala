// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.participant.util

import com.digitalasset.daml.lf.data.{ImmArray, Ref, SortedLookupList}
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.value.Value.{ValueDecimal, ValueInt64, ValueMap, ValueRecord}
import com.digitalasset.ledger.api.domain.Value._
import com.digitalasset.ledger.api.domain._
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.ledger.api.v1.value.{
  Identifier => ApiIdentifier,
  List => ApiList,
  Record => ApiRecord,
  RecordField => ApiRecordField,
  Value => ApiValue,
  Variant => ApiVariant
}
import com.digitalasset.platform.participant.util.ApiToLfEngine.ApiToLfResult.{Done, Error}

import scala.collection.immutable

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ApiToLfEngineSpec extends WordSpec with Matchers {

  val fakeDef: Ast.Definition =
    Ast.DDataType(true, ImmArray.empty, Ast.DataRecord(ImmArray.empty, None))

  def paramShowcaseArgs(verbose: Boolean) = {

    def verboseString(s: String) = if (verbose) Some(Label(s)) else None

    val variantId =
      if (verbose)
        Some(
          Ref.Identifier(
            Ref.PackageId.assertFromString("v"),
            Ref.QualifiedName.assertFromString("module:variantId")))
      else None
    val recordId =
      if (verbose)
        Some(
          Ref.Identifier(
            Ref.PackageId.assertFromString("r"),
            Ref.QualifiedName.assertFromString("module:recordId")))
      else None
    val variant = VariantValue(variantId, VariantConstructor("SomeInteger"), Int64Value(1))

    val nestedVariant =
      RecordValue(recordId, immutable.Seq(RecordField(verboseString("value"), variant)))

    val values = immutable.Seq(1L, 2L).map(i => Int64Value(i))
    val integerList = ListValue(values)

    RecordValue(
      if (verbose)
        Some(
          Ref.Identifier(
            Ref.PackageId.assertFromString("templateIds"),
            Ref.QualifiedName.assertFromString("parameterShowcase:parameterShowcase")))
      else None,
      immutable.Seq(
        RecordField(verboseString("operator"), PartyValue(Ref.Party.assertFromString("party"))),
        RecordField(verboseString("integer"), Int64Value(1)),
        RecordField(verboseString("decimal"), DecimalValue("1.1")),
        RecordField(verboseString("text"), TextValue("text")),
        RecordField(verboseString("bool"), BoolValue(true)),
        RecordField(verboseString("time"), TimeStampValue(0)),
        RecordField(verboseString("nestedOptionalInteger"), nestedVariant),
        RecordField(verboseString("integerListRecordLabel"), integerList)
      )
    )

  }

  def paramShowcaseArgsApi(verbose: Boolean) = {

    def verboseString(s: String) = if (verbose) s else ""
    val variantId = if (verbose) Some(ApiIdentifier("v", "variantId")) else None
    val recordId = if (verbose) Some(ApiIdentifier("r", "recordId")) else None
    val variant = ApiValue(
      ApiValue.Sum.Variant(
        ApiVariant(variantId, "SomeInteger", Some(ApiValue(ApiValue.Sum.Int64(1))))))
    val nestedVariant = ApiValue(
      ApiValue.Sum.Record(
        ApiRecord(recordId, Seq(ApiRecordField(verboseString("value"), Some(variant))))))
    val values = Seq(1L, 2L).map(i => ApiValue(ApiValue.Sum.Int64(i)))
    val integerList = Some(ApiValue(ApiValue.Sum.List(ApiList(values.toList))))

    ApiValue(
      ApiValue.Sum.Record(ApiRecord(
        if (verbose) Some(ApiIdentifier("templateIds", "parameterShowcase", "parameterShowcase"))
        else None,
        Vector(
          ApiRecordField(verboseString("operator"), Some(ApiValue(ApiValue.Sum.Party("party")))),
          ApiRecordField(verboseString("integer"), Some(ApiValue(ApiValue.Sum.Int64(1)))),
          ApiRecordField(verboseString("decimal"), Some(ApiValue(ApiValue.Sum.Decimal("1.1")))),
          ApiRecordField(verboseString("text"), Some(ApiValue(ApiValue.Sum.Text("text")))),
          ApiRecordField(verboseString("bool"), Some(ApiValue(ApiValue.Sum.Bool(true)))),
          ApiRecordField(verboseString("time"), Some(ApiValue(ApiValue.Sum.Timestamp(0)))),
          ApiRecordField(verboseString("nestedOptionalInteger"), Some(nestedVariant)),
          ApiRecordField(verboseString("integerListRecordLabel"), integerList)
        )
      )))

  }

  "conversion of values" should {
    "convert of instant to timestamp and back" in {
      val t0 = java.time.Instant.now()
      ApiToLfEngine.toInstant(LfEngineToApi.toTimestamp(t0)) shouldEqual t0
    }

    "transform back and forth in non-verbose mode non verbose value" in {
      val Right(lfValue) = ApiToLfEngine
        .apiValueToLfValue(paramShowcaseArgs(false))
        .consume(_ => Some(Ast.Package(Map.empty[Ref.ModuleName, Ast.Module])))
      LfEngineToApi.lfValueToApiValue(false, lfValue) shouldEqual Right(paramShowcaseArgsApi(false))
    }

    "turns empty record labels into Nothing" in {
      val Right(apiVal) = ApiToLfEngine
        .apiValueToLfValue(RecordValue(None, Vector(RecordField(None, Int64Value(42)))))
        .consume(_ => Some(Ast.Package(Map.empty[Ref.ModuleName, Ast.Module])))
      apiVal shouldBe ValueRecord(None, ImmArray((None, ValueInt64(42))))
    }

    "outputs map keys in order" in {
      val Right(lfValue) = ApiToLfEngine
        .apiValueToLfValue(paramShowcaseArgs(false))
        .consume(_ => Some(Ast.Package(Map.empty[Ref.ModuleName, Ast.Module])))

      val testCases = List(
        List.empty,
        List("1" -> ValueInt64(1)),
        List("1" -> ValueInt64(1), "2" -> ValueInt64(2)),
        List(
          "1" -> ValueInt64(1),
          "2" -> ValueInt64(2),
          "3" -> ValueInt64(4),
          "4" -> ValueInt64(4),
          "5" -> ValueInt64(5))
      )

      testCases.foreach { list =>
        val Right(apiMap) =
          LfEngineToApi.lfValueToApiValue(
            false,
            ValueMap(SortedLookupList.fromImmArray(ImmArray(list)).right.get))
        apiMap.getMap.entries.toList.map(_.key) shouldBe list.map(_._1)
      }
    }

    "handle Decimals exceeding scale correctly" in {
      ApiToLfEngine.apiValueToLfValue(DecimalValue("0.0000000001")) shouldBe Done(
        ValueDecimal(BigDecimal("0.0000000001")))
      ApiToLfEngine.apiValueToLfValue(DecimalValue("0.00000000005")) shouldBe a[Error[_]]
    }

    "handle Decimals exceeding bounds" in {
      ApiToLfEngine.apiValueToLfValue(DecimalValue("10000000000000000000000000000.0000000000")) shouldBe
        a[Error[_]]
      ApiToLfEngine.apiValueToLfValue(DecimalValue("-10000000000000000000000000000.0000000000")) shouldBe
        a[Error[_]]
    }
  }
}
