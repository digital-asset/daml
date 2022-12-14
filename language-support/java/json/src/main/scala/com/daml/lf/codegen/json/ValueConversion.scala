// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.data.Identifier
import com.daml.ledger.javaapi.{data => JData}
import com.daml.lf.data.{FrontStack, Numeric, ImmArray, Ref, SortedLookupList, Time}
import com.daml.lf.value.{Value => LfValue}

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import java.util.function.{Function => JFunction}

object ValueConversion {
  def toLfValue(v: JData.Value): LfValue = v match {
    case record: JData.DamlRecord =>
      LfValue.ValueRecord(
        record.getRecordId.toScala.map(toRefId),
        ImmArray.from(
          record.getFields.asScala.map(f =>
            f.getLabel.toScala.map(Ref.Name.assertFromString) ->
              toLfValue(f.getValue)
          )
        ),
      )
    case variant: JData.Variant =>
      LfValue.ValueVariant(
        variant.getVariantId.toScala.map(toRefId),
        Ref.Name.assertFromString(variant.getConstructor),
        toLfValue(variant.getValue),
      )
    case cid: JData.ContractId =>
      LfValue.ValueContractId(
        LfValue.ContractId.assertFromString(cid.getValue)
      )
    case list: JData.DamlList =>
      LfValue.ValueList(
        FrontStack.from(list.getValues.asScala.map(toLfValue))
      )
    case optional: JData.DamlOptional =>
      LfValue.ValueOptional(
        optional.getValue.toScala.map(toLfValue)
      )
    case textMap: JData.DamlTextMap =>
      LfValue.ValueTextMap(
        SortedLookupList(textMap.toMap(JFunction.identity(), toLfValue).asScala.toMap)
      )
    case genMap: JData.DamlGenMap =>
      LfValue.ValueGenMap(
        ImmArray.from(genMap.toMap(toLfValue, toLfValue).asScala)
      )
    case enum: JData.DamlEnum =>
      LfValue.ValueEnum(
        enum.getEnumId.toScala.map(toRefId),
        Ref.Name.assertFromString(enum.getConstructor),
      )
    case int64: JData.Int64 => LfValue.ValueInt64(int64.getValue)
    case numeric: JData.Numeric =>
      LfValue.ValueNumeric(Numeric.assertFromUnscaledBigDecimal(numeric.getValue))
    case text: JData.Text => LfValue.ValueText(text.getValue)
    case timestamp: JData.Timestamp =>
      LfValue.ValueTimestamp(
        Time.Timestamp.assertFromInstant(timestamp.getValue)
      )
    case date: JData.Date =>
      LfValue.ValueDate(Time.Date.assertFromDaysSinceEpoch(date.getValue.toEpochDay.toInt))
    case party: JData.Party => LfValue.ValueParty(Ref.Party.assertFromString(party.getValue))
    case bool: JData.Bool => LfValue.ValueBool(bool.getValue)
    case _: JData.Unit => LfValue.ValueUnit
  }

  def fromLfValue(lfV: LfValue): JData.Value = lfV match {
    case LfValue.ValueRecord(tycon, fields) => ???
    case LfValue.ValueVariant(tycon, variant, value) => ???
    case LfValue.ValueContractId(value) => ???
    case LfValue.ValueList(values) => ???
    case LfValue.ValueOptional(value) => ???
    case LfValue.ValueTextMap(value) => ???
    case LfValue.ValueGenMap(entries) => ???
    case LfValue.ValueEnum(tycon, value) => ???
    case LfValue.ValueInt64(value) => ???
    case LfValue.ValueNumeric(value) => ???
    case LfValue.ValueText(value) => ???
    case LfValue.ValueTimestamp(value) => ???
    case LfValue.ValueDate(value) => ???
    case LfValue.ValueParty(value) => ???
    case LfValue.ValueBool(value) => ???
    case LfValue.ValueUnit => JData.Unit
  }

  private def toRefId(jid: Identifier): Ref.Identifier = {
    Ref.Identifier(
      Ref.PackageId.assertFromString(jid.getPackageId),
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(jid.getModuleName),
        Ref.DottedName.assertFromString(jid.getEntityName),
      ),
    )
  }
}
