// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.{data => JData}
import com.daml.lf.data.{FrontStack, ImmArray, Numeric, Ref, SortedLookupList, Time}
import com.daml.lf.value.{Value => LfValue}
import scalaz.syntax.std.option._

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import java.util.function.{Function => JFunction}
import java.util.stream.Collectors
import scala.collection.immutable.ListMap

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
        list.toList(toLfValue).asScala.to(FrontStack)
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
        ImmArray.from(
          genMap.stream
            .map(x => toLfValue(x.getKey) -> toLfValue(x.getValue))
            .collect(Collectors.toList[(LfValue, LfValue)])
            .asScala
        )
      )
    case enum: JData.DamlEnum =>
      LfValue.ValueEnum(
        enum.getEnumId.toScala.map(toRefId),
        Ref.Name.assertFromString(enum.getConstructor),
      )
    case int64: JData.Int64 => LfValue.ValueInt64(int64.getValue)
    case numeric: JData.Numeric =>
      LfValue.ValueNumeric(
        Numeric.assertFromBigDecimal(
          Numeric.Scale.assertFromInt(numeric.getValue.scale()),
          numeric.getValue,
        )
      )
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
    case x =>
      throw new IllegalArgumentException(
        s"Unknown value type $x."
      )
  }

  def fromLfValue(lfV: LfValue): JData.Value = lfV match {
    case LfValue.ValueRecord(tycon, fields) =>
      val lfFields = fields.toSeq.map { case (name, value) =>
        name.cata(
          n => new JData.DamlRecord.Field(n, fromLfValue(value)),
          new JData.DamlRecord.Field(fromLfValue(value)),
        )
      }.asJava
      tycon.cata(
        id => new JData.DamlRecord(fromRefId(id), lfFields),
        new JData.DamlRecord(lfFields),
      )
    case LfValue.ValueVariant(tycon, variant, value) =>
      tycon.cata(
        id => new JData.Variant(fromRefId(id), variant, fromLfValue(value)),
        new JData.Variant(variant, fromLfValue(value)),
      )

    case LfValue.ValueContractId(value) => new JData.ContractId(value.coid)
    case LfValue.ValueList(values) =>
      JData.DamlList.of(values.toImmArray.toSeq.map(fromLfValue).asJava)
    case LfValue.ValueOptional(value) =>
      JData.DamlOptional.of(value.map(fromLfValue).toJava)
    case LfValue.ValueTextMap(value) =>
      JData.DamlTextMap.of(value.toHashMap.view.mapValues(fromLfValue).toMap.asJava)
    case LfValue.ValueGenMap(entries) =>
      JData.DamlGenMap.of(
        ListMap(
          entries.toSeq.view.map { case (v1, v2) =>
            fromLfValue(v1) -> fromLfValue(v2)
          }.toSeq: _*
        ).asJava
      )
    case LfValue.ValueEnum(tycon, value) =>
      tycon.cata(
        id => new JData.DamlEnum(fromRefId(id), value),
        new JData.DamlEnum(value),
      )
    case LfValue.ValueInt64(value) => new JData.Int64(value)
    case LfValue.ValueNumeric(value) => new JData.Numeric(value)
    case LfValue.ValueText(value) => new JData.Text(value)
    case LfValue.ValueTimestamp(value) => new JData.Timestamp(value.micros)
    case LfValue.ValueDate(value) => new JData.Date(value.days)
    case LfValue.ValueParty(value) => new JData.Party(value)
    case LfValue.ValueBool(value) => JData.Bool.of(value)
    case LfValue.ValueUnit => JData.Unit.getInstance
  }

  private def toRefId(jid: JData.Identifier): Ref.Identifier = Ref.Identifier(
    Ref.PackageId.assertFromString(jid.getPackageId),
    Ref.QualifiedName(
      Ref.DottedName.assertFromString(jid.getModuleName),
      Ref.DottedName.assertFromString(jid.getEntityName),
    ),
  )

  private def fromRefId(id: Ref.Identifier): JData.Identifier = new JData.Identifier(
    id.packageId,
    id.qualifiedName.module.dottedName,
    id.qualifiedName.name.dottedName,
  )
}
