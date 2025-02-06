// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.daml.ledger.api.v2.value
import com.daml.ledger.api.v2.value.Value.Sum
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.schema.DynamicValue.*
import com.google.protobuf.empty.Empty

/** This codec converts Ledger API GRPC values. */
object GrpcValueCodec extends SchemaVisitor {
  type Type = Codec[value.Value]

  override def record(
      id: Identifier,
      appliedArgs: => Seq[(TypeVarName, Type)],
      fields: => Seq[(FieldName, Type)],
  ): Type = new Type {
    private lazy val codecs = fields.map(_._2)

    override def fromDynamicValue(dv: DynamicValue): value.Value = {
      val fs = dv.record.iterator zip codecs map { case (f, c) =>
        value.RecordField(
          label = f._1.getOrElse(""),
          value = Some(c.fromDynamicValue(f._2)),
        )
      }
      value.Value(Sum.Record(value.Record(fields = fs.toSeq)))
    }

    override def toDynamicValue(a: value.Value): DynamicValue =
      DynamicValue.Record(
        a.getRecord.fields.view zip codecs map { case (f, c) =>
          if (f.label.isEmpty) {
            (Option.empty[String], c.toDynamicValue(f.getValue))
          } else {
            (Some(f.label), c.toDynamicValue(f.getValue))
          }
        }
      )
  }

  override def variant(
      id: Identifier,
      appliedArgs: => Seq[(TypeVarName, Type)],
      cases: => Seq[(VariantConName, Type)],
  ): Type = new Type {
    private lazy val caseList = cases.map { case (n, c) => (n.variantConName, c) }.toArray
    private lazy val caseMap = cases.zipWithIndex.map { case ((n, c), ix) =>
      n.variantConName -> (ix, c)
    }.toMap

    override def fromDynamicValue(dv: DynamicValue): value.Value = {
      val (ctor, valueCodec) = caseList(dv.variant.ctorIx)
      value.Value(
        Sum.Variant(
          value.Variant(
            constructor = ctor,
            value = Some(valueCodec.fromDynamicValue(dv.variant.value)),
          )
        )
      )
    }

    override def toDynamicValue(a: value.Value): DynamicValue = {
      val (ctor, valueCodec) = caseMap(a.getVariant.constructor)
      DynamicValue.Variant(ctor, valueCodec.toDynamicValue(a.getVariant.getValue))
    }
  }

  override def `enum`(id: Identifier, cases: Seq[EnumConName]): Type = new Type {
    private val caseList = cases.map(_.enumConName).toArray
    private val caseMap = cases.map(_.enumConName).zipWithIndex.toMap

    override def toDynamicValue(a: value.Value): DynamicValue =
      DynamicValue.Enum(caseMap(a.getEnum.constructor))

    override def fromDynamicValue(dv: DynamicValue): value.Value =
      value.Value(Sum.Enum(value.Enum(constructor = caseList(dv.`enum`))))
  }

  override def list(elem: Type): Type = new Type {
    override def fromDynamicValue(dv: DynamicValue): value.Value =
      value.Value(Sum.List(value.List(dv.list.iterator.map(elem.fromDynamicValue).toSeq)))

    override def toDynamicValue(a: value.Value): DynamicValue =
      DynamicValue.List(a.getList.elements.view.map(elem.toDynamicValue))
  }

  override def optional(elem: Type): Type = new Type {
    override def fromDynamicValue(dv: DynamicValue): value.Value =
      value.Value(Sum.Optional(value.Optional(value = dv.optional.map(elem.fromDynamicValue))))

    override def toDynamicValue(a: value.Value): DynamicValue =
      DynamicValue.Optional(a.getOptional.value.map(elem.toDynamicValue))
  }

  override def textMap(valueCodec: Type): Type = new Type {
    override def fromDynamicValue(dv: DynamicValue): value.Value = {
      val entries = dv.textMap.iterator.map { case (k, v) =>
        value.TextMap.Entry(k, Some(valueCodec.fromDynamicValue(v)))
      }
      value.Value(Sum.TextMap(value.TextMap(entries = entries.toSeq)))
    }

    override def toDynamicValue(a: value.Value): DynamicValue =
      DynamicValue.TextMap(
        a.getTextMap.entries.view.map(e => e.key -> valueCodec.toDynamicValue(e.getValue))
      )
  }

  override def genMap(keyCodec: Type, valueCodec: Type): Type = new Type {
    override def fromDynamicValue(dv: DynamicValue): value.Value = {
      val entries = dv.genMap.iterator.map { case (k, v) =>
        value.GenMap.Entry(Some(keyCodec.fromDynamicValue(k)), Some(valueCodec.fromDynamicValue(v)))
      }
      value.Value(Sum.GenMap(value.GenMap(entries = entries.toSeq)))
    }

    override def toDynamicValue(a: value.Value): DynamicValue = {
      val entries =
        a.getGenMap.entries.view.map(e =>
          keyCodec.toDynamicValue(e.getKey) -> valueCodec.toDynamicValue(e.getValue)
        )
      DynamicValue.GenMap(entries)
    }
  }

  override def unit: Type =
    primitive(_ => Sum.Unit(Empty()), _ => DynamicValue.Unit)

  override def int64: Type =
    primitive(v => Sum.Int64(v.int64), v => DynamicValue.Int64(v.getInt64))

  override def numeric(scale: Int): Type =
    primitive(v => Sum.Numeric(v.numeric), v => DynamicValue.Numeric(v.getNumeric))

  override def text: Type =
    primitive(v => Sum.Text(v.text), v => DynamicValue.Text(v.getText))

  override def timestamp: Type =
    primitive(v => Sum.Timestamp(v.timestamp), v => DynamicValue.Timestamp(v.getTimestamp))

  override def party: Type =
    primitive(v => Sum.Party(v.party), v => DynamicValue.Party(v.getParty))

  override def bool: Type =
    primitive(v => Sum.Bool(v.bool), v => DynamicValue.Bool(v.getBool))

  override def date: Type =
    primitive(v => Sum.Date(v.date), v => DynamicValue.Date(v.getDate))

  override def contractId(template: Type): Type =
    primitive(v => Sum.ContractId(v.contractId), v => DynamicValue.ContractId(v.getContractId))

  override def interface(id: Identifier): Type = unit

  override def variable(name: TypeVarName, value: Type): Type = value

  private def primitive(f: DynamicValue => Sum, g: value.Value => DynamicValue): Type = new Type {
    override def fromDynamicValue(dv: DynamicValue): value.Value = value.Value(f(dv))

    override def toDynamicValue(a: value.Value): DynamicValue = g(a)
  }
}
