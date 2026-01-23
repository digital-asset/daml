// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.digitalasset.transcode.codec.proto.Value.Sum
import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.schema.CodecVisitor.*
import com.google.protobuf.empty.Empty

/** This codec converts Ledger API GRPC values. */
object ProtobufCodec extends CodecVisitor[Value]:
  override def record(fields: Seq[(FieldName, Type)]): Type = new Type:
    private val codecs = fields.map(_._2)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val _ = dv.checkRecordSize(fields.size)
      val fs =
        dv.recordIterator.zip(fields).map { case (f, (name, c)) =>
          RecordField(label = name, value = Some(c.fromDynamicValue(f)))
        }
      Value(Sum.Record(Record(fields = fs.toSeq)))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val record = a.sum.record.getOrElse(unexpectedCase(a))
      DynamicValue.Record(
        record.fields.view zip codecs map ((f, c) => c.toDynamicValue(f.getValue))
      )
  end record

  override def variant(cases: Seq[(VariantConName, Type)]): Type = new Type:
    private val caseList = cases.map((n, c) => (n.variantConName, c)).toArray
    private val caseMap = cases.zipWithIndex.map { case ((n, c), ix) =>
      n.variantConName -> (ix, c)
    }.toMap
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val (ctorIx, v) = dv.variant
      val (ctor, valueCodec) = caseList.getMaybe(ctorIx)
      Value(Sum.Variant(Variant(constructor = ctor, value = Some(valueCodec.fromDynamicValue(v)))))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val variant = a.sum.variant.getOrElse(unexpectedCase(a))
      val (ctor, valueCodec) = caseMap.getMaybe(variant.constructor)
      DynamicValue.Variant(ctor, valueCodec.toDynamicValue(variant.getValue))
  end variant

  override def enumeration(cases: Seq[EnumConName]): Type = new Type:
    private val caseList = cases.map(_.enumConName).toArray
    private val caseMap = cases.map(_.enumConName).zipWithIndex.toMap
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Value(Sum.Enum(Enum(constructor = caseList.getMaybe(dv.enumeration))))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val enumeration = a.sum.`enum`.getOrElse(unexpectedCase(a))
      DynamicValue.Enumeration(caseMap.getMaybe(enumeration.constructor))
  end enumeration

  override def list(elem: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Value(Sum.List(List(dv.list.iterator.map(elem.fromDynamicValue).toSeq)))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val list = a.sum.list.getOrElse(unexpectedCase(a))
      DynamicValue.List(list.elements.view.map(elem.toDynamicValue))
  end list

  override def optional(elem: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Value(Sum.Optional(Optional(value = dv.optional.map(elem.fromDynamicValue))))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val optional = a.sum.optional.getOrElse(unexpectedCase(a))
      DynamicValue.Optional(optional.value.map(elem.toDynamicValue))
  end optional

  override def textMap(valueCodec: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Value(mkTextMap(dv.textMap.iterator.map((k, v) => (k, valueCodec.fromDynamicValue(v)))))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val textMap = a.`textMap!`.getOrElse(unexpectedCase(a))
      DynamicValue.TextMap(
        textMap.entries.view.map(e => e.key -> valueCodec.toDynamicValue(e.getValue))
      )
  end textMap

  override def genMap(keyCodec: Type, valueCodec: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val entries = dv.genMap.iterator.map((k, v) =>
        GenMap.Entry(Some(keyCodec.fromDynamicValue(k)), Some(valueCodec.fromDynamicValue(v)))
      )
      Value(Sum.GenMap(GenMap(entries = entries.toSeq)))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val genMap = a.sum.genMap.getOrElse(unexpectedCase(a))
      val entries =
        genMap.entries.view.map(e =>
          keyCodec.toDynamicValue(e.getKey) -> valueCodec.toDynamicValue(e.getValue)
        )
      DynamicValue.GenMap(entries)
  end genMap

  override def unit: Type =
    primitive(
      _ => Sum.Unit(Empty()),
      v => v.sum.unit.map(_ => DynamicValue.Unit).getOrElse(unexpectedCase(v)),
    )
  override def int64: Type =
    primitive(
      v => Sum.Int64(v.int64),
      v => DynamicValue.Int64(v.sum.int64.getOrElse(unexpectedCase(v))),
    )
  override def numeric(scale: Int): Type =
    primitive(
      v => Sum.Numeric(v.numeric),
      v => DynamicValue.Numeric(v.sum.numeric.getOrElse(unexpectedCase(v))),
    )
  override def text: Type =
    primitive(
      v => Sum.Text(v.text),
      v => DynamicValue.Text(v.sum.text.getOrElse(unexpectedCase(v))),
    )
  override def timestamp: Type =
    primitive(
      v => Sum.Timestamp(v.timestamp),
      v => DynamicValue.Timestamp(v.sum.timestamp.getOrElse(unexpectedCase(v))),
    )
  override def party: Type =
    primitive(
      v => Sum.Party(v.party),
      v => DynamicValue.Party(v.sum.party.getOrElse(unexpectedCase(v))),
    )
  override def bool: Type =
    primitive(
      v => Sum.Bool(v.bool),
      v => DynamicValue.Bool(v.sum.bool.getOrElse(unexpectedCase(v))),
    )
  override def date: Type =
    primitive(
      v => Sum.Date(v.date),
      v => DynamicValue.Date(v.sum.date.getOrElse(unexpectedCase(v))),
    )
  override def contractId(template: Type): Type =
    primitive(
      v => Sum.ContractId(v.contractId),
      v => DynamicValue.ContractId(v.sum.contractId.getOrElse(unexpectedCase(v))),
    )

  private def primitive(f: DynamicValue => Sum, g: Value => DynamicValue): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value = Value(
      f(dv)
    )
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue = g(a)

  private def unexpectedCase(v: Value): Nothing = throw Exception(
    s"Unexpected case: ${v.sum.number}"
  )
end ProtobufCodec
