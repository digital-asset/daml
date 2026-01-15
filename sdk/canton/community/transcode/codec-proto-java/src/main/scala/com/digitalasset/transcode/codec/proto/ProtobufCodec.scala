// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.digitalasset.transcode.codec.proto.Value
import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.schema.CodecVisitor.*
import com.google.protobuf.Empty

import scala.jdk.CollectionConverters.*

/** This codec converts Ledger API GRPC values. */
class ProtobufCodec extends CodecVisitor[Value]:

  override def record(fields: Seq[(FieldName, Type)]): Type = new Type:
    private val codecs = fields.map(_._2)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val _ = dv.checkRecordSize(fields.size)
      val record = newRecord()
      dv.recordIterator.zip(fields).foreach { case (f, (name, c)) =>
        record.addFields(newRecordField().setLabel(name).setValue(c.fromDynamicValue(f)))
      }
      newValue().setRecord(record).build()
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Record(
        a.getRecord.getFieldsList.asScala.view zip codecs map ((f, c) =>
          c.toDynamicValue(f.getValue)
        )
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
      newValue()
        .setVariant(newVariant().setConstructor(ctor).setValue(valueCodec.fromDynamicValue(v)))
        .build
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val (ctor, valueCodec) = caseMap.getMaybe(a.getVariant.getConstructor)
      DynamicValue.Variant(ctor, valueCodec.toDynamicValue(a.getVariant.getValue))
  end variant

  override def enumeration(cases: Seq[EnumConName]): Type = new Type:
    private val caseList = cases.map(_.enumConName).toArray
    private val caseMap = cases.map(_.enumConName).zipWithIndex.toMap
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      newValue().setEnum(newEnum().setConstructor(caseList.getMaybe(dv.enumeration))).build()
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Enumeration(caseMap.getMaybe(a.getEnum.getConstructor))
  end enumeration

  override def list(elem: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val list = newList()
      dv.list.iterator.foreach(a => list.addElements(elem.fromDynamicValue(a)))
      newValue().setList(list).build()
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.List(a.getList.getElementsList.asScala.view.map(elem.toDynamicValue))
  end list

  override def optional(elem: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val optional = newOptional()
      dv.optional.foreach(a => optional.setValue(elem.fromDynamicValue(a)))
      newValue().setOptional(optional).build()
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Optional(
        if a.getOptional.hasValue then Some(elem.toDynamicValue(a.getOptional.getValue)) else None
      )
  end optional

  override def textMap(valueCodec: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      mkTextMap(dv.textMap.iterator.map((k, v) => k -> valueCodec.fromDynamicValue(v)))
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.TextMap(
        a.textMap.getEntriesList.asScala.view.map(e =>
          e.getKey -> valueCodec.toDynamicValue(e.getValue)
        )
      )
  end textMap

  override def genMap(keyCodec: Type, valueCodec: Type): Type = new Type:
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val genMap = newGenMap()
      dv.genMap.iterator.foreach { (k, v) =>
        genMap.addEntries(
          newGenMapEntry()
            .setKey(keyCodec.fromDynamicValue(k))
            .setValue(valueCodec.fromDynamicValue(v))
        )
      }
      newValue().setGenMap(genMap).build()
    override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val entries =
        a.getGenMap.getEntriesList.asScala.view.map(e =>
          keyCodec.toDynamicValue(e.getKey) -> valueCodec.toDynamicValue(e.getValue)
        )
      DynamicValue.GenMap(entries)
  end genMap

  override def unit: Type =
    primitive((dv, v) => v.setUnit(Empty.getDefaultInstance), _ => DynamicValue.Unit)
  override def int64: Type =
    primitive((dv, v) => v.setInt64(dv.int64), v => DynamicValue.Int64(v.getInt64))
  override def numeric(scale: Int): Type =
    primitive((dv, v) => v.setNumeric(dv.numeric), v => DynamicValue.Numeric(v.getNumeric))
  override def text: Type =
    primitive((dv, v) => v.setText(dv.text), v => DynamicValue.Text(v.getText))
  override def timestamp: Type =
    primitive((dv, v) => v.setTimestamp(dv.timestamp), v => DynamicValue.Timestamp(v.getTimestamp))
  override def party: Type =
    primitive((dv, v) => v.setParty(dv.party), v => DynamicValue.Party(v.getParty))
  override def bool: Type =
    primitive((dv, v) => v.setBool(dv.bool), v => DynamicValue.Bool(v.getBool))
  override def date: Type =
    primitive((dv, v) => v.setDate(dv.date), v => DynamicValue.Date(v.getDate))
  override def contractId(template: Type): Type =
    primitive(
      (dv, v) => v.setContractId(dv.contractId),
      v => DynamicValue.ContractId(v.getContractId),
    )

  private def primitive(
      f: (DynamicValue, ValueBuilder) => ValueBuilder,
      g: Value => DynamicValue,
  ): Type =
    new Type:
      override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
        val v = newValue()
        val _ = f(dv, v)
        v.build()
      override def toDynamicValue(a: Value)(using VarMap[Decoder[Value]]): DynamicValue = g(a)

end ProtobufCodec
