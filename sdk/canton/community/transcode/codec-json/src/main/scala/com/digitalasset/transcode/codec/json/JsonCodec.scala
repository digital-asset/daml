// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.schema.CodecVisitor.*
import com.digitalasset.transcode.{
  IncorrectVariantRepresentationException,
  MissingFieldsException,
  UnexpectedFieldsException,
}
import ujson.*

import java.time.*
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

/** Json Codec.
  *
  * @param encodeNumericAsString
  *   encode numeric as string (true) or as a json number (false). The latter might be useful for
  *   querying and mathematical operations, but can lose precision due to float point errors.
  * @param encodeInt64AsString
  *   encode int64 as a string (true) or as a json number (false). The latter might be useful for
  *   querying and mathematical operations, but can lose precision, as numbers in some json
  *   implementations are backed [[Double]].
  * @param allowMissingFields
  *   allow missing fields in records or throw, Canton uses this for testing only
  * @param removeTrailingNonesInRecords
  *   if true, trailing nones are removed from the record values
  */
class JsonCodec(
    encodeNumericAsString: Boolean = true,
    encodeInt64AsString: Boolean = true,
    allowMissingFields: Boolean = false,
    removeTrailingNonesInRecords: Boolean = false,
) extends CodecVisitor[Value]:

  override def record(fields: Seq[(FieldName, Type)]): Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val valueMap = v.obj.value

      val missingFields = fields.collect {
        case (name, c) if !c.isOptional => name
      }.toSet -- valueMap.keys
      if !allowMissingFields && missingFields.nonEmpty then
        throw MissingFieldsException(missingFields)

      val unexpectedFields = valueMap.collect {
        case (name, v) if !v.isNull => name
      }.toSet -- fields.map(_._1)
      if unexpectedFields.nonEmpty then throw new UnexpectedFieldsException(unexpectedFields)

      val values = fields.view.map((name, c) => c.toDynamicValue(valueMap.getOrElse(name, Null)))
      if removeTrailingNonesInRecords then
        DynamicValue.Record(values.reverse.dropWhile(_.isEmpty).toSeq.reverse)
      else DynamicValue.Record(values)
    end toDynamicValue

    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      dv.checkRecordSize(fields.size)
      val iterator =
        if allowMissingFields then dv.recordIterator else dv.recordIteratorPadded(fields.size)
      val values = iterator.zip(fields).map { case (f, (name, c)) => name -> c.fromDynamicValue(f) }
      if removeTrailingNonesInRecords then
        Obj.from(values.toSeq.reverse.dropWhile((_, v) => v.isNull).reverse)
      else Obj.from(values)
  end record

  override def variant(cases: Seq[(VariantConName, Type)]): Type = new Type:
    private val caseList = cases.map((name, codec) => name.variantConName -> codec).toArray
    private val caseMap = cases.zipWithIndex.map { case ((n, c), ix) =>
      n.variantConName -> (ix, c)
    }.toMap
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      (for
        obj <- v.objOpt
        tag <- obj.get("tag")
        value <- obj.get("value")
      yield (tag, value)) match
        case Some((tag, value)) =>
          val (ctorIx, valueCodec) = caseMap
            .getOrElse(
              tag.str,
              throw new IncorrectVariantRepresentationException(
                s"Unexpected variant tag: ${tag.str}"
              ),
            )
          DynamicValue.Variant(ctorIx, valueCodec.toDynamicValue(value))
        case _ =>
          throw new IncorrectVariantRepresentationException(
            s"Variant type json must have 'tag' and 'value' fields."
          )
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val (ctorIx, value) = dv.variant
      val (ctorName, valueCodec) = caseList.getMaybe(ctorIx)
      Obj("tag" -> Str(ctorName), "value" -> valueCodec.fromDynamicValue(value))
  end variant

  override def enumeration(cases: Seq[EnumConName]): Type = new Type:
    private val caseList = cases.map(_.enumConName).toArray
    private val caseMap = cases.map(_.enumConName).zipWithIndex.toMap
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Enumeration(caseMap.getMaybe(v.str))
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Str(caseList.getMaybe(dv.enumeration))
  end enumeration

  override def list(elem: Type): Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.List(v.arr.view.map(elem.toDynamicValue))
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Arr(ArrayBuffer.from(dv.list.iterator.map(elem.fromDynamicValue)))
  end list

  override def optional(elem: Type): Type = new OptionalProcessor(elem)
  private class OptionalProcessor(elem: Type) extends Type {
    override def isOptional(using VarMap[Decoder[Value]]): Boolean = true
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue = elem match
      case _: OptionalProcessor => toDynamicValueNested(v)
      case _ => toDynamicValueSimple(v)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      elem match
        case _: OptionalProcessor => fromDynamicValueNested(dv)
        case _ => fromDynamicValueSimple(dv)

    private def toDynamicValueSimple(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Optional(if v.isNull then None else Some(elem.toDynamicValue(v)))
    private def fromDynamicValueSimple(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      dv.optional.fold(Null)(v => elem.fromDynamicValue(v))
    private def toDynamicValueNested(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Optional(
        if v.arr.isEmpty then None else Some(toDynamicValueNext(elem)(v.arr(0)))
      )
    private def fromDynamicValueNested(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      dv.optional.fold(Arr())(v => Arr(fromDynamicValueNext(elem)(v)))

    private def toDynamicValueNext(e: Type)(using VarMap[Decoder[Value]]) = e match
      case elem: OptionalProcessor => elem.toDynamicValueNested
      case elem => elem.toDynamicValue

    private def fromDynamicValueNext(e: Type)(using VarMap[Encoder[Value]]) = e match
      case elem: OptionalProcessor => elem.fromDynamicValueNested
      case elem => elem.fromDynamicValue
  }

  override def textMap(value: Type): Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.TextMap(v.obj.value.view.mapValues(value.toDynamicValue))
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Obj.from(dv.textMap.iterator.map((k, v) => k -> value.fromDynamicValue(v)))
  end textMap

  override def genMap(key: Type, value: Type): Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.GenMap(
        v.arr.view.map(e => key.toDynamicValue(e.arr(0)) -> value.toDynamicValue(e.arr(1)))
      )
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Arr.from(
        dv.genMap.iterator.map((k, v) => Arr(key.fromDynamicValue(k), value.fromDynamicValue(v)))
      )
  end genMap

  override def unit: Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Unit
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value = Obj()

  override def bool: Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Bool(v.bool)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value = Bool(
      dv.bool
    )

  override def text: Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Text(v.str)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value = Str(
      dv.text
    )

  override def int64: Type =
    if encodeInt64AsString then
      new Type:
        override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
          DynamicValue.Int64(v.str.toLong)
        override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
          Str(String.valueOf(dv.int64))
    else
      new Type:
        override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
          DynamicValue.Int64(v.num.longValue)
        override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
          Num(dv.int64.toDouble)

  override def numeric(scale: Int): Type =
    if encodeNumericAsString then
      new Type:
        override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
          v match
            case num: Num => DynamicValue.Numeric(num.value.toString)
            case str: Str => DynamicValue.Numeric(str.value)
            case _ => throw new IllegalArgumentException(s"$v is not a valid numeric value")
        override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
          Str(dv.numeric)
    else
      new Type:
        override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
          DynamicValue.Numeric(v.num.toString)
        override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
          Num(dv.numeric.toDouble)

  override def party: Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Party(v.str)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value = Str(
      dv.party
    )

  override def timestamp: Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      val time = DateTimeFormatter.ISO_DATE_TIME.parse(v.str, ZonedDateTime.from)
      DynamicValue.Timestamp(time.toEpochSecond * 1_000_000 + time.getNano / 1_000)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      val time = LocalDateTime
        .ofEpochSecond(dv.timestamp / 1_000_000, 0, ZoneOffset.UTC)
        .plusNanos(dv.timestamp % 1_000_000 * 1_000)
        .atZone(ZoneOffset.UTC)
      Str(DateTimeFormatter.ISO_DATE_TIME.format(time))

  override def date: Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.Date(
        DateTimeFormatter.ISO_LOCAL_DATE.parse(v.str, LocalDate.from).toEpochDay.intValue
      )
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Str(DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(dv.date.longValue)))

  override def contractId(template: Type): Type = new Type:
    override def toDynamicValue(v: Value)(using VarMap[Decoder[Value]]): DynamicValue =
      DynamicValue.ContractId(v.str)
    override def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[Value]]): Value =
      Str(dv.contractId)

end JsonCodec
