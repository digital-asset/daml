// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.schema.DynamicValue.*
import com.digitalasset.transcode.{Codec, MissingFieldException, UnexpectedFieldsException}
import ujson.*

import java.time.*
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

/** Json Codec.
  *
  * @param encodeNumericAsString
  *   encode numeric as string (true) or as a json number (false). The latter might be useful for
  *   querying and mathematical operations, but can loose precision due to float point errors.
  * @param encodeInt64AsString
  *   encode int64 as a string (true) or as a json number (false). The latter might be useful for
  *   querying and mathematical operations, but can loose precision, as numbers in some json
  *   implementations are backed by Double
  */
class JsonCodec(
    encodeNumericAsString: Boolean = true,
    encodeInt64AsString: Boolean = true,
) extends SchemaVisitor {
  override type Type = Codec[Value]

  override def record(
      id: Identifier,
      appliedArgs: => Seq[(TypeVarName, Type)],
      fields0: => Seq[(FieldName, Type)],
  ): Type = new Type {
    private lazy val fields = fields0.map { case (n, c) => n.fieldName -> c }

    override def toDynamicValue(v: Value): DynamicValue = {
      val valueMap = v.obj.value

      val unexpectedFields = valueMap.keySet.toSet -- fields.map(_._1)

      val result = DynamicValue.Record(fields map { case (name, c) =>
        if (c.isOptional()) {
          (Some(name), c.toDynamicValue(valueMap.getOrElse(name, ujson.Null)))
        } else {
          valueMap.get(name) match {
            case Some(value) => (Some(name), c.toDynamicValue(value))
            case None =>
              throw new MissingFieldException(name)
          }
        }
      })
      // We handle unexpectedValues after creation of value, as the more important exception is a MissingFieldException and should be
      // thrown if needed
      val unexpectedValues =
        valueMap
          .filter { case (k, v) => unexpectedFields.contains(k) && v != ujson.Null }
      if (!unexpectedValues.isEmpty) {
        throw new UnexpectedFieldsException(unexpectedFields)
      }
      result
    }

    override def fromDynamicValue(dv: DynamicValue): Value =
      Obj.from(dv.record.iterator zip fields map { case (f, (name, c)) =>
        f._1.getOrElse(name) -> c.fromDynamicValue(f._2)
      })
  }

  override def variant(
      id: Identifier,
      appliedArgs: => Seq[(TypeVarName, Type)],
      cases: => Seq[(VariantConName, Type)],
  ): Type = new Type {
    private lazy val caseList = cases.map { case (name, codec) =>
      name.variantConName -> codec
    }.toArray
    private lazy val caseMap = cases.zipWithIndex.map { case ((n, c), ix) =>
      n.variantConName -> (ix, c)
    }.toMap

    override def toDynamicValue(v: Value): DynamicValue = {
      val (ctorIx, valueCodec) = caseMap(v.obj("tag").str)
      DynamicValue.Variant(ctorIx, valueCodec.toDynamicValue(v.obj("value")))
    }

    override def fromDynamicValue(dv: DynamicValue): Value = {
      val (ctorName, valueCodec) = caseList(dv.variant.ctorIx)
      Obj("tag" -> Str(ctorName), "value" -> valueCodec.fromDynamicValue(dv.variant.value))
    }
  }

  override def `enum`(id: Identifier, cases: Seq[EnumConName]): Type = new Type {
    private val caseList = cases.map(_.enumConName).toArray
    private val caseMap = cases.map(_.enumConName).zipWithIndex.toMap

    override def toDynamicValue(v: Value): DynamicValue =
      DynamicValue.Enum(caseMap(v.str))

    override def fromDynamicValue(dv: DynamicValue): Value =
      Str(caseList(dv.`enum`))
  }

  override def list(elem: Type): Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue =
      DynamicValue.List(v.arr.view.map(elem.toDynamicValue))

    override def fromDynamicValue(dv: DynamicValue): Value =
      Arr(ArrayBuffer.from(dv.list.iterator.map(elem.fromDynamicValue)))
  }

  override def optional(elem: Type): Type = new OptionalProcessor(elem)
  private class OptionalProcessor(elem: Type) extends Type {
    override def isOptional(): Boolean = true

    def toDynamicValue(v: Value): DynamicValue = elem match {
      case _: OptionalProcessor => toDynamicValueNested(v)
      case _ => toDynamicValueSimple(v)
    }

    def fromDynamicValue(dv: DynamicValue): Value = elem match {
      case _: OptionalProcessor => fromDynamicValueNested(dv)
      case _ => fromDynamicValueSimple(dv)
    }

    private def toDynamicValueSimple(v: Value): DynamicValue =
      DynamicValue.Optional(if (v.isNull) None else Some(elem.toDynamicValue(v)))
    private def fromDynamicValueSimple(dv: DynamicValue): Value =
      dv.optional.fold[ujson.Value](ujson.Null)(v => elem.fromDynamicValue(v))

    private def toDynamicValueNested(v: Value): DynamicValue =
      DynamicValue.Optional(if (v.arr.isEmpty) None else Some(toDynamicValueNext(elem)(v.arr(0))))
    private def fromDynamicValueNested(dv: DynamicValue): Value =
      dv.optional.fold(Arr())(v => Arr(fromDynamicValueNext(elem)(v)))

    private def toDynamicValueNext(e: Type) = e match {
      case processor: OptionalProcessor => processor.toDynamicValueNested
      case otherElem => otherElem.toDynamicValue
    }

    private def fromDynamicValueNext(e: Type) = e match {
      case processor: OptionalProcessor => processor.fromDynamicValueNested
      case otherElem => otherElem.fromDynamicValue
    }
  }

  override def textMap(value: Type): Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue =
      DynamicValue.TextMap(v.obj.value.view.mapValues(value.toDynamicValue))

    override def fromDynamicValue(dv: DynamicValue): Value =
      Obj.from(dv.textMap.iterator.map { case (k, v) => k -> value.fromDynamicValue(v) })
  }

  override def genMap(key: Type, value: Type): Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue =
      DynamicValue.GenMap(
        v.arr.view.map(e => key.toDynamicValue(e.arr(0)) -> value.toDynamicValue(e.arr(1)))
      )

    override def fromDynamicValue(dv: DynamicValue): Value =
      Arr.from(
        dv.genMap.iterator.map { case (k, v) =>
          Arr(key.fromDynamicValue(k), value.fromDynamicValue(v))
        }
      )
  }

  override def unit: Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue = DynamicValue.Unit

    override def fromDynamicValue(dv: DynamicValue): Value = Obj()
  }

  override def bool: Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue = DynamicValue.Bool(v.bool)

    override def fromDynamicValue(dv: DynamicValue): Value = ujson.Bool(dv.bool)
  }

  override def text: Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue = DynamicValue.Text(v.str)

    override def fromDynamicValue(dv: DynamicValue): Value = Str(dv.text)
  }

  override def int64: Type =
    if (encodeInt64AsString) {
      new Type {

        /** While decoding we support numeric values on input. */
        override def toDynamicValue(v: Value): DynamicValue = v match {
          case Str(value) => DynamicValue.Int64(value.toLong)

          case Num(value) => DynamicValue.Int64(value.longValue)

          case _ => throw new IllegalArgumentException(s"$v is not a valid Int64 value")
        }

        override def fromDynamicValue(dv: DynamicValue): Value = Str(String.valueOf(dv.int64))
      }
    } else {
      new Type {
        override def toDynamicValue(v: Value): DynamicValue = DynamicValue.Int64(v.num.longValue)

        override def fromDynamicValue(dv: DynamicValue): Value = Num(dv.int64.toDouble)
      }
    }

  override def numeric(scale: Int): Type =
    if (encodeNumericAsString) {
      new Type {

        /** While decoding we support numeric values on input. */
        override def toDynamicValue(v: Value): DynamicValue =
          v match {
            case Num(value) => DynamicValue.Numeric(value.toString())
            case Str(value) => DynamicValue.Numeric(value)
            case _ => throw new IllegalArgumentException(s"$v is not a valid numeric value")
          }

        override def fromDynamicValue(dv: DynamicValue): Value = Str(dv.numeric)
      }
    } else {
      new Type {
        override def toDynamicValue(v: Value): DynamicValue = DynamicValue.Numeric(v.num.toString)

        override def fromDynamicValue(dv: DynamicValue): Value = Num(dv.numeric.toDouble)
      }
    }

  override def party: Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue = DynamicValue.Party(v.str)

    override def fromDynamicValue(dv: DynamicValue): Value = Str(dv.party)
  }

  override def timestamp: Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue = {
      val time = DateTimeFormatter.ISO_DATE_TIME.parse(v.str, ZonedDateTime.from)
      DynamicValue.Timestamp(time.toEpochSecond * 1000000 + time.getNano / 1000)
    }

    override def fromDynamicValue(dv: DynamicValue): Value = {
      val time = LocalDateTime
        .ofEpochSecond(
          dv.timestamp / 1000000,
          (dv.timestamp % 1000000 * 1000).intValue,
          ZoneOffset.UTC,
        )
        .atZone(ZoneOffset.UTC)
      Str(DateTimeFormatter.ISO_DATE_TIME.format(time))
    }
  }

  override def date: Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue =
      DynamicValue.Date(
        DateTimeFormatter.ISO_LOCAL_DATE.parse(v.str, LocalDate.from).toEpochDay.intValue
      )

    override def fromDynamicValue(dv: DynamicValue): Value =
      Str(DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(dv.date.longValue)))
  }

  override def contractId(template: Type): Type = new Type {
    override def toDynamicValue(v: Value): DynamicValue = DynamicValue.ContractId(v.str)

    override def fromDynamicValue(dv: DynamicValue): Value = Str(dv.contractId)
  }

  override def interface(id: Identifier): Type = unit

  override def variable(name: TypeVarName, value: Codec[Value]): Codec[Value] = value

}
