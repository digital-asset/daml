// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.api.refinements.ApiTypes.ContractId
import com.digitalasset.ledger.client.binding.{Template, Primitive => P}
import spray.{json => sj}
import spray.json.JsValue
import SlickTypeEncoding.SupportedProfile
import JdbcJsonTypeCodecs.Codec
import slick.ast.FieldSymbol

trait JdbcJsonTypeCodecs[Profile <: SupportedProfile] extends ValuePrimitiveEncoding[Codec] {
  def jsonValueList[A: JsonLfTypeEncoding.Out]: Codec[P.List[A]]
  def jsonValueOptional[A: JsonLfTypeEncoding.Out]: Codec[P.Optional[A]]
  def jsonValueMap[A: JsonLfTypeEncoding.Out]: Codec[P.Map[A]]
}

object JdbcJsonTypeCodecs {

  type Codec[A] = (SupportedProfile#BaseColumnType[A], JsonLfTypeEncoding.Out[A])

  def apply(
      profile: SupportedProfile,
      jsonColumnType: SupportedProfile#BaseColumnType[JsValue],
      jsonCodec: ValuePrimitiveEncoding[JsonLfTypeEncoding.Out] = JsonTypeCodecs)
    : JdbcJsonTypeCodecs[profile.type] = {
    new JdbcJsonTypeCodecs[profile.type] {
      private val jsonCodec = JsonLfTypeEncoding.primitive

      private val customJdbcTypes =
        CustomJdbcTypes[JsValue, JsonLfTypeEncoding.Out, profile.type](
          profile,
          new ListOptionMapToJsonCodec(CustomJsonFormats))(jsonColumnType)

      override val valueInt64: Codec[P.Int64] =
        (profile.api.longColumnType, jsonCodec.valueInt64)

      override val valueDecimal: Codec[P.Decimal] =
        (columnDecimal, jsonCodec.valueDecimal)

      private[this] def columnDecimal: profile.BaseColumnType[P.Decimal] =
        new profile.columnTypes.BigDecimalJdbcType {
          override def sqlTypeName(sym: Option[FieldSymbol]): String =
            "DECIMAL(38,10)"
        }

      override val valueText: Codec[P.Text] =
        (profile.api.stringColumnType, jsonCodec.valueText)

      override val valueBool: Codec[P.Bool] =
        (profile.api.booleanColumnType, jsonCodec.valueBool)

      override val valueDate: Codec[P.Date] =
        (customJdbcTypes.primitiveDateColumnType, jsonCodec.valueDate)

      override val valueTimestamp: Codec[P.Timestamp] =
        (customJdbcTypes.primitiveTimestampColumnType, jsonCodec.valueTimestamp)

      override val valueUnit: Codec[P.Unit] =
        (customJdbcTypes.primitiveUnitColumnType, jsonCodec.valueUnit)

      override val valueParty: Codec[P.Party] =
        (P.Party.subst(profile.api.stringColumnType), jsonCodec.valueParty)

      override def valueContractId[Tpl <: Template[Tpl]]: Codec[P.ContractId[Tpl]] =
        (
          P.substContractId(ContractId.subst(profile.api.stringColumnType)),
          jsonCodec.valueContractId[Tpl])

      override def valueList[A: Codec]: Codec[P.List[A]] = {
        val (_, jsonFormat) = implicitly[Codec[A]]
        jsonValueList(jsonFormat)
      }

      override def valueOptional[A: Codec]: Codec[P.Optional[A]] = {
        val (_, jsonFormat) = implicitly[Codec[A]]
        jsonValueOptional(jsonFormat)
      }

      override implicit def valueMap[A: Codec]: Codec[P.Map[A]] = {
        val (_, jsonFormat) = implicitly[Codec[A]]
        jsonValueMap(jsonFormat)
      }

      override def jsonValueList[A: JsonLfTypeEncoding.Out]: Codec[P.List[A]] = {
        (customJdbcTypes.primitiveListColumnType[A], jsonCodec.valueList)
      }

      override def jsonValueOptional[A: JsonLfTypeEncoding.Out]: Codec[P.Optional[A]] = {
        (customJdbcTypes.primitiveOptionalColumnType[A], jsonCodec.valueOptional)
      }

      override def jsonValueMap[A: JsonLfTypeEncoding.Out]: Codec[P.Map[A]] = {
        (customJdbcTypes.primitiveMapColumnType[A], jsonCodec.valueMap)
      }
    }
  }

  private[encoding] final class ListOptionMapToJsonCodec(
      jsonFormats: sj.StandardFormats with sj.CollectionFormats)
      extends ListOptionMapCodec[JsValue, JsonLfTypeEncoding.Out] {
    override def encodeList[A: JsonLfTypeEncoding.Out](as: P.List[A]): JsValue = {
      import spray.json._
      import jsonFormats._
      as.toJson
    }

    override def decodeList[A: JsonLfTypeEncoding.Out](t: JsValue): P.List[A] = {
      import jsonFormats._
      t.convertTo[P.List[A]]
    }

    override def encodeOption[A: JsonLfTypeEncoding.Out](a: P.Optional[A]): JsValue = {
      CustomJsonFormats.optionJsonFormat[A].write(a)
    }

    override def decodeOption[A: JsonLfTypeEncoding.Out](t: JsValue): P.Optional[A] = {
      CustomJsonFormats.optionJsonFormat[A].read(t)
    }

    override def encodeMap[A: JsonLfTypeEncoding.Out](map: P.Map[A]): JsValue = {
      import spray.json._
      import jsonFormats._
      import sj.DefaultJsonProtocol.StringJsonFormat
      map.toJson
    }

    override def decodeMap[A: JsonLfTypeEncoding.Out](t: JsValue): P.Map[A] = {
      import jsonFormats._
      import sj.DefaultJsonProtocol.StringJsonFormat
      t.convertTo[Map[String, A]]
    }
  }
}
