// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.json

import com.daml.lf.data.{Decimal => LfDecimal, FrontStack, ImmArray, Ref, SortedLookupList}
import com.daml.navigator.{model => Model}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.json.ApiValueImplicits._
import com.daml.navigator.json.DamlLfCodec.JsonImplicits._
import com.daml.navigator.json.Util._
import com.daml.navigator.model.DamlLfIdentifier
import spray.json._

/** A verbose encoding of API values.
  *
  * The encoded values include type information and can be losslessly encoded.
  */
object ApiCodecVerbose {

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  private[this] final val propType: String = "type"
  private[this] final val propValue: String = "value"
  private[this] final val propLabel: String = "label"
  private[this] final val propFields: String = "fields"
  private[this] final val propId: String = "id"
  private[this] final val propConstructor: String = "constructor"
  private[this] final val tagText: String = "text"
  private[this] final val tagInt64: String = "int64"
  private[this] final val tagNumeric: String = "numeric"
  private[this] final val tagBool: String = "bool"
  private[this] final val tagContractId: String = "contractid"
  private[this] final val tagTimestamp: String = "timestamp"
  private[this] final val tagDate: String = "date"
  private[this] final val tagParty: String = "party"
  private[this] final val tagUnit: String = "unit"
  private[this] final val tagOptional: String = "optional"
  private[this] final val tagList: String = "list"
  private[this] final val tagTextMap: String = "textmap"
  private[this] final val tagGenMap: String = "genmap"
  private[this] final val tagRecord: String = "record"
  private[this] final val tagVariant: String = "variant"
  private[this] final val tagEnum: String = "enum"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def apiValueToJsValue(value: Model.ApiValue): JsValue = value match {
    case v: Model.ApiRecord => apiRecordToJsValue(v)
    case v: Model.ApiVariant => apiVariantToJsValue(v)
    case v: V.ValueEnum => apiEnumToJsValue(v)
    case v: Model.ApiList => apiListToJsValue(v)
    case V.ValueText(v) => JsObject(propType -> JsString(tagText), propValue -> JsString(v))
    case V.ValueInt64(v) =>
      JsObject(propType -> JsString(tagInt64), propValue -> JsString((v: Long).toString))
    case V.ValueNumeric(v) =>
      JsObject(propType -> JsString(tagNumeric), propValue -> JsString(v.toUnscaledString))
    case V.ValueBool(v) => JsObject(propType -> JsString(tagBool), propValue -> JsBoolean(v))
    case V.ValueContractId(v) =>
      JsObject(propType -> JsString(tagContractId), propValue -> JsString(v.coid))
    case v: V.ValueTimestamp =>
      JsObject(propType -> JsString(tagTimestamp), propValue -> JsString(v.toIso8601))
    case v: V.ValueDate =>
      JsObject(propType -> JsString(tagDate), propValue -> JsString(v.toIso8601))
    case V.ValueParty(v) =>
      JsObject(propType -> JsString(tagParty), propValue -> JsString(v))
    case V.ValueUnit => JsObject(propType -> JsString(tagUnit))
    case V.ValueOptional(None) => JsObject(propType -> JsString(tagOptional), propValue -> JsNull)
    case V.ValueOptional(Some(v)) =>
      JsObject(propType -> JsString(tagOptional), propValue -> apiValueToJsValue(v))
    case v: Model.ApiMap => apiTextMapToJsValue(v)
    case v: Model.ApiGenMap => apiGenMapToJsValue(v)
  }

  def apiListToJsValue(value: Model.ApiList): JsValue =
    JsObject(
      propType -> JsString(tagList),
      propValue -> JsArray(value.values.map(apiValueToJsValue).toImmArray.toSeq: _*),
    )

  def apiTextMapToJsValue(value: Model.ApiMap): JsValue =
    JsObject(
      propType -> JsString(tagTextMap),
      propValue -> JsArray(value.value.toImmArray.toSeq.toVector.map { case (k, v) =>
        JsObject("key" -> JsString(k), "value" -> apiValueToJsValue(v))
      }),
    )

  def apiGenMapToJsValue(value: Model.ApiGenMap): JsValue =
    JsObject(
      propType -> JsString(tagGenMap),
      propValue -> JsArray(value.entries.toSeq.toVector.map { case (k, v) =>
        JsObject("key" -> apiValueToJsValue(k), "value" -> apiValueToJsValue(v))
      }),
    )

  def apiVariantToJsValue(value: Model.ApiVariant): JsValue =
    JsObject(
      propType -> JsString(tagVariant),
      propId -> value.tycon.map(_.toJson).getOrElse(JsNull),
      propConstructor -> JsString(value.variant),
      propValue -> apiValueToJsValue(value.value),
    )

  def apiEnumToJsValue(value: V.ValueEnum): JsValue =
    JsObject(
      propType -> JsString(tagEnum),
      propId -> value.tycon.map(_.toJson).getOrElse(JsNull),
      propConstructor -> JsString(value.value),
    )

  def apiRecordToJsValue(value: Model.ApiRecord): JsValue =
    JsObject(
      propType -> JsString(tagRecord),
      propId -> value.tycon.map(_.toJson).getOrElse(JsNull),
      propFields -> JsArray(value.fields.toSeq.zipWithIndex.map { case ((oflabel, fvalue), ix) =>
        JsObject(
          propLabel -> JsString(oflabel getOrElse (ix: Int).toString),
          propValue -> apiValueToJsValue(fvalue),
        )
      }.toVector),
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------

  private[this] def jsValueToApiRecordField(value: JsValue): Model.ApiRecordField = {
    val label = strField(value, propLabel, "ApiRecordField")
    val avalue = jsValueToApiValue(anyField(value, propValue, "ApiRecordField"))
    (Some(assertDE(Ref.Name fromString label)), avalue)
  }

  def jsValueToApiValue(value: JsValue): Model.ApiValue =
    strField(value, propType, "ApiValue") match {
      case `tagRecord` => jsValueToApiRecord(value)
      case `tagVariant` => jsValueToApiVariant(value)
      case `tagEnum` => jsValueToApiEnum(value)
      case `tagList` =>
        V.ValueList(arrayField(value, propValue, "ApiList").map(jsValueToApiValue).to(FrontStack))
      case `tagText` => V.ValueText(strField(value, propValue, "ApiText"))
      case `tagInt64` => V.ValueInt64(strField(value, propValue, "ApiInt64").toLong)
      case `tagNumeric` =>
        V.ValueNumeric(assertDE(LfDecimal fromString strField(value, propValue, "ApiNumeric")))
      case `tagBool` => V.ValueBool(boolField(value, propValue, "ApiBool"))
      case `tagContractId` =>
        V.ValueContractId(
          assertDE(V.ContractId.fromString(strField(value, propValue, "ApiContractId")))
        )
      case `tagTimestamp` =>
        V.ValueTimestamp.fromIso8601(strField(value, propValue, "ApiTimestamp"))
      case `tagDate` => V.ValueDate.fromIso8601(strField(value, propValue, "ApiDate"))
      case `tagParty` =>
        V.ValueParty(assertDE(Ref.Party fromString strField(value, propValue, "ApiParty")))
      case `tagUnit` => V.ValueUnit
      case `tagOptional` =>
        anyField(value, propValue, "ApiOptional") match {
          case JsNull => V.ValueNone
          case v => V.ValueOptional(Some(jsValueToApiValue(v)))
        }
      case `tagTextMap` =>
        V.ValueTextMap(
          SortedLookupList
            .fromImmArray(
              arrayField(value, propValue, "ApiMap").view.map(jsValueToMapEntry).to(ImmArray)
            )
            .fold(
              err => deserializationError(s"Can't read ${value.prettyPrint} as ApiValue, $err'"),
              identity,
            )
        )
      case `tagGenMap` =>
        V.ValueGenMap(
          arrayField(value, propValue, "ApiGenMap").view.map(jsValueToGenMapEntry).to(ImmArray)
        )
      case t =>
        deserializationError(s"Can't read ${value.prettyPrint} as ApiValue, unknown type '$t'")
    }

  def jsValueToApiRecord(value: JsValue): Model.ApiRecord =
    strField(value, propType, "ApiRecord") match {
      case `tagRecord` =>
        V.ValueRecord(
          asObject(value, "ApiRecord").fields
            .get(propId)
            .flatMap(_.convertTo[Option[DamlLfIdentifier]]),
          arrayField(value, propFields, "ApiRecord").map(jsValueToApiRecordField).to(ImmArray),
        )
      case t =>
        deserializationError(
          s"Can't read ${value.prettyPrint} as ApiRecord, type '$t' is not a record"
        )
    }

  def jsValueToMapEntry(value: JsValue): (String, Model.ApiValue) = {
    val translation = value match {
      case JsObject(map) =>
        for {
          key <- map.get("key").collect { case JsString(s) => s }
          value <- map.get("value").map(jsValueToApiValue)
        } yield key -> value
      case _ => None
    }

    translation.getOrElse(deserializationError(s"Can't read ${value.prettyPrint} as a map entry"))
  }

  def jsValueToGenMapEntry(value: JsValue): (Model.ApiValue, Model.ApiValue) = {
    val translation = value match {
      case JsObject(genMap) =>
        for {
          key <- genMap.get("key").map(jsValueToApiValue)
          value <- genMap.get("value").map(jsValueToApiValue)
        } yield key -> value
      case _ => None
    }

    translation.getOrElse(deserializationError(s"Can't read ${value.prettyPrint} as a map entry"))
  }

  def jsValueToApiVariant(value: JsValue): Model.ApiVariant =
    strField(value, propType, "ApiVariant") match {
      case `tagVariant` =>
        V.ValueVariant(
          asObject(value, "ApiVariant").fields
            .get(propId)
            .flatMap(_.convertTo[Option[DamlLfIdentifier]]),
          assertDE(Ref.Name fromString strField(value, propConstructor, "ApiVariant")),
          jsValueToApiValue(anyField(value, propValue, "ApiVariant")),
        )
      case t =>
        deserializationError(
          s"Can't read ${value.prettyPrint} as ApiVariant, type '$t' is not a variant"
        )
    }

  def jsValueToApiEnum(value: JsValue): V.ValueEnum =
    strField(value, propType, "ApiEnum") match {
      case `tagEnum` =>
        V.ValueEnum(
          asObject(value, "ApiEnum").fields
            .get(propId)
            .flatMap(_.convertTo[Option[DamlLfIdentifier]]),
          assertDE(Ref.Name fromString strField(value, propConstructor, "ApiEnum")),
        )
      case t =>
        deserializationError(
          s"Can't read ${value.prettyPrint} as ApiEnum, type '$t' is not a enum"
        )
    }

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported for .parseJson and .toJson functions
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {
    implicit object ApiValueJsonFormat extends RootJsonFormat[Model.ApiValue] {
      def write(v: Model.ApiValue): JsValue = apiValueToJsValue(v)
      def read(v: JsValue): Model.ApiValue = jsValueToApiValue(v)
    }

    implicit object ApiRecordJsonFormat extends RootJsonFormat[Model.ApiRecord] {
      def write(v: Model.ApiRecord): JsValue = apiRecordToJsValue(v)
      def read(v: JsValue): Model.ApiRecord = jsValueToApiRecord(v)
    }

    implicit object ApiVariantJsonFormat extends RootJsonFormat[Model.ApiVariant] {
      def write(v: Model.ApiVariant): JsValue = apiVariantToJsValue(v)
      def read(v: JsValue): Model.ApiVariant = jsValueToApiVariant(v)
    }
  }

  private[this] def assertDE[A](ea: Either[String, A]): A =
    ea.fold(deserializationError(_), identity)
}
