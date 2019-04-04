// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import com.digitalasset.daml.lf.data.UTF8
import com.digitalasset.navigator.{model => Model}
import com.digitalasset.navigator.json.DamlLfCodec.JsonImplicits._
import com.digitalasset.navigator.json.Util._
import com.digitalasset.navigator.model.DamlLfIdentifier
import spray.json._

/**
  * A verbose encoding of API values.
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
  private[this] final val tagDecimal: String = "decimal"
  private[this] final val tagBool: String = "bool"
  private[this] final val tagContractId: String = "contractid"
  private[this] final val tagTimestamp: String = "timestamp"
  private[this] final val tagDate: String = "date"
  private[this] final val tagParty: String = "party"
  private[this] final val tagUnit: String = "unit"
  private[this] final val tagOptional: String = "optional"
  private[this] final val tagList: String = "list"
  private[this] final val tagMap: String = "map"
  private[this] final val tagRecord: String = "record"
  private[this] final val tagVariant: String = "variant"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def apiValueToJsValue(value: Model.ApiValue): JsValue = value match {
    case v: Model.ApiRecord => apiRecordToJsValue(v)
    case v: Model.ApiVariant => apiVariantToJsValue(v)
    case v: Model.ApiList => apiListToJsValue(v)
    case Model.ApiText(v) => JsObject(propType -> JsString(tagText), propValue -> JsString(v))
    case Model.ApiInt64(v) =>
      JsObject(propType -> JsString(tagInt64), propValue -> JsString(v.toString))
    case Model.ApiDecimal(v) => JsObject(propType -> JsString(tagDecimal), propValue -> JsString(v))
    case Model.ApiBool(v) => JsObject(propType -> JsString(tagBool), propValue -> JsBoolean(v))
    case Model.ApiContractId(v) =>
      JsObject(propType -> JsString(tagContractId), propValue -> JsString(v.toString))
    case v: Model.ApiTimestamp =>
      JsObject(propType -> JsString(tagTimestamp), propValue -> JsString(v.toIso8601))
    case v: Model.ApiDate =>
      JsObject(propType -> JsString(tagDate), propValue -> JsString(v.toIso8601))
    case Model.ApiParty(v) =>
      JsObject(propType -> JsString(tagParty), propValue -> JsString(v.toString))
    case Model.ApiUnit() => JsObject(propType -> JsString(tagUnit))
    case Model.ApiOptional(None) => JsObject(propType -> JsString(tagOptional), propValue -> JsNull)
    case Model.ApiOptional(Some(v)) =>
      JsObject(propType -> JsString(tagOptional), propValue -> apiValueToJsValue(v))
    case v: Model.ApiMap => apiMapToJsValue(v)
  }

  def apiListToJsValue(value: Model.ApiList): JsValue =
    JsObject(
      propType -> JsString(tagList),
      propValue -> JsArray(value.elements.map(apiValueToJsValue).toVector)
    )

  private[this] val fieldKey = "key"
  private[this] val fieldValue = "value"

  def apiMapToJsValue(value: Model.ApiMap): JsValue =
    JsObject(
      propType -> JsString(tagMap),
      propValue -> JsArray(value.value.toVector.sortBy(_._1)(UTF8.ordering).map{
        case (k, v) => JsObject(fieldKey -> JsString(k), fieldValue -> apiValueToJsValue(v))
      })
    )

  def apiVariantToJsValue(value: Model.ApiVariant): JsValue =
    JsObject(
      propType -> JsString(tagVariant),
      propId -> value.variantId.map(_.toJson).getOrElse(JsNull),
      propConstructor -> JsString(value.constructor),
      propValue -> apiValueToJsValue(value.value)
    )

  def apiRecordToJsValue(value: Model.ApiRecord): JsValue =
    JsObject(
      propType -> JsString(tagRecord),
      propId -> value.recordId.map(_.toJson).getOrElse(JsNull),
      propFields -> JsArray(
        value.fields
          .map(
            f =>
              JsObject(
                propLabel -> JsString(f.label),
                propValue -> apiValueToJsValue(f.value)
            ))
          .toVector)
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------

  private[this] def jsValueToApiRecordField(value: JsValue): Model.ApiRecordField = {
    val label = strField(value, propLabel, "ApiRecordField")
    val avalue = jsValueToApiValue(anyField(value, propValue, "ApiRecordField"))
    Model.ApiRecordField(label, avalue)
  }

  def jsValueToApiValue(value: JsValue): Model.ApiValue =
    strField(value, propType, "ApiValue") match {
      case `tagRecord` => jsValueToApiRecord(value)
      case `tagVariant` => jsValueToApiVariant(value)
      case `tagList` =>
        Model.ApiList(arrayField(value, propValue, "ApiList").map(jsValueToApiValue))
      case `tagText` => Model.ApiText(strField(value, propValue, "ApiText"))
      case `tagInt64` => Model.ApiInt64(strField(value, propValue, "ApiInt64").toLong)
      case `tagDecimal` => Model.ApiDecimal(strField(value, propValue, "ApiDecimal"))
      case `tagBool` => Model.ApiBool(boolField(value, propValue, "ApiBool"))
      case `tagContractId` => Model.ApiContractId(strField(value, propValue, "ApiContractId"))
      case `tagTimestamp` =>
        Model.ApiTimestamp.fromIso8601(strField(value, propValue, "ApiTimestamp"))
      case `tagDate` => Model.ApiDate.fromIso8601(strField(value, propValue, "ApiDate"))
      case `tagParty` => Model.ApiParty(strField(value, propValue, "ApiParty"))
      case `tagUnit` => Model.ApiUnit()
      case `tagOptional` =>
        anyField(value, propValue, "ApiOptional") match {
          case JsNull => Model.ApiOptional(None)
          case v => Model.ApiOptional(Some(jsValueToApiValue(v)))
        }
      case `tagMap` =>
        Model.ApiMap(arrayField(value, propValue, "ApiMap").map(jsValueToMapEntry).toMap)
      case t =>
        deserializationError(s"Can't read ${value.prettyPrint} as ApiValue, unknown type '$t'")
    }

  def jsValueToApiRecord(value: JsValue): Model.ApiRecord =
    strField(value, propType, "ApiRecord") match {
      case `tagRecord` =>
        Model.ApiRecord(
          asObject(value, "ApiRecord").fields
            .get(propId)
            .flatMap(_.convertTo[Option[DamlLfIdentifier]]),
          arrayField(value, propFields, "ApiRecord").map(jsValueToApiRecordField)
        )
      case t =>
        deserializationError(
          s"Can't read ${value.prettyPrint} as ApiRecord, type '$t' is not a record")
    }

  def jsValueToMapEntry(value: JsValue): (String, Model.ApiValue) = {
    val translation = value match {
      case JsObject(map)  => for {
        key <- map.get(fieldKey).collect { case JsString(s) => s }
        value <- map.get(fieldValue).map(jsValueToApiValue)
      } yield key -> value
      case _ => None
    }

    translation.getOrElse( deserializationError(s"Can't read ${value.prettyPrint} as a map entry"))
  }


  def jsValueToApiVariant(value: JsValue): Model.ApiVariant =
    strField(value, propType, "ApiVariant") match {
      case `tagVariant` =>
        Model.ApiVariant(
          asObject(value, "ApiVariant").fields
            .get(propId)
            .flatMap(_.convertTo[Option[DamlLfIdentifier]]),
          strField(value, propConstructor, "ApiVariant"),
          jsValueToApiValue(anyField(value, propValue, "ApiVariant"))
        )
      case t =>
        deserializationError(
          s"Can't read ${value.prettyPrint} as ApiVariant, type '$t' is not a variant")
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
}
