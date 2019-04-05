// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import com.digitalasset.daml.lf.data.UTF8
import com.digitalasset.navigator.model.DamlLfIdentifier
import com.digitalasset.navigator.{model => Model}
import spray.json._

/**
  * A compressed encoding of API values.
  *
  * The encoded values do not include type information.
  * For example, it is impossible to distinguish party and text values in the encoded format.
  *
  * Therefore, this JSON format only includes writers, and not readers.
  */
object ApiCodecCompressed {
  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  private[this] final val fieldSome: String = "Some"
  private[this] final val fieldNone: String = "None"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def apiValueToJsValue(value: Model.ApiValue): JsValue = value match {
    case v: Model.ApiRecord => apiRecordToJsValue(v)
    case v: Model.ApiVariant => apiVariantToJsValue(v)
    case v: Model.ApiList => apiListToJsValue(v)
    case Model.ApiText(v) => JsString(v)
    case Model.ApiInt64(v) => JsString(v.toString)
    case Model.ApiDecimal(v) => JsString(v)
    case Model.ApiBool(v) => JsBoolean(v)
    case Model.ApiContractId(v) => JsString(v.toString)
    case t: Model.ApiTimestamp => JsString(t.toIso8601)
    case d: Model.ApiDate => JsString(d.toIso8601)
    case Model.ApiParty(v) => JsString(v.toString)
    case Model.ApiUnit() => JsObject.empty
    // Note: Optional needs to be boxed, otherwise the following values are indistinguishable:
    // None, Some(None), Some(Some(None)), ...
    case Model.ApiOptional(None) => JsObject(fieldNone -> JsObject.empty)
    case Model.ApiOptional(Some(v)) => JsObject(fieldSome -> apiValueToJsValue(v))
    case v: Model.ApiMap =>
      apiMapToJsValue(v)
  }

  def apiListToJsValue(value: Model.ApiList): JsValue =
    JsArray(value.elements.map(apiValueToJsValue).toVector)

  def apiVariantToJsValue(value: Model.ApiVariant): JsValue =
    JsObject(Map(value.constructor -> apiValueToJsValue(value.value)))

  def apiRecordToJsValue(value: Model.ApiRecord): JsValue =
    JsObject(value.fields.map(f => f.label -> apiValueToJsValue(f.value)).toMap)

  private[this] val fieldKey = "key"
  private[this] val fieldValue = "value"

  def apiMapToJsValue(value: Model.ApiMap): JsValue =
    JsArray(value.value.toVector.sortBy(_._1)(UTF8.ordering).map {
      case (k, v) => JsObject(fieldKey -> JsString(k), fieldValue -> apiValueToJsValue(v))
    })

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding - this needs access to DAML-LF types
  // ------------------------------------------------------------------------------------------------------------------

  def jsValueToApiPrimitive(
      value: JsValue,
      prim: Model.DamlLfTypePrim,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    (value, prim.typ) match {
      case (JsString(v), Model.DamlLfPrimType.Decimal) => Model.ApiDecimal(v)
      case (JsString(v), Model.DamlLfPrimType.Int64) => Model.ApiInt64(v.toLong)
      case (JsString(v), Model.DamlLfPrimType.Text) => Model.ApiText(v)
      case (JsString(v), Model.DamlLfPrimType.Party) => Model.ApiParty(v)
      case (JsString(v), Model.DamlLfPrimType.ContractId) => Model.ApiContractId(v)
      case (JsObject(_), Model.DamlLfPrimType.Unit) => Model.ApiUnit()
      case (JsString(v), Model.DamlLfPrimType.Timestamp) => Model.ApiTimestamp.fromIso8601(v)
      case (JsString(v), Model.DamlLfPrimType.Date) => Model.ApiDate.fromIso8601(v)
      case (JsBoolean(v), Model.DamlLfPrimType.Bool) => Model.ApiBool(v)
      case (JsArray(v), Model.DamlLfPrimType.List) =>
        Model.ApiList(v.toList.map(e => jsValueToApiType(e, prim.typArgs.head, defs)))
      case (JsObject(f), Model.DamlLfPrimType.Optional) =>
        f.headOption match {
          case Some((`fieldNone`, _)) => Model.ApiOptional(None)
          case Some((`fieldSome`, v)) =>
            Model.ApiOptional(Some(jsValueToApiType(v, prim.typArgs.head, defs)))
          case Some(_) => deserializationError(s"Can't read ${value.prettyPrint} as Optional")
          case None => deserializationError(s"Can't read ${value.prettyPrint} as Optional")
        }
      case (JsArray(a), Model.DamlLfPrimType.Map) =>
        Model.ApiMap(a.map(jsValueToMapEntry(_, prim.typArgs.head, defs)).toMap)
      case _ => deserializationError(s"Can't read ${value.prettyPrint} as $prim")
    }
  }

  def jsValueToApiDataType(
      value: JsValue,
      id: DamlLfIdentifier,
      dt: Model.DamlLfDataType,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    (value, dt) match {
      case (JsObject(v), Model.DamlLfRecord(fields)) =>
        Model.ApiRecord(
          Some(id),
          fields.toList.map(f => {
            val jsField = v
              .getOrElse(
                f._1,
                deserializationError(
                  s"Can't read ${value.prettyPrint} as DamlLfRecord $id, missing field '${f._1}'"))
            Model.ApiRecordField(f._1, jsValueToApiType(jsField, f._2, defs))
          })
        )
      case (JsObject(v), Model.DamlLfVariant(cons)) =>
        val constructor = v.toList match {
          case x :: Nil => x
          case _ =>
            deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfVariant $id, single constructor required")
        }
        val constructorType = cons.toList
          .find(_._1 == constructor._1)
          .map(_._2)
          .getOrElse(deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfVariant $id, unknown constructor ${constructor._1}"))

        Model.ApiVariant(
          Some(id),
          constructor._1,
          jsValueToApiType(constructor._2, constructorType, defs)
        )

      case _ => deserializationError(s"Can't read ${value.prettyPrint} as $dt")
    }
  }

  /** Deserialize a value, to a map entry give the type */
  def jsValueToMapEntry(
      value: JsValue,
      typ: Model.DamlLfType,
      defs: Model.DamlLfTypeLookup): (String, Model.ApiValue) = {
    val translation = value match {
      case JsObject(map) =>
        for {
          key <- map.get(fieldKey).collect { case JsString(s) => s }
          value <- map.get(fieldValue).map(jsValueToApiType(_, typ, defs))
        } yield key -> value
      case _ => None
    }

    translation.getOrElse(deserializationError(s"Can't read ${value.prettyPrint} as a map entry"))
  }

  /** Deserialize a value, given the type */
  def jsValueToApiType(
      value: JsValue,
      typ: Model.DamlLfType,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    typ match {
      case prim: Model.DamlLfTypePrim =>
        jsValueToApiPrimitive(value, prim, defs)
      case typeCon: Model.DamlLfTypeCon =>
        val id = Model.DamlLfIdentifier(
          typeCon.name.identifier.packageId,
          typeCon.name.identifier.qualifiedName)
        // val dt = typeCon.instantiate(defs(id).getOrElse(deserializationError(s"Type $id not found")))
        val dt = Model.damlLfInstantiate(
          typeCon,
          defs(id).getOrElse(deserializationError(s"Type $id not found")))
        jsValueToApiDataType(value, id, dt, defs)
      case v: Model.DamlLfTypeVar =>
        deserializationError(s"Can't read ${value.prettyPrint} as DamlLfTypeVar")
    }
  }

  /** Deserialize a value, given the ID of the corresponding closed type */
  def jsValueToApiType(
      value: JsValue,
      id: Model.DamlLfIdentifier,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    val typeCon = Model.DamlLfTypeCon(Model.DamlLfTypeConName(id), Model.DamlLfImmArraySeq())
    // val dt = typeCon.instantiate(defs(id).getOrElse(deserializationError(s"Type $id not found")))
    val dt = Model.damlLfInstantiate(
      typeCon,
      defs(id).getOrElse(deserializationError(s"Type $id not found")))
    jsValueToApiDataType(value, id, dt, defs)
  }

  /** Same as jsValueToApiType, but with unparsed input */
  def stringToApiType(
      value: String,
      typ: Model.DamlLfType,
      defs: Model.DamlLfTypeLookup): Model.ApiValue =
    jsValueToApiType(value.parseJson, typ, defs)

  /** Same as jsValueToApiType, but with unparsed input */
  def stringToApiType(
      value: String,
      id: Model.DamlLfIdentifier,
      defs: Model.DamlLfTypeLookup): Model.ApiValue =
    jsValueToApiType(value.parseJson, id, defs)

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported to write JSON
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {
    implicit object ApiValueJsonFormat extends RootJsonWriter[Model.ApiValue] {
      def write(v: Model.ApiValue): JsValue = apiValueToJsValue(v)
    }
    implicit object ApiRecordJsonFormat extends RootJsonWriter[Model.ApiRecord] {
      def write(v: Model.ApiRecord): JsValue = apiRecordToJsValue(v)
    }
  }
}
