// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import com.digitalasset.daml.lf.data.{Decimal => LfDecimal, FrontStack, Ref, SortedLookupList}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.value.json.{NavigatorModelAliases => Model}
import Model.{ApiValue, DamlLfIdentifier, DamlLfType, DamlLfTypeLookup}
import spray.json._
import ApiValueImplicits._

/**
  * A compressed encoding of API values.
  *
  * The encoded values do not include type information.
  * For example, it is impossible to distinguish party and text values in the encoded format.
  *
  * Therefore, this JSON format only includes writers, and not readers.
  *
  * [[ApiCodecCompressed.apiValueJsonReader]] can create a JSON reader with the necessary type information
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
    case v: Model.ApiEnum => apiEnumToJsValue(v)
    case v: Model.ApiList => apiListToJsValue(v)
    case Model.ApiText(v) => JsString(v)
    case Model.ApiInt64(v) => JsString((v: Long).toString)
    case Model.ApiDecimal(v) => JsString(v.decimalToString)
    case Model.ApiBool(v) => JsBoolean(v)
    case Model.ApiContractId(v) => JsString(v)
    case t: Model.ApiTimestamp => JsString(t.toIso8601)
    case d: Model.ApiDate => JsString(d.toIso8601)
    case Model.ApiParty(v) => JsString(v)
    case Model.ApiUnit => JsObject.empty
    // Note: Optional needs to be boxed, otherwise the following values are indistinguishable:
    // None, Some(None), Some(Some(None)), ...
    case Model.ApiOptional(None) => JsObject(fieldNone -> JsObject.empty)
    case Model.ApiOptional(Some(v)) => JsObject(fieldSome -> apiValueToJsValue(v))
    case v: Model.ApiMap =>
      apiMapToJsValue(v)
    case _: Model.ApiImpossible => serializationError("impossible! tuples are not serializable")
  }

  private[this] def apiListToJsValue(value: Model.ApiList): JsValue =
    JsArray(value.values.map(apiValueToJsValue).toImmArray.toSeq: _*)

  private[this] def apiVariantToJsValue(value: Model.ApiVariant): JsValue =
    JsObject(Map((value.variant: String) -> apiValueToJsValue(value.value)))

  private[this] def apiEnumToJsValue(value: Model.ApiEnum): JsValue =
    JsString(value.value)

  private[this] def apiRecordToJsValue(value: Model.ApiRecord): JsValue =
    value match {
      case FullyNamedApiRecord(_, fields) =>
        JsObject(fields.toSeq.map {
          case (flabel, fvalue) => (flabel: String) -> apiValueToJsValue(fvalue)
        }.toMap)
      case _ =>
        JsArray(value.fields.toSeq.map {
          case (_, fvalue) => apiValueToJsValue(fvalue)
        }: _*)
    }

  private[this] def apiMapToJsValue(value: Model.ApiMap): JsValue =
    JsObject(
      value.value.toImmArray
        .map { case (k, v) => k -> apiValueToJsValue(v) }
        .toSeq
        .toMap)

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding - this needs access to DAML-LF types
  // ------------------------------------------------------------------------------------------------------------------

  private[this] def jsValueToApiPrimitive(
      value: JsValue,
      prim: Model.DamlLfTypePrim,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    (value, prim.typ) match {
      case (JsString(v), Model.DamlLfPrimType.Decimal) =>
        Model.ApiDecimal(assertDE(LfDecimal fromString v))
      case (JsString(v), Model.DamlLfPrimType.Int64) => Model.ApiInt64(v.toLong)
      case (JsString(v), Model.DamlLfPrimType.Text) => Model.ApiText(v)
      case (JsString(v), Model.DamlLfPrimType.Party) =>
        Model.ApiParty(assertDE(Ref.Party fromString v))
      case (JsString(v), Model.DamlLfPrimType.ContractId) => Model.ApiContractId(v)
      case (JsObject(_), Model.DamlLfPrimType.Unit) => Model.ApiUnit
      case (JsString(v), Model.DamlLfPrimType.Timestamp) => Model.ApiTimestamp.fromIso8601(v)
      case (JsString(v), Model.DamlLfPrimType.Date) => Model.ApiDate.fromIso8601(v)
      case (JsBoolean(v), Model.DamlLfPrimType.Bool) => Model.ApiBool(v)
      case (JsArray(v), Model.DamlLfPrimType.List) =>
        Model.ApiList(v.map(e => jsValueToApiValue(e, prim.typArgs.head, defs)).to[FrontStack])
      case (JsObject(f), Model.DamlLfPrimType.Optional) =>
        f.headOption match {
          case Some((`fieldNone`, _)) => Model.ApiOptional(None)
          case Some((`fieldSome`, v)) =>
            Model.ApiOptional(Some(jsValueToApiValue(v, prim.typArgs.head, defs)))
          case Some(_) => deserializationError(s"Can't read ${value.prettyPrint} as Optional")
          case None => deserializationError(s"Can't read ${value.prettyPrint} as Optional")
        }
      case (JsObject(a), Model.DamlLfPrimType.Map) =>
        Model.ApiMap(SortedLookupList(a.map {
          case (k, v) => k -> jsValueToApiValue(v, prim.typArgs.head, defs)
        }))
      case _ => deserializationError(s"Can't read ${value.prettyPrint} as $prim")
    }
  }

  private[this] def jsValueToApiDataType(
      value: JsValue,
      id: DamlLfIdentifier,
      dt: Model.DamlLfDataType,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    (value, dt) match {
      case (JsObject(v), Model.DamlLfRecord(fields)) =>
        Model.ApiRecord(
          Some(id),
          fields.map { f =>
            val jsField = v
              .getOrElse(
                f._1,
                deserializationError(
                  s"Can't read ${value.prettyPrint} as DamlLfRecord $id, missing field '${f._1}'"))
            Model.ApiRecordField(Some(f._1), jsValueToApiValue(jsField, f._2, defs))
          }.toImmArray
        )
      case (JsArray(fValues), Model.DamlLfRecord(fields)) =>
        if (fValues.length != fields.length)
          deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfRecord $id, wrong number of record fields")
        else
          Model.ApiRecord(
            Some(id),
            (fields zip fValues).map {
              case ((fName, fTy), fValue) =>
                (Some(fName), jsValueToApiValue(fValue, fTy, defs))
            }.toImmArray
          )
      case (JsObject(v), Model.DamlLfVariant(cons)) =>
        val constructor = v.toList match {
          case x :: Nil => x
          case _ =>
            deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfVariant $id, single constructor required")
        }
        val (constructorName, constructorType) = cons.toList
          .find(_._1 == constructor._1)
          .getOrElse(deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfVariant $id, unknown constructor ${constructor._1}"))

        Model.ApiVariant(
          Some(id),
          constructorName,
          jsValueToApiValue(constructor._2, constructorType, defs)
        )
      case (JsString(c), Model.DamlLfEnum(cons)) =>
        cons
          .collectFirst { case kc @ `c` => kc }
          .map(
            Model.ApiEnum(
              Some(id),
              _
            ))
          .getOrElse(deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfEnum $id, unknown constructor $c"))

      case _ => deserializationError(s"Can't read ${value.prettyPrint} as $dt")
    }
  }

  /** Deserialize a value, given the type */
  def jsValueToApiValue(
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
  def jsValueToApiValue(
      value: JsValue,
      id: Model.DamlLfIdentifier,
      defs: Model.DamlLfTypeLookup): Model.ApiValue = {
    val typeCon = Model.DamlLfTypeCon(Model.DamlLfTypeConName(id), ImmArraySeq())
    // val dt = typeCon.instantiate(defs(id).getOrElse(deserializationError(s"Type $id not found")))
    val dt = Model.damlLfInstantiate(
      typeCon,
      defs(id).getOrElse(deserializationError(s"Type $id not found")))
    jsValueToApiDataType(value, id, dt, defs)
  }

  /** Creates a [[JsonReader]] for arbitrary [[ApiValue]]s with the relevant type information */
  def apiValueJsonReader(typ: DamlLfType, defs: DamlLfTypeLookup): JsonReader[ApiValue] =
    jsValueToApiValue(_, typ, defs)

  /** Creates a [[JsonReader]] for arbitrary [[ApiValue]]s with the relevant type information */
  def apiValueJsonReader(typ: DamlLfIdentifier, defs: DamlLfTypeLookup): JsonReader[ApiValue] =
    jsValueToApiValue(_, typ, defs)

  /** Same as jsValueToApiType, but with unparsed input */
  def stringToApiType(
      value: String,
      typ: Model.DamlLfType,
      defs: Model.DamlLfTypeLookup): Model.ApiValue =
    jsValueToApiValue(value.parseJson, typ, defs)

  /** Same as jsValueToApiType, but with unparsed input */
  def stringToApiType(
      value: String,
      id: Model.DamlLfIdentifier,
      defs: Model.DamlLfTypeLookup): Model.ApiValue =
    jsValueToApiValue(value.parseJson, id, defs)

  private[this] def assertDE[A](ea: Either[String, A]): A =
    ea fold (deserializationError(_), identity)

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
