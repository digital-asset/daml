// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value.json

import com.digitalasset.daml.lf.data.{Decimal => LfDecimal, FrontStack, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.ScalazEqual._
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.json.{NavigatorModelAliases => Model}
import Model.{DamlLfIdentifier, DamlLfType, DamlLfTypeLookup}
import ApiValueImplicits._

import spray.json._
import scalaz.syntax.std.string._

/**
  * A compressed encoding of API values.
  *
  * The encoded values do not include type information.
  * For example, it is impossible to distinguish party and text values in the encoded format.
  *
  * Therefore, this JSON format can only decode given a target type.
  *
  * [[ApiCodecCompressed.apiValueJsonReader]] can create a JSON reader with the necessary type information
  *
  * @param encodeDecimalAsString Not used yet.
  * @param encodeInt64AsString Not used yet.
  */
abstract class ApiCodecCompressed[Cid](
    val encodeDecimalAsString: Boolean,
    val encodeInt64AsString: Boolean) {

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def apiValueToJsValue(value: V[Cid]): JsValue = value match {
    case v: V.ValueRecord[Cid] => apiRecordToJsValue(v)
    case v: V.ValueVariant[Cid] => apiVariantToJsValue(v)
    case v: V.ValueEnum => apiEnumToJsValue(v)
    case v: V.ValueList[Cid] => apiListToJsValue(v)
    case V.ValueText(v) => JsString(v)
    case V.ValueInt64(v) => JsString((v: Long).toString)
    case V.ValueDecimal(v) => JsString(v.decimalToString)
    case V.ValueBool(v) => JsBoolean(v)
    case V.ValueContractId(v) => apiContractIdToJsValue(v)
    case t: V.ValueTimestamp => JsString(t.toIso8601)
    case d: V.ValueDate => JsString(d.toIso8601)
    case V.ValueParty(v) => JsString(v)
    case V.ValueUnit => JsObject.empty
    case V.ValueOptional(None) => JsNull
    case V.ValueOptional(Some(v)) =>
      v match {
        case V.ValueOptional(None) => JsArray()
        case V.ValueOptional(Some(_)) => JsArray(apiValueToJsValue(v))
        case _ => apiValueToJsValue(v)
      }
    case v: V.ValueMap[Cid] =>
      apiMapToJsValue(v)
    case _: V.ValueTuple[Cid] => serializationError("impossible! tuples are not serializable")
  }

  @throws[SerializationException]
  protected[this] def apiContractIdToJsValue(v: Cid): JsValue

  private[this] def apiListToJsValue(value: V.ValueList[Cid]): JsValue =
    JsArray(value.values.map(apiValueToJsValue(_)).toImmArray.toSeq: _*)

  private[this] def apiVariantToJsValue(value: V.ValueVariant[Cid]): JsValue =
    JsObject(Map((value.variant: String) -> apiValueToJsValue(value.value)))

  private[this] def apiEnumToJsValue(value: V.ValueEnum): JsValue =
    JsString(value.value)

  private[ApiCodecCompressed] def apiRecordToJsValue(value: V.ValueRecord[Cid]): JsValue =
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

  private[this] def apiMapToJsValue(value: V.ValueMap[Cid]): JsValue =
    JsObject(
      value.value.toImmArray
        .map { case (k, v) => k -> apiValueToJsValue(v) }
        .toSeq
        .toMap)

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding - this needs access to DAML-LF types
  // ------------------------------------------------------------------------------------------------------------------

  @throws[DeserializationException]
  protected[this] def jsValueToApiContractId(value: JsValue): Cid

  private[this] def jsValueToApiPrimitive(
      value: JsValue,
      prim: Model.DamlLfTypePrim,
      defs: Model.DamlLfTypeLookup): V[Cid] = {
    (prim.typ, value).match2 {
      case Model.DamlLfPrimType.Decimal => {
        case JsString(v) =>
          V.ValueDecimal(assertDE(LfDecimal fromString v))
        case JsNumber(v) =>
          V.ValueDecimal(assertDE(LfDecimal fromBigDecimal v))
      }
      case Model.DamlLfPrimType.Int64 => {
        case JsString(v) => V.ValueInt64(assertDE(v.parseLong.leftMap(_.getMessage).toEither))
        case JsNumber(v) if v.isValidLong => V.ValueInt64(v.toLongExact)
      }
      case Model.DamlLfPrimType.Text => { case JsString(v) => V.ValueText(v) }
      case Model.DamlLfPrimType.Party => {
        case JsString(v) =>
          V.ValueParty(assertDE(Ref.Party fromString v))
      }
      case Model.DamlLfPrimType.ContractId => {
        case v => V.ValueContractId(jsValueToApiContractId(v))
      }
      case Model.DamlLfPrimType.Unit => { case JsObject(_) => V.ValueUnit }
      case Model.DamlLfPrimType.Timestamp => {
        case JsString(v) => V.ValueTimestamp(assertDE(Time.Timestamp fromString v))
      }
      case Model.DamlLfPrimType.Date => { case JsString(v) => V.ValueDate.fromIso8601(v) }
      case Model.DamlLfPrimType.Bool => { case JsBoolean(v) => V.ValueBool(v) }
      case Model.DamlLfPrimType.List => {
        case JsArray(v) =>
          V.ValueList(v.map(e => jsValueToApiValue(e, prim.typArgs.head, defs)).to[FrontStack])
      }
      case Model.DamlLfPrimType.Optional =>
        val typArg = prim.typArgs.head
        val useArray = nestsOptional(prim);
        {
          case JsNull => V.ValueOptional(None)
          case JsArray(ov) if useArray =>
            ov match {
              case Seq() => V.ValueOptional(Some(V.ValueOptional(None)))
              case Seq(v) => V.ValueOptional(Some(jsValueToApiValue(v, typArg, defs)))
              case _ =>
                deserializationError(s"Can't read ${value.prettyPrint} as Optional of Optional")
            }
          case _ if !useArray => V.ValueOptional(Some(jsValueToApiValue(value, typArg, defs)))
        }
      case Model.DamlLfPrimType.Map => {
        case JsObject(a) =>
          V.ValueMap(SortedLookupList(a.transform { (_, v) =>
            jsValueToApiValue(v, prim.typArgs.head, defs)
          }))
      }
    }(fallback = deserializationError(s"Can't read ${value.prettyPrint} as $prim"))
  }

  private[this] def nestsOptional(prim: iface.TypePrim): Boolean =
    prim match {
      case iface.TypePrim(_, Seq(iface.TypePrim(iface.PrimType.Optional, _))) => true
      case _ => false
    }

  private[this] def jsValueToApiDataType(
      value: JsValue,
      id: DamlLfIdentifier,
      dt: Model.DamlLfDataType,
      defs: Model.DamlLfTypeLookup): V[Cid] = {
    (dt, value).match2 {
      case Model.DamlLfRecord(fields) => {
        case JsObject(v) =>
          V.ValueRecord(
            Some(id),
            fields.map { f =>
              val jsField = v
                .getOrElse(
                  f._1,
                  deserializationError(
                    s"Can't read ${value.prettyPrint} as DamlLfRecord $id, missing field '${f._1}'"))
              (Some(f._1), jsValueToApiValue(jsField, f._2, defs))
            }.toImmArray
          )
        case JsArray(fValues) =>
          if (fValues.length != fields.length)
            deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfRecord $id, wrong number of record fields")
          else
            V.ValueRecord(
              Some(id),
              (fields zip fValues).map {
                case ((fName, fTy), fValue) =>
                  (Some(fName), jsValueToApiValue(fValue, fTy, defs))
              }.toImmArray
            )
      }
      case Model.DamlLfVariant(cons) => {
        case JsObject(v) =>
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

          V.ValueVariant(
            Some(id),
            constructorName,
            jsValueToApiValue(constructor._2, constructorType, defs)
          )
      }
      case Model.DamlLfEnum(cons) => {
        case JsString(c) =>
          cons
            .collectFirst { case kc @ `c` => kc }
            .map(
              V.ValueEnum(
                Some(id),
                _
              ))
            .getOrElse(deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfEnum $id, unknown constructor $c"))
      }
    }(fallback = deserializationError(s"Can't read ${value.prettyPrint} as $dt"))
  }

  /** Deserialize a value, given the type */
  def jsValueToApiValue(
      value: JsValue,
      typ: Model.DamlLfType,
      defs: Model.DamlLfTypeLookup): V[Cid] = {
    typ match {
      case prim: Model.DamlLfTypePrim =>
        jsValueToApiPrimitive(value, prim, defs)
      case typeCon: Model.DamlLfTypeCon =>
        val id = typeCon.name.identifier
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
      defs: Model.DamlLfTypeLookup): V[Cid] = {
    val typeCon = Model.DamlLfTypeCon(Model.DamlLfTypeConName(id), ImmArraySeq())
    // val dt = typeCon.instantiate(defs(id).getOrElse(deserializationError(s"Type $id not found")))
    val dt = Model.damlLfInstantiate(
      typeCon,
      defs(id).getOrElse(deserializationError(s"Type $id not found")))
    jsValueToApiDataType(value, id, dt, defs)
  }

  /** Creates a [[JsonReader]] for arbitrary [[Model.ApiValue]]s with the relevant type information */
  def apiValueJsonReader(typ: DamlLfType, defs: DamlLfTypeLookup): JsonReader[V[Cid]] =
    jsValueToApiValue(_, typ, defs)

  /** Creates a [[JsonReader]] for arbitrary [[Model.ApiValue]]s with the relevant type information */
  def apiValueJsonReader(typ: DamlLfIdentifier, defs: DamlLfTypeLookup): JsonReader[V[Cid]] =
    jsValueToApiValue(_, typ, defs)

  /** Same as jsValueToApiType, but with unparsed input */
  def stringToApiType(value: String, typ: Model.DamlLfType, defs: Model.DamlLfTypeLookup): V[Cid] =
    jsValueToApiValue(value.parseJson, typ, defs)

  /** Same as jsValueToApiType, but with unparsed input */
  def stringToApiType(
      value: String,
      id: Model.DamlLfIdentifier,
      defs: Model.DamlLfTypeLookup): V[Cid] =
    jsValueToApiValue(value.parseJson, id, defs)

  private[this] def assertDE[A](ea: Either[String, A]): A =
    ea fold (deserializationError(_), identity)

}

object ApiCodecCompressed
    extends ApiCodecCompressed[String](encodeDecimalAsString = true, encodeInt64AsString = true) {

  override protected[this] def apiContractIdToJsValue(v: String): JsValue = JsString(v)

  override protected[this] def jsValueToApiContractId(value: JsValue): String = {
    import JsonImplicits.StringJsonFormat
    value.convertTo[String]
  }

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
