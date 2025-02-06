// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.daml.lf.value.json

import com.digitalasset.canton.daml.lf.value.json.NavigatorModelAliases as Model
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.ScalazEqual.*
import com.digitalasset.daml.lf.data.{
  FrontStack,
  ImmArray,
  Numeric as LfNumeric,
  Ref,
  SortedLookupList,
  Time,
}
import com.digitalasset.daml.lf.typesig
import com.digitalasset.daml.lf.value.Value as V
import com.digitalasset.daml.lf.value.Value.ContractId
import scalaz.syntax.std.string.*
import scalaz.{@@, Tag}
import spray.json.*

import java.time.Instant
import scala.annotation.nowarn
import scala.util.Try

import NavigatorModelAliases.DamlLfIdentifier
import ApiValueImplicits.*

/** A compressed encoding of API values.
  *
  * The encoded values do not include type information.
  * For example, it is impossible to distinguish party and text values in the encoded format.
  *
  * Therefore, this JSON format can only decode given a target type.
  *
  * `apiValueJsonReader` can create a JSON reader with the necessary type information.
  *
  * @param encodeDecimalAsString Not used yet.
  * @param encodeInt64AsString Not used yet.
  */
class ApiCodecCompressed(val encodeDecimalAsString: Boolean, val encodeInt64AsString: Boolean)(
    implicit
    readCid: JsonReader[ContractId],
    writeCid: JsonWriter[ContractId],
) { self =>

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def apiValueToJsValue(value: V): JsValue = value match {
    case v: V.ValueRecord => apiRecordToJsValue(v)
    case v: V.ValueVariant => apiVariantToJsValue(v)
    case v: V.ValueEnum => apiEnumToJsValue(v)
    case v: V.ValueList => apiListToJsValue(v)
    case V.ValueText(v) => JsString(v)
    case V.ValueInt64(v) => if (encodeInt64AsString) JsString((v: Long).toString) else JsNumber(v)
    case V.ValueNumeric(v) =>
      if (encodeDecimalAsString) JsString(LfNumeric.toUnscaledString(v)) else JsNumber(v)
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
    case textMap: V.ValueTextMap =>
      apiMapToJsValue(textMap)
    case genMap: V.ValueGenMap =>
      apiGenMapToJsValue(genMap)
  }

  @throws[SerializationException]
  private[this] final def apiContractIdToJsValue(v: ContractId): JsValue = v.toJson

  private[this] def apiListToJsValue(value: V.ValueList): JsValue =
    JsArray(value.values.map(apiValueToJsValue(_)).toImmArray.toSeq*)

  private[this] def apiVariantToJsValue(value: V.ValueVariant): JsValue =
    JsonVariant(value.variant, apiValueToJsValue(value.value))

  private[this] def apiEnumToJsValue(value: V.ValueEnum): JsValue =
    JsString(value.value)

  private[ApiCodecCompressed] def apiRecordToJsValue(value: V.ValueRecord): JsValue = {
    val namedOrNoneFields = value.fields.toSeq collect {
      case (Some(k), v) => Some((k, v))
      case (_, V.ValueOptional(None)) => None
    }
    if (namedOrNoneFields.lengthIs == value.fields.length)
      JsObject(namedOrNoneFields.iterator.collect { case Some((flabel, fvalue)) =>
        (flabel: String) -> apiValueToJsValue(fvalue)
      }.toMap)
    else
      JsArray(value.fields.toSeq.map { case (_, fvalue) =>
        apiValueToJsValue(fvalue)
      }*)
  }

  private[this] def apiMapToJsValue(value: V.ValueTextMap): JsValue =
    JsObject(
      value.value
        .mapValue(apiValueToJsValue)
        .toHashMap
    )

  private[this] def apiGenMapToJsValue(value: V.ValueGenMap): JsValue =
    JsArray(
      value.entries.map { case (key, value) =>
        JsArray(apiValueToJsValue(key), apiValueToJsValue(value))
      }.toSeq*
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding - this needs access to Daml-LF types
  // ------------------------------------------------------------------------------------------------------------------

  @throws[DeserializationException]
  private[this] final def jsValueToApiContractId(value: JsValue): ContractId =
    value.convertTo[ContractId]

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  private[this] def jsValueToApiPrimitive(
      value: JsValue,
      prim: Model.DamlLfTypePrim,
      defs: Model.DamlLfTypeLookup,
  ): V =
    (prim.typ, value).match2 {
      case Model.DamlLfPrimType.Int64 => {
        case JsString(v) => V.ValueInt64(assertDE(v.parseLong.leftMap(_.getMessage).toEither))
        case JsNumber(v) if v.isValidLong => V.ValueInt64(v.toLongExact)
      }
      case Model.DamlLfPrimType.Text => { case JsString(v) => V.ValueText(v) }
      case Model.DamlLfPrimType.Party => { case JsString(v) =>
        V.ValueParty(assertDE(Ref.Party fromString v))
      }
      case Model.DamlLfPrimType.ContractId => { case v =>
        V.ValueContractId(jsValueToApiContractId(v))
      }
      case Model.DamlLfPrimType.Unit => { case JsObject(_) => V.ValueUnit }
      case Model.DamlLfPrimType.Timestamp => { case JsString(v) =>
        val optTimestamp = for {
          instant <- Try(Instant.parse(v)).toEither.left.map(_.getMessage)
          timestamp <- Time.Timestamp.fromInstant(instant)
        } yield timestamp
        V.ValueTimestamp(assertDE(optTimestamp))
      }
      case Model.DamlLfPrimType.Date => { case JsString(v) =>
        try {
          V.ValueDate.fromIso8601(v)
        } catch {
          case _: java.time.format.DateTimeParseException | _: IllegalArgumentException =>
            throw DeserializationException(s"Invalid date: $v")
        }
      }
      case Model.DamlLfPrimType.Bool => { case JsBoolean(v) => V.ValueBool(v) }
      case Model.DamlLfPrimType.List => { case JsArray(v) =>
        V.ValueList(
          v.iterator.map(e => jsValueToApiValue(e, prim.typArgs.head, defs)).to(FrontStack)
        )
      }
      case Model.DamlLfPrimType.Optional =>
        val typArg = prim.typArgs.head
        val useArray = nestsOptional(prim)

        {
          case JsNull => V.ValueNone
          case JsArray(ov) if useArray =>
            ov match {
              case Seq() => V.ValueOptional(Some(V.ValueNone))
              case Seq(v) => V.ValueOptional(Some(jsValueToApiValue(v, typArg, defs)))
              case _ =>
                deserializationError(s"Can't read ${value.prettyPrint} as Optional of Optional")
            }
          case _ if !useArray => V.ValueOptional(Some(jsValueToApiValue(value, typArg, defs)))
        }
      case Model.DamlLfPrimType.TextMap => { case JsObject(a) =>
        V.ValueTextMap(SortedLookupList(a.transform { (_, v) =>
          jsValueToApiValue(v, prim.typArgs.head, defs)
        }))
      }
      case Model.DamlLfPrimType.GenMap =>
        val Seq(kType, vType) = prim.typArgs: @nowarn("msg=match may not be exhaustive")

        { case JsArray(entries) =>
          type OK[K] = Vector[(K, V)]
          val decEntries: Vector[(V @@ defs.type, V)] = Tag
            .subst[V, OK, defs.type](entries.map {
              case JsArray(Vector(key, value)) =>
                jsValueToApiValue(key, kType, defs) ->
                  jsValueToApiValue(value, vType, defs)
              case _ =>
                deserializationError(s"Can't read ${value.prettyPrint} as key+value of $prim")
            })
          V.ValueGenMap(Tag.unsubst[V, OK, defs.type](decEntries).to(ImmArray))
        }

    }(fallback = deserializationError(s"Can't read ${value.prettyPrint} as $prim"))

  private[this] def nestsOptional(prim: typesig.TypePrim): Boolean =
    prim match {
      case typesig.TypePrim(_, Seq(typesig.TypePrim(typesig.PrimType.Optional, _))) => true
      case _ => false
    }

  private[this] def jsValueToApiDataType(
      value: JsValue,
      id: DamlLfIdentifier,
      dt: Model.DamlLfDataType,
      defs: Model.DamlLfTypeLookup,
  ): V =
    (dt, value).match2 {
      case Model.DamlLfRecord(fields) => {
        case JsObject(v) =>
          V.ValueRecord(
            Some(id),
            fields.map { case (fName, fTy) =>
              val fValue = v
                .get(fName)
                .map(jsValueToApiValue(_, fTy, defs))
                .getOrElse(fTy match {
                  case typesig.TypePrim(typesig.PrimType.Optional, _) => V.ValueNone
                  case _ =>
                    deserializationError(
                      s"Can't read ${value.prettyPrint} as DamlLfRecord $id, missing field '$fName'"
                    )
                })
              (Some(fName), fValue)
            }.toImmArray,
          )
        case JsArray(fValues) =>
          if (fValues.sizeCompare(fields) != 0)
            deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfRecord $id, wrong number of record fields (expected ${fields.length}, found ${fValues.length})."
            )
          else
            V.ValueRecord(
              Some(id),
              (fields zip fValues).map { case ((fName, fTy), fValue) =>
                (Some(fName), jsValueToApiValue(fValue, fTy, defs))
              }.toImmArray,
            )
      }
      case Model.DamlLfVariant(cons) => {
        case JsonVariant(tag, nestedValue) =>
          val (constructorName, constructorType) = cons.toList
            .find(_._1 == tag)
            .getOrElse(
              deserializationError(
                s"Can't read ${value.compactPrint} as DamlLfVariant $id, unknown constructor $tag"
              )
            )

          V.ValueVariant(
            Some(id),
            constructorName,
            jsValueToApiValue(nestedValue, constructorType, defs),
          )
        case _ =>
          deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfVariant $id, expected JsObject with 'tag' and 'value' fields"
          )
      }
      case Model.DamlLfEnum(cons) => { case JsString(c) =>
        cons
          .collectFirst { case kc if kc == c => kc }
          .map(
            V.ValueEnum(
              Some(id),
              _,
            )
          )
          .getOrElse(
            deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfEnum $id, unknown constructor $c"
            )
          )
      }
    }(fallback = deserializationError(s"Can't read ${value.prettyPrint} as $dt"))

  /** Deserialize a value, given the type */
  def jsValueToApiValue(
      value: JsValue,
      typ: Model.DamlLfType,
      defs: Model.DamlLfTypeLookup,
  ): V =
    typ match {
      case prim: Model.DamlLfTypePrim =>
        jsValueToApiPrimitive(value, prim, defs)
      case typeCon: Model.DamlLfTypeCon =>
        val id = typeCon.name.identifier
        val dt =
          typeCon.instantiate(defs(id).getOrElse(deserializationError(s"Type $id not found")))
        jsValueToApiDataType(value, id, dt, defs)
      case Model.DamlLfTypeNumeric(scale) =>
        val numericOrError = value match {
          case JsString(v) =>
            LfNumeric.checkWithinBoundsAndRound(scale, BigDecimal(v))
          case JsNumber(v) =>
            LfNumeric.checkWithinBoundsAndRound(scale, v)
          case _ =>
            deserializationError(s"Can't read ${value.prettyPrint} as (Numeric $scale)")
        }
        V.ValueNumeric(assertDE(numericOrError))
      case Model.DamlLfTypeVar(_) =>
        deserializationError(s"Can't read ${value.prettyPrint} as DamlLfTypeVar")
    }

  /** Deserialize a value, given the ID of the corresponding closed type */
  def jsValueToApiValue(
      value: JsValue,
      id: Model.DamlLfIdentifier,
      defs: Model.DamlLfTypeLookup,
  ): V = {
    val typeCon = Model.DamlLfTypeCon(Model.DamlLfTypeConName(id), ImmArraySeq())
    val dt = typeCon.instantiate(defs(id).getOrElse(deserializationError(s"Type $id not found")))
    jsValueToApiDataType(value, id, dt, defs)
  }

  private[this] def assertDE[A](ea: Either[String, A]): A =
    ea.fold(deserializationError(_), identity)

  private[json] def copy(
      encodeDecimalAsString: Boolean = this.encodeDecimalAsString,
      encodeInt64AsString: Boolean = this.encodeInt64AsString,
  ): ApiCodecCompressed =
    new ApiCodecCompressed(
      encodeDecimalAsString = encodeDecimalAsString,
      encodeInt64AsString = encodeInt64AsString,
    )
}

private[json] object JsonContractIdFormat {
  implicit val ContractIdFormat: JsonFormat[ContractId] =
    new JsonFormat[ContractId] {
      override def write(obj: ContractId) =
        JsString(obj.coid)
      override def read(json: JsValue) = json match {
        case JsString(s) =>
          ContractId.fromString(s).fold(deserializationError(_), identity)
        case _ => deserializationError("ContractId must be a string")
      }
    }
}
import JsonContractIdFormat.*

object ApiCodecCompressed
    extends ApiCodecCompressed(encodeDecimalAsString = true, encodeInt64AsString = true) {
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

    implicit val ContractIdFormat: JsonFormat[ContractId] = JsonContractIdFormat.ContractIdFormat
  }
}
