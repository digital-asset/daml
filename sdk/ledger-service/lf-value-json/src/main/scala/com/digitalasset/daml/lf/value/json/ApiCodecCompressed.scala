// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value.json

import com.digitalasset.daml.lf.data.{
  FrontStack,
  ImmArray,
  Ref,
  SortedLookupList,
  Time,
  Numeric => LfNumeric,
}
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.Value.ContractId
import ApiValueImplicits._
import com.digitalasset.daml.lf.language.{Ast, PackageInterface, TypeDestructor}
import spray.json._

import java.time.Instant
import scala.util.Try

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
private[digitalasset] class ApiCodecCompressed(
    val encodeDecimalAsString: Boolean,
    val encodeInt64AsString: Boolean,
)(implicit
    readCid: JsonReader[ContractId],
    writeCid: JsonWriter[ContractId],
) { self =>

  import ApiCodecCompressed._

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
    JsArray(value.values.map(apiValueToJsValue(_)).toImmArray.toSeq: _*)

  private[this] def apiVariantToJsValue(value: V.ValueVariant): JsValue =
    JsonVariant(value.variant, apiValueToJsValue(value.value))

  private[this] def apiEnumToJsValue(value: V.ValueEnum): JsValue =
    JsString(value.value)

  private[ApiCodecCompressed] def apiRecordToJsValue(value: V.ValueRecord): JsValue = {
    val namedOrNoneFields = value.fields.toSeq collect {
      case (Some(k), v) => Some((k, v))
      case (_, V.ValueOptional(None)) => None
    }
    if (namedOrNoneFields.length == value.fields.length)
      JsObject(namedOrNoneFields.iterator.collect { case Some((flabel, fvalue)) =>
        (flabel: String) -> apiValueToJsValue(fvalue)
      }.toMap)
    else
      JsArray(value.fields.toSeq.map { case (_, fvalue) =>
        apiValueToJsValue(fvalue)
      }: _*)
  }

  private[this] def apiMapToJsValue(value: V.ValueTextMap): JsValue =
    JsObject(
      value.value.iterator.map { case (key, value) => key -> apiValueToJsValue(value) }.toMap
    )

  private[this] def apiGenMapToJsValue(value: V.ValueGenMap): JsValue =
    JsArray(
      value.entries.map { case (key, value) =>
        JsArray(apiValueToJsValue(key), apiValueToJsValue(value))
      }.toSeq: _*
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding - this needs access to Daml-LF types
  // ------------------------------------------------------------------------------------------------------------------

  @throws[DeserializationException]
  private[this] final def jsValueToApiContractId(value: JsValue): ContractId =
    value.convertTo[ContractId]

  private def handleError[X](either: Either[TypeDestructor.Error, X]) =
    either match {
      case Right(value) => value
      case Left(TypeDestructor.Error.LookupError(error)) =>
        throw new Error(s"Lookup error: ${error.pretty}")
      case Left(TypeDestructor.Error.TypeError(msg)) =>
        throw new Error(s"Type error: $msg")
    }

  /** Deserialize a value, given the type */
  def jsValueToApiValue(
      value: JsValue,
      typ: Ast.Type,
      destructor: TypeDestructor,
  ): V =
    jsValueToApiValueF(value, handleError(destructor.destruct(typ)), typ, destructor)

  private[this] def jsValueToApiValueF(
      value: JsValue,
      typF: TypeDestructor.SerializableTypeF[Ast.Type],
      typ: Ast.Type,
      destructor: TypeDestructor,
  ): V = {
    (typF, value).match2 {
      case TypeDestructor.SerializableTypeF.UnitF => { case JsObject(_) =>
        V.ValueUnit
      }
      case TypeDestructor.SerializableTypeF.BoolF => { case JsBoolean(v) =>
        V.ValueBool(v)
      }
      case TypeDestructor.SerializableTypeF.Int64F => {
        case JsString(v) => V.ValueInt64(assertDE(Try(v.toLong).toEither.left.map(_.getMessage)))
        case JsNumber(v) if v.isValidLong => V.ValueInt64(v.toLongExact)
      }
      case TypeDestructor.SerializableTypeF.DateF => { case JsString(v) =>
        try {
          V.ValueDate.fromIso8601(v)
        } catch {
          case _: java.time.format.DateTimeParseException | _: IllegalArgumentException =>
            throw DeserializationException(s"Invalid date: $v")
        }
      }
      case TypeDestructor.SerializableTypeF.TimestampF => { case JsString(v) =>
        val optTimestamp = for {
          instant <- Try(Instant.parse(v)).toEither.left.map(_.getMessage)
          timestamp <- Time.Timestamp.fromInstant(instant)
        } yield timestamp
        V.ValueTimestamp(assertDE(optTimestamp))
      }
      case TypeDestructor.SerializableTypeF.NumericF(scale) => {
        case JsString(v) =>
          V.ValueNumeric(assertDE(LfNumeric.checkWithinBoundsAndRound(scale, BigDecimal(v))))
        case JsNumber(v) =>
          V.ValueNumeric(assertDE(LfNumeric.checkWithinBoundsAndRound(scale, v)))
        case _ =>
          deserializationError(s"Can't read ${value.prettyPrint} as (Numeric $scale)")
      }
      case TypeDestructor.SerializableTypeF.PartyF => { case JsString(v) =>
        V.ValueParty(assertDE(Ref.Party fromString v))
      }
      case TypeDestructor.SerializableTypeF.TextF => { case JsString(v) =>
        V.ValueText(v)
      }
      case TypeDestructor.SerializableTypeF.ContractIdF(_) => { case v =>
        V.ValueContractId(jsValueToApiContractId(v))
      }
      case TypeDestructor.SerializableTypeF.OptionalF(a) =>
        val aF = handleError(destructor.destruct(a))
        val useArray = aF match {
          case TypeDestructor.SerializableTypeF.OptionalF(_) => true
          case _ => false
        }
        {
          case JsNull => V.ValueNone
          case JsArray(ov) if useArray =>
            ov match {
              case Seq() => V.ValueOptional[Nothing](Some(V.ValueNone))
              case Seq(v) =>
                V.ValueOptional[Nothing](Some(jsValueToApiValueF(v, aF, a, destructor)))
              case _ =>
                deserializationError(s"Can't read ${value.prettyPrint} as Optional of Optional")
            }
          case _ if !useArray =>
            V.ValueOptional[Nothing](Some(jsValueToApiValueF(value, aF, a, destructor)))
        }
      case TypeDestructor.SerializableTypeF.ListF(a) => { case JsArray(v) =>
        V.ValueList[Nothing](
          v.iterator.map(e => jsValueToApiValue(e, a, destructor)).to(FrontStack)
        )
      }
      case TypeDestructor.SerializableTypeF.MapF(a, b) => { case JsArray(entries) =>
        val decEntries: Vector[(V, V)] = entries.map {
          case JsArray(Vector(key, value)) =>
            jsValueToApiValue(key, a, destructor) ->
              jsValueToApiValue(value, b, destructor)
          case _ =>
            deserializationError(s"Can't read ${value.prettyPrint} as key+value of ${typ.pretty}")
        }
        V.ValueGenMap[Nothing](decEntries.to(ImmArray))
      }
      case TypeDestructor.SerializableTypeF.TextMapF(a) => { case JsObject(m) =>
        V.ValueTextMap[Nothing](SortedLookupList.from(m.transform { (_, v) =>
          jsValueToApiValue(v, a, destructor)
        }))
      }
      case TypeDestructor.SerializableTypeF.RecordF(id, _, fieldNames, fieldTypes) =>
        val fields = (fieldNames zip fieldTypes)
        ({
          case JsObject(v) =>
            V.ValueRecord[Nothing](
              Some(id),
              fields
                .map { case (fName, fTy) =>
                  val fValue = v
                    .get(fName)
                    .map(jsValueToApiValue(_, fTy, destructor))
                    .getOrElse(handleError(destructor.destruct(fTy)) match {
                      case TypeDestructor.SerializableTypeF.OptionalF(_) => V.ValueNone
                      case _ =>
                        deserializationError(
                          s"Can't read ${value.prettyPrint} as DamlLfRecord $id, missing field '$fName'"
                        )
                    })
                  (Some(fName), fValue)
                }
                .to(ImmArray),
            )
          case JsArray(fValues) =>
            if (fValues.length != fields.length)
              deserializationError(
                s"Can't read ${value.prettyPrint} as DamlLfRecord $id, wrong number of record fields (expected ${fields.length}, found ${fValues.length})."
              )
            else
              V.ValueRecord[Nothing](
                Some(id),
                (fields zip fValues)
                  .map { case ((fName, fTy), fValue) =>
                    (Some(fName), jsValueToApiValue(fValue, fTy, destructor))
                  }
                  .to(ImmArray),
              )
        })
      case TypeDestructor.SerializableTypeF.VariantF(id, _, cons, consTypes) => {
        case JsonVariant(tag, nestedValue) =>
          val idx = cons.indexWhere(_ == tag)
          if (idx < 0)
            deserializationError(
              s"Can't read ${value.prettyPrint} as DamlLfVariant $id, unknown constructor $tag"
            )
          val constructorName = cons(idx)
          val constructorType = consTypes(idx)
          V.ValueVariant[Nothing](
            Some(id),
            constructorName,
            jsValueToApiValue(nestedValue, constructorType, destructor),
          )
        case _ =>
          deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfVariant $id, expected JsObject with 'tag' and 'value' fields"
          )
      }
      case TypeDestructor.SerializableTypeF.EnumF(id, pkgName, cons) => { case JsString(c) =>
        val idx = cons.indexWhere(_ == c)
        if (idx < 0)
          deserializationError(
            s"Can't read ${value.prettyPrint} as DamlLfEnum $id, unknown constructor $c"
          )
        val constructorName = cons(idx)
        V.ValueEnum(Some(id), constructorName)
      }
    }(fallback = deserializationError(s"Can't read ${value.prettyPrint} as ${typ.pretty}"))
  }

  /** Creates a JsonReader for Values with the relevant type information */
  def apiValueJsonReader(typ: Ast.Type, pkgIface: PackageInterface): JsonReader[V] =
    jsValueToApiValue(_, typ, TypeDestructor(pkgIface))

  /** Creates a JsonReader for Values with the relevant type information */
  def apiValueJsonReader(id: Ref.Identifier, pkgIface: PackageInterface): JsonReader[V] =
    jsValueToApiValue(_, Ast.TTyCon(id), TypeDestructor(pkgIface))

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
import JsonContractIdFormat._

object ApiCodecCompressed
    extends ApiCodecCompressed(encodeDecimalAsString = true, encodeInt64AsString = true) {
  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported to write JSON
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {
    implicit object ApiValueJsonFormat extends RootJsonWriter[V] {
      def write(v: V): JsValue = apiValueToJsValue(v)
    }
    implicit object ApiRecordJsonFormat extends RootJsonWriter[V.ValueRecord] {
      def write(v: V.ValueRecord): JsValue = apiRecordToJsValue(v)
    }
    implicit val ContractIdFormat: JsonFormat[ContractId] = JsonContractIdFormat.ContractIdFormat
  }

  implicit final class `Match2 syntax`[+A, +B](private val self: (A, B)) extends AnyVal {
    def match2[C](f: A => (B PartialFunction C))(fallback: => C): C =
      f(self._1).applyOrElse(self._2, (_: B) => fallback)
  }
}
