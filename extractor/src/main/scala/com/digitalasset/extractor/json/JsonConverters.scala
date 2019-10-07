// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.json

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.extractor.ledger.types.{Identifier, LedgerValue}
import com.digitalasset.extractor.ledger.types.LedgerValue._
import com.digitalasset.extractor.writers.postgresql.DataFormatState.MultiTableState
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import scalaz.@@

object JsonConverters {
  private[this] object LfValueSprayEnc
      extends ApiCodecCompressed[String](
        encodeDecimalAsString = true,
        encodeInt64AsString = false
      ) {
    import spray.json._, ApiCodecCompressed.JsonImplicits.StringJsonFormat
    override protected[this] def apiContractIdToJsValue(v: String): JsValue = JsString(v)
    override protected[this] def jsValueToApiContractId(value: JsValue): String =
      value.convertTo[String]
  }

  private[this] def sprayToCirce(s: spray.json.JsValue): Json = {
    import spray.{json => sj}
    s match {
      case sj.JsString(v) => Json fromString v
      case sj.JsNumber(v) => Json fromBigDecimal v
      case sj.JsBoolean(v) => Json fromBoolean v
      case sj.JsObject(v) => Json fromFields (v transform ((_, e) => sprayToCirce(e)))
      case sj.JsArray(v) => Json fromValues (v map sprayToCirce)
      case sj.JsNull => Json.Null
    }
  }

  def toJsonString[A: Encoder](a: A): String = {
    a.asJson.noSpaces
  }

  implicit val recordEncoder: Encoder[OfCid[V.ValueRecord]] = record =>
    if (record.fields.toSeq.forall(_._1.isDefined))
      JsonObject(
        record.fields.toSeq
          .collect {
            case (Some(label), value) =>
              label -> value.asJson
          }: _*
      ).asJson
    else record.fields.toSeq.map(_.asJson).asJson

  private val emptyRecord = V.ValueRecord(None, ImmArray.empty).asJson

  // TODO it might be much more performant if exploded into separate vals
  implicit def valueEncoder[T <: LedgerValue]: Encoder[T] =
    t => sprayToCirce(LfValueSprayEnc.apiValueToJsValue(t))

  private implicit def frontStackEncoder[A: Encoder]: Encoder[FrontStack[A]] =
    _.toImmArray.map(_.asJson).toSeq.asJson

  private implicit val variantEncoder: Encoder[OfCid[V.ValueVariant]] = {
    case V.ValueVariant(tycon @ _, ctor, value) =>
      JsonObject(
        ctor -> value.asJson
      ).asJson
  }

  implicit val scalaOptionEncoder: Encoder[Option[LedgerValue]] = _ match {
    case None =>
      JsonObject("None" -> emptyRecord).asJson
    case Some(value) =>
      JsonObject("Some" -> value.asJson).asJson
  }

  implicit val mapEncoder: Encoder[SortedLookupList[LedgerValue]] = m =>
    JsonObject(
      "Map" ->
        JsonObject
          .fromIterable(m.toImmArray.map { case (k, v) => k -> v.asJson }.toSeq)
          .asJson).asJson

  private implicit val idKeyEncoder: KeyEncoder[Identifier] = id => s"${id.packageId}@${id.name}"
  private implicit val idKeyDecoder: KeyDecoder[Identifier] = StringEncodedIdentifier.unapply

  private implicit val idEncoder: Encoder[Identifier] = deriveEncoder[Identifier]
  private implicit val idDecoder: Decoder[Identifier] = deriveDecoder[Identifier]

  private object StringEncodedIdentifier {
    private val idPattern = raw"(\w*)@(.*)".r

    def unapply(str: String): Option[Identifier] = str match {
      case idPattern(hash, name) => Some(Identifier(hash, name))
      case _ => None
    }
  }

  private implicit def taggedEncoder[A: Encoder, T]: Encoder[A @@ T] =
    scalaz.Tag.subst(Encoder[A])
  private implicit def taggedDecoder[A: Decoder, T]: Decoder[A @@ T] =
    scalaz.Tag.subst(Decoder[A])

  private implicit val nameEncoder: Encoder[Ref.Name] =
    Encoder[String].contramap(identity)
  private implicit val partyEncoder: Encoder[Ref.Party] =
    Encoder[String].contramap(identity)

  private implicit val lfDateEncoder: Encoder[Time.Date] =
    Encoder[String].contramap(_.toString)
  private implicit val lfTimestampEncoder: Encoder[Time.Timestamp] =
    Encoder[String].contramap(_.toString)

  implicit val multiTableStateEncoder: Encoder[MultiTableState] = deriveEncoder[MultiTableState]
  implicit val multiTableStateDecoder: Decoder[MultiTableState] = deriveDecoder[MultiTableState]
}
