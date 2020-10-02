// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.json

import com.daml.lf.data.{ImmArray, SortedLookupList}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.extractor.ledger.types.{Identifier, LedgerValue}
import com.daml.extractor.ledger.types.LedgerValue._
import com.daml.extractor.writers.postgresql.DataFormatState.MultiTableState
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import scalaz.std.string._

object JsonConverters {
  import ApiCodecCompressed.JsonImplicits.StringJsonFormat
  private[this] object LfValueSprayEnc
      extends ApiCodecCompressed[String](
        encodeDecimalAsString = true,
        encodeInt64AsString = false
      )

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

  def toJsonString[A: Encoder](a: A): String =
    a.asJson.noSpaces

  implicit val recordEncoder: Encoder[OfCid[V.ValueRecord]] = valueEncoder

  implicit def valueEncoder[T <: LedgerValue]: Encoder[T] =
    t => sprayToCirce(LfValueSprayEnc.apiValueToJsValue(t))

  implicit val variantEncoder: Encoder[OfCid[V.ValueVariant]] = valueEncoder

  implicit val textMapEncoder: Encoder[SortedLookupList[LedgerValue]] =
    valueEncoder.contramap(V.ValueTextMap(_))

  implicit val genMapEncoder: Encoder[ImmArray[(LedgerValue, LedgerValue)]] =
    valueEncoder.contramap(V.ValueGenMap(_))

  implicit val idKeyEncoder: KeyEncoder[Identifier] = id => s"${id.packageId}@${id.name}"
  implicit val idKeyDecoder: KeyDecoder[Identifier] = StringEncodedIdentifier.unapply

  private object StringEncodedIdentifier {
    private val idPattern = raw"(\w*)@(.*)".r

    def unapply(str: String): Option[Identifier] = str match {
      case idPattern(hash, name) => Some(Identifier(hash, name))
      case _ => None
    }
  }

  implicit val multiTableStateEncoder: Encoder[MultiTableState] = deriveEncoder[MultiTableState]
  implicit val multiTableStateDecoder: Decoder[MultiTableState] = deriveDecoder[MultiTableState]
}
