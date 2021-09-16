// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.json

import com.daml.lf.data.{ImmArray, SortedLookupList}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.extractor.ledger.types.{Identifier, LedgerValue}
import com.daml.extractor.writers.postgresql.DataFormatState.MultiTableState
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._

object JsonConverters {

  import ApiCodecCompressed.JsonImplicits.ContractIdFormat
  private[this] object LfValueSprayEnc
      extends ApiCodecCompressed(
        encodeDecimalAsString = true,
        encodeInt64AsString = false,
      )

  private[this] def sprayToCirce(s: spray.json.JsValue): Json = {
    import spray.{json => sj}
    s match {
      case sj.JsString(v) => Json fromString v
      case sj.JsNumber(v) => Json fromBigDecimal v
      case sj.JsTrue => Json fromBoolean true
      case sj.JsFalse => Json fromBoolean false
      case sj.JsObject(v) => Json fromFields (v transform ((_, e) => sprayToCirce(e)))
      case sj.JsArray(v) => Json fromValues (v map sprayToCirce)
      case sj.JsNull => Json.Null
    }
  }

  def toJsonString[A: Encoder](a: A): String =
    a.asJson.noSpaces

  implicit val recordEncoder: Encoder[V.ValueRecord] = valueEncoder

  implicit def valueEncoder[T <: LedgerValue]: Encoder[T] =
    t => sprayToCirce(LfValueSprayEnc.apiValueToJsValue(t))

  implicit val variantEncoder: Encoder[V.ValueVariant] = valueEncoder

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
