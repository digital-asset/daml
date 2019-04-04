// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.json

import com.digitalasset.daml.lf.data.UTF8
import com.digitalasset.extractor.ledger.types.{Identifier, LedgerValue}
import com.digitalasset.extractor.ledger.types.LedgerValue._
import com.digitalasset.extractor.writers.postgresql.DataFormatState.MultiTableState
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._

object JsonConverters {
  def toJsonString[A: Encoder](a: A): String = {
    a.asJson.noSpaces
  }

  implicit val recordEncoder: Encoder[Record] = record => {
    record.fields
      .foldLeft(JsonObject.empty) {
        case (acc, field) =>
          acc.+:(field.label -> field.value.asJson)
      }
      .asJson
  }

  private val emptyRecord = Record(List.empty).asJson

  // TODO it might be much more performant if exploded into separate vals
  implicit def valueEncoder[T <: LedgerValue]: Encoder[T] = {
    case r @ LedgerValue.Record(_) => r.asJson
    case v @ LedgerValue.Variant(_, _) => v.asJson
    case LedgerValue.ValueList(value) => value.asJson
    case LedgerValue.Optional(value) => value.asJson
    case LedgerValue.ValueMap(value) => value.asJson
    case LedgerValue.Bool(value) => value.asJson
    case LedgerValue.ContractId(value) => value.asJson
    case LedgerValue.Int64(value) => value.asJson
    case LedgerValue.Decimal(value) => value.asJson
    case LedgerValue.Text(value) => value.asJson
    case LedgerValue.Timestamp(value) => value.asJson
    case LedgerValue.Party(value) => value.asJson
    case LedgerValue.Date(value) => value.asJson
    case LedgerValue.Unit => emptyRecord
    case LedgerValue.Empty => emptyRecord
  }

  implicit val listEncoder: Encoder[List[_ <: LedgerValue]] = list => {
    list.map(_.asJson).asJson
  }

  implicit val variantEncoder: Encoder[Variant] = variant => {
    JsonObject(
      variant.constructor -> variant.value.asJson
    ).asJson
  }

  implicit val scalaOptionEncoder: Encoder[Option[LedgerValue]] = _ match {
    case None =>
      JsonObject("None" -> emptyRecord).asJson
    case Some(value) =>
      JsonObject("Some" -> value.asJson).asJson
  }

  implicit val mapEncoder: Encoder[Map[String, LedgerValue]] = m => {
    JsonObject(
      "Map" ->
        m.toList
          .sortBy(_._1)(UTF8.ordering)
          .map { case (k, v) => JsonObject("key" -> k.asJson, "value" -> v.asJson) }
          .asJson).asJson
  }

  implicit val idKeyEncoder: KeyEncoder[Identifier] = id => s"${id.packageId}@${id.name}"
  implicit val idKeyDecoder: KeyDecoder[Identifier] = {
    case StringEncodedIdentifier(id) => Some(id)
    case _ => None
  }

  implicit val idEncoder: Encoder[Identifier] = deriveEncoder[Identifier]
  implicit val idDecoder: Decoder[Identifier] = deriveDecoder[Identifier]

  object StringEncodedIdentifier {
    private val idPattern = raw"(\w*)@(.*)".r

    def unapply(str: String): Option[Identifier] = str match {
      case idPattern(hash, name) => Some(Identifier(hash, name))
      case _ => None
    }
  }

  implicit def taggedEncoder[A: Encoder, T]: Encoder[scalaz.@@[A, T]] =
    Encoder[A].contramap(scalaz.Tag.unwrap)
  implicit def taggedDecoder[A: Decoder, T]: Decoder[scalaz.@@[A, T]] =
    Decoder[A].map(scalaz.Tag.apply)

  implicit val multiTableStateEncoder: Encoder[MultiTableState] = deriveEncoder[MultiTableState]
  implicit val multiTableStateDecoder: Decoder[MultiTableState] = deriveDecoder[MultiTableState]
}
