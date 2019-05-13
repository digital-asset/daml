// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.json

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, SortedLookupList}
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.extractor.ledger.types.{Identifier, LedgerValue}
import com.digitalasset.extractor.ledger.types.LedgerValue._
import com.digitalasset.extractor.writers.postgresql.DataFormatState.MultiTableState
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import scalaz.@@

object JsonConverters {
  def toJsonString[A: Encoder](a: A): String = {
    a.asJson.noSpaces
  }

  implicit val recordEncoder: Encoder[Record] = record =>
    if (record.fields.toSeq.forall(_._1.isDefined))
      JsonObject(
        record.fields.toSeq
          .collect {
            case (Some(label), value) =>
              label -> value.asJson
          }: _*
      ).asJson
    else record.fields.toSeq.map(_.asJson).asJson

  private val emptyRecord = Record(None, ImmArray.empty).asJson

  // TODO it might be much more performant if exploded into separate vals
  implicit def valueEncoder[T <: LedgerValue]: Encoder[T] = {
    case r @ V.ValueRecord(_, _) => r.asJson
    case v @ V.ValueVariant(_, _, _) => v.asJson
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
  }

  implicit def frontStackEncoder[A: Encoder]: Encoder[FrontStack[A]] =
    _.toImmArray.map(_.asJson).toSeq.asJson

  implicit val variantEncoder: Encoder[Variant] = {
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
        JsonObject.fromIterable(m.mapValue(_.asJson).toImmArray.toSeq).asJson).asJson

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

  implicit def taggedEncoder[A: Encoder, T]: Encoder[A @@ T] =
    scalaz.Tag.subst(Encoder[A])
  implicit def taggedDecoder[A: Decoder, T]: Decoder[A @@ T] =
    scalaz.Tag.subst(Decoder[A])

  implicit val nameEncoder: Encoder[Ref.Name] =
    Encoder[String].contramap(identity)

  implicit val multiTableStateEncoder: Encoder[MultiTableState] = deriveEncoder[MultiTableState]
  implicit val multiTableStateDecoder: Decoder[MultiTableState] = deriveDecoder[MultiTableState]
}
