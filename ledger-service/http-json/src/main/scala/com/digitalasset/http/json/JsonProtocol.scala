// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import java.time.Instant

import spray.json._
import com.digitalasset.http.domain
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.http.json.TaggedJsonFormat._
import com.digitalasset.ledger.api.v1.value.{Record, Value}
import scalaz.{-\/, \/-}

object JsonProtocol extends DefaultJsonProtocol {

  implicit val LedgerIdFormat: JsonFormat[lar.LedgerId] = taggedJsonFormat[String, lar.LedgerIdTag]

  implicit val ApplicationIdFormat: JsonFormat[lar.ApplicationId] =
    taggedJsonFormat[String, lar.ApplicationIdTag]

  implicit val WorkflowIdFormat: JsonFormat[lar.WorkflowId] =
    taggedJsonFormat[String, lar.WorkflowIdTag]

  implicit val PartyFormat: JsonFormat[lar.Party] =
    taggedJsonFormat[String, lar.PartyTag]

  implicit val CommandIdFormat: JsonFormat[lar.CommandId] =
    taggedJsonFormat[String, lar.CommandIdTag]

  implicit val ChoiceFormat: JsonFormat[lar.Choice] = taggedJsonFormat[String, lar.ChoiceTag]

  implicit val ContractIdFormat: JsonFormat[lar.ContractId] =
    taggedJsonFormat[String, lar.ContractIdTag]

  implicit val JwtPayloadFormat: RootJsonFormat[domain.JwtPayload] = jsonFormat3(domain.JwtPayload)

  implicit val InstantFormat: JsonFormat[java.time.Instant] = new JsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsNumber(obj.toEpochMilli)

    override def read(json: JsValue): Instant = json match {
      case JsNumber(a) => java.time.Instant.ofEpochMilli(a.toLongExact)
      case _ => deserializationError("java.time.Instant must be epoch millis")
    }
  }

  implicit def TemplateIdFormat[A: JsonFormat]: RootJsonFormat[domain.TemplateId[A]] =
    jsonFormat3(domain.TemplateId.apply[A])

  implicit val ContractLookupRequestFormat
    : RootJsonReader[domain.ContractLookupRequest[JsValue]] = {
    case JsObject(fields) =>
      val ledgerId = fields get "ledgerId" map (_.convertTo[String])
      val id = (fields get "templateId", fields get "key", fields get "contractId") match {
        case (Some(templateId), Some(key), None) =>
          -\/((templateId.convertTo[domain.TemplateId.OptionalPkg], key))
        case (otid, None, Some(contractId)) =>
          \/-((otid map (_.convertTo[domain.TemplateId.OptionalPkg]), contractId.convertTo[String]))
        case (None, Some(_), None) =>
          deserializationError(
            "ContractLookupRequest requires key to be accompanied by a templateId")
        case (_, None, None) | (_, Some(_), Some(_)) =>
          deserializationError("ContractLookupRequest requires exactly one of a key or contractId")
      }
      domain.ContractLookupRequest(ledgerId, id)
    case _ => deserializationError("ContractLookupRequest must be an object")
  }

  implicit val ActiveContractFormat: RootJsonFormat[domain.ActiveContract[JsValue]] =
    jsonFormat6(domain.ActiveContract.apply[JsValue])

  implicit val GetActiveContractsRequestFormat: RootJsonFormat[domain.GetActiveContractsRequest] =
    jsonFormat1(domain.GetActiveContractsRequest)

  // sigh @ induction
  implicit def SeqJsonWriter[A: JsonWriter]: JsonWriter[Seq[A]] =
    as => JsArray(as.iterator.map(_.toJson).toVector)

  implicit val GetActiveContractsResponseFormat
    : JsonWriter[domain.GetActiveContractsResponse[JsValue]] =
    gacr => JsString(gacr.toString) // TODO actual format

  // TODO (Leo): get rid of this when we have lf value json formats
  implicit val RecordFormat: RootJsonFormat[lav1.value.Record] =
    new RootJsonFormat[lav1.value.Record] {
      override def write(obj: Record): JsValue = sys.error("not implemented")
      override def read(json: JsValue): Record = sys.error("not implemented")
    }

  // TODO (Leo): get rid of this when we have lf value json formats
  implicit val ValueFormat: RootJsonFormat[lav1.value.Value] =
    new RootJsonFormat[lav1.value.Value] {
      override def write(obj: Value): JsValue = sys.error("not implemented")
      override def read(json: JsValue): Value = sys.error("not implemented")
    }

  implicit val CommandMetaFormat: RootJsonFormat[domain.CommandMeta] = jsonFormat4(
    domain.CommandMeta)

  implicit val CreateCommandFormat: RootJsonFormat[domain.CreateCommand] = jsonFormat3(
    domain.CreateCommand)

  implicit val ExerciseCommandFormat: RootJsonFormat[domain.ExerciseCommand] = jsonFormat5(
    domain.ExerciseCommand)
}
