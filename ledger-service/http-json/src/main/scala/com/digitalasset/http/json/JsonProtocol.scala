// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import spray.json._
import com.digitalasset.http.domain
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.http.json.TaggedJsonFormat._
import com.digitalasset.ledger.api.v1.value.Record
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

  implicit val JwtPayloadFormat: RootJsonFormat[domain.JwtPayload] = jsonFormat3(domain.JwtPayload)

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

  // TODO (Leo): get rid of this
  implicit val RecordFormat: RootJsonFormat[lav1.value.Record] =
    new RootJsonFormat[lav1.value.Record] {
      override def write(obj: Record): JsValue = sys.error("not implemented")

      override def read(json: JsValue): Record = sys.error("not implemented")
    }

  implicit val CreateCommandFormat: RootJsonFormat[domain.CreateCommand] = jsonFormat4(
    domain.CreateCommand)
}
