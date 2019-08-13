// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.http.domain
import com.digitalasset.http.json.TaggedJsonFormat._
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import scalaz.{-\/, \/-}
import spray.json._

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

  object LfValueCodec
      extends ApiCodecCompressed[AbsoluteContractId](
        encodeDecimalAsString = true,
        encodeInt64AsString = true) {
    protected override def apiContractIdToJsValue(obj: AbsoluteContractId) = JsString(obj.coid)
    protected override def jsValueToApiContractId(json: JsValue) = json match {
      case JsString(s) =>
        Ref.ContractIdString fromString s fold (deserializationError(_), AbsoluteContractId)
      case _ => deserializationError("ContractId must be a string")
    }
  }

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

  implicit val GetActiveContractsResponseFormat
    : RootJsonFormat[domain.GetActiveContractsResponse[JsValue]] =
    jsonFormat3(domain.GetActiveContractsResponse[JsValue])

  implicit val CommandMetaFormat: RootJsonFormat[domain.CommandMeta] = jsonFormat4(
    domain.CommandMeta)

  implicit val CreateCommandFormat: RootJsonFormat[domain.CreateCommand[JsObject]] = jsonFormat3(
    domain.CreateCommand[JsObject])

  implicit val ExerciseCommandFormat: RootJsonFormat[domain.ExerciseCommand[JsObject]] =
    jsonFormat5(domain.ExerciseCommand[JsObject])
}
