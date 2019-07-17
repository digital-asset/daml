// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import java.time.Instant

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.http.domain
import com.digitalasset.http.json.TaggedJsonFormat._
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.value.Value
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.{-\/, Show, \/, \/-}
import spray.json._

object JsonProtocol extends DefaultJsonProtocol {

  final case class Error(message: String)

  object Error {
    implicit val show = new Show[Error] {
      override def shows(f: Error): String = s"JsonProtocol.Error: ${f.message}"
    }
  }

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

  def valueWriter(apiToLf: ApiValueToLfValueConverter.ApiValueToLfValue) =
    new RootJsonWriter[Value] {
      override def write(a: lav1.value.Value): JsValue =
        apiValueToJsValue(apiToLf)(a).fold(e => serializationError(e.shows), identity)
    }

  def apiValueToJsValue(apiToLf: ApiValueToLfValueConverter.ApiValueToLfValue)(
      a: lav1.value.Value): Error \/ JsValue =
    apiToLf(a)
      .map { b: lf.value.Value[lf.value.Value.AbsoluteContractId] =>
        ApiCodecCompressed.apiValueToJsValue(lfValueOfString(b))
      }
      .leftMap(e => Error(e.shows))

  def valueReader(apiToLf: ApiValueToLfValueConverter.ApiValueToLfValue) =
    new RootJsonReader[lav1.value.Value] {
      override def read(json: JsValue): lav1.value.Value =
        jsValueToApiValue(json).fold(e => deserializationError(e.shows), identity)
    }

  def jsValueToApiValue(jsValue: JsValue): Error \/ lav1.value.Value = {
    sys.error("not implemented")
  }

  private def lfValueOfString(
      lfValue: lf.value.Value[lf.value.Value.AbsoluteContractId]): lf.value.Value[String] =
    lfValue.mapContractId(x => x.coid)

  implicit val CommandMetaFormat: RootJsonFormat[domain.CommandMeta] = jsonFormat4(
    domain.CommandMeta)

  implicit val CreateCommandFormat: RootJsonFormat[domain.CreateCommand[JsValue]] = jsonFormat3(
    domain.CreateCommand[JsValue])

  implicit val ExerciseCommandFormat: RootJsonFormat[domain.ExerciseCommand[JsValue]] = jsonFormat5(
    domain.ExerciseCommand[JsValue])
}
