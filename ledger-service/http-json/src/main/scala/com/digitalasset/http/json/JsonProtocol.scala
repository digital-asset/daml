// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import java.time.Instant

import akka.http.scaladsl.model.StatusCode
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

  implicit val PartyFormat: JsonFormat[domain.Party] =
    taggedJsonFormat[String, domain.PartyTag]

  implicit val CommandIdFormat: JsonFormat[lar.CommandId] =
    taggedJsonFormat[String, lar.CommandIdTag]

  implicit val ChoiceFormat: JsonFormat[lar.Choice] = taggedJsonFormat[String, lar.ChoiceTag]

  implicit val ContractIdFormat: JsonFormat[domain.ContractId] =
    taggedJsonFormat[String, domain.ContractIdTag]

  implicit val PartyDetails: JsonFormat[domain.PartyDetails] =
    jsonFormat3(domain.PartyDetails.apply)

  object LfValueCodec
      extends ApiCodecCompressed[AbsoluteContractId](
        encodeDecimalAsString = true,
        encodeInt64AsString = true)
      with CodecAbsoluteContractIds

  // DB *must not* use stringly ints or decimals; see ValuePredicate Range comments
  object LfValueDatabaseCodec
      extends ApiCodecCompressed[AbsoluteContractId](
        encodeDecimalAsString = false,
        encodeInt64AsString = false)
      with CodecAbsoluteContractIds {
    private[http] def asLfValueCodec(jv: JsValue): JsValue = jv match {
      case JsObject(fields) => JsObject(fields transform ((_, v) => asLfValueCodec(v)))
      case JsArray(elements) => JsArray(elements map asLfValueCodec)
      case JsNull | _: JsString | _: JsBoolean => jv
      case JsNumber(value) =>
        // diverges slightly from ApiCodecCompressed: integers of numeric type
        // will not have a ".0" included in their string representation.  We can't
        // tell the difference here between an int64 and a numeric
        JsString(value.bigDecimal.stripTrailingZeros.toPlainString)
    }
  }

  sealed trait CodecAbsoluteContractIds extends ApiCodecCompressed[AbsoluteContractId] {
    protected override final def apiContractIdToJsValue(obj: AbsoluteContractId) =
      JsString(obj.coid)
    protected override final def jsValueToApiContractId(json: JsValue) = json match {
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

  implicit val TemplateIdRequiredPkgFormat: RootJsonFormat[domain.TemplateId.RequiredPkg] =
    new RootJsonFormat[domain.TemplateId.RequiredPkg] {
      override def write(a: domain.TemplateId.RequiredPkg): JsValue =
        JsString(s"${a.packageId: String}:${a.moduleName: String}:${a.entityName: String}")

      override def read(json: JsValue): domain.TemplateId.RequiredPkg = json match {
        case JsString(str) =>
          str.split(':') match {
            case Array(p, m, e) => domain.TemplateId(p, m, e)
            case _ => error(json)
          }
        case _ => error(json)
      }

      private def error(json: JsValue): Nothing =
        deserializationError(s"Expected JsString(<packageId>:<module>:<entity>), got: $json")
    }

  implicit val TemplateIdOptionalPkgFormat: RootJsonFormat[domain.TemplateId.OptionalPkg] =
    new RootJsonFormat[domain.TemplateId.OptionalPkg] {
      override def write(a: domain.TemplateId.OptionalPkg): JsValue = a.packageId match {
        case Some(p) => JsString(s"${p: String}:${a.moduleName: String}:${a.entityName: String}")
        case None => JsString(s"${a.moduleName: String}:${a.entityName: String}")
      }

      override def read(json: JsValue): domain.TemplateId.OptionalPkg = json match {
        case JsString(str) =>
          str.split(':') match {
            case Array(p, m, e) => domain.TemplateId(Some(p), m, e)
            case Array(m, e) => domain.TemplateId(None, m, e)
            case _ => error(json)
          }
        case _ => error(json)
      }

      private def error(json: JsValue): Nothing =
        deserializationError(s"Expected JsString([<packageId>:]<module>:<entity>), got: $json")
    }

  private[this] def decodeContractRef(
      fields: Map[String, JsValue],
      what: String): domain.InputContractRef[JsValue] =
    (fields get "templateId", fields get "key", fields get "contractId") match {
      case (Some(templateId), Some(key), None) =>
        -\/((templateId.convertTo[domain.TemplateId.OptionalPkg], key))
      case (otid, None, Some(contractId)) =>
        val a = otid map (_.convertTo[domain.TemplateId.OptionalPkg])
        val b = contractId.convertTo[domain.ContractId]
        \/-((a, b))
      case (None, Some(_), None) =>
        deserializationError(s"$what requires key to be accompanied by a templateId")
      case (_, None, None) | (_, Some(_), Some(_)) =>
        deserializationError(s"$what requires exactly one of a key or contractId")
    }

  // implicit val ContractLookupRequestFormat
  //   : RootJsonReader[domain.ContractLookupRequest[JsValue]] = {
  //   case JsObject(fields) =>
  //     val id = decodeContractRef(fields, "ContractLookupRequest")
  //     domain.ContractLookupRequest(id)
  //   case _ => deserializationError("ContractLookupRequest must be an object")
  // }

  implicit val EnrichedContractKeyFormat: RootJsonFormat[domain.EnrichedContractKey[JsValue]] =
    jsonFormat2(domain.EnrichedContractKey.apply[JsValue])

  implicit val EnrichedContractIdFormat: RootJsonFormat[domain.EnrichedContractId] =
    jsonFormat2(domain.EnrichedContractId)

  implicit val ContractLocatorFormat: RootJsonFormat[domain.ContractLocator[JsValue]] =
    new RootJsonFormat[domain.ContractLocator[JsValue]] {
      override def write(obj: domain.ContractLocator[JsValue]): JsValue = obj match {
        case a: domain.EnrichedContractKey[JsValue] => EnrichedContractKeyFormat.write(a)
        case b: domain.EnrichedContractId => EnrichedContractIdFormat.write(b)
      }

      override def read(json: JsValue): domain.ContractLocator[JsValue] = json match {
        case JsObject(fields) =>
          domain.ContractLocator.structure.from(decodeContractRef(fields, "ContractLocator"))
        case _ =>
          deserializationError(s"Cannot read ContractLocator from json: $json")
      }
    }

  implicit val ContractFormat: RootJsonFormat[domain.Contract[JsValue]] =
    new RootJsonFormat[domain.Contract[JsValue]] {
      private val archivedKey = "archived"
      private val activeKey = "created"

      override def read(json: JsValue): domain.Contract[JsValue] = json match {
        case JsObject(fields) =>
          fields.toList match {
            case List((`archivedKey`, archived)) =>
              domain.Contract(-\/(ArchivedContractFormat.read(archived)))
            case List((`activeKey`, active)) =>
              domain.Contract(\/-(ActiveContractFormat.read(active)))
            case _ =>
              deserializationError(
                s"Contract must be either {$archivedKey: obj} or {$activeKey: obj}, got: $fields")
          }
        case _ => deserializationError("Contract must be an object")
      }

      override def write(obj: domain.Contract[JsValue]): JsValue = obj.value match {
        case -\/(archived) => JsObject(archivedKey -> ArchivedContractFormat.write(archived))
        case \/-(active) => JsObject(activeKey -> ActiveContractFormat.write(active))
      }
    }

  implicit val ActiveContractFormat: RootJsonFormat[domain.ActiveContract[JsValue]] =
    jsonFormat7(domain.ActiveContract.apply[JsValue])

  implicit val ArchivedContractFormat: RootJsonFormat[domain.ArchivedContract] =
    jsonFormat2(domain.ArchivedContract.apply)

  private val templatesKey = "%templates"

  implicit val GetActiveContractsRequestFormat: RootJsonReader[domain.GetActiveContractsRequest] = {
    case JsObject(fields) =>
      val templates = (fields get templatesKey)
        .map(_.convertTo[Set[domain.TemplateId.OptionalPkg]])
        .filter(_.nonEmpty)
        .getOrElse(
          deserializationError("/contracts/search requires at least one item in '%templates'"))
      domain.GetActiveContractsRequest(templateIds = templates, query = fields - templatesKey)
    case _ => deserializationError("/contracts/search must receive an object")
  }

  implicit val CommandMetaFormat: RootJsonFormat[domain.CommandMeta] = jsonFormat3(
    domain.CommandMeta)

  implicit val CreateCommandFormat: RootJsonFormat[domain.CreateCommand[JsObject]] = jsonFormat3(
    domain.CreateCommand[JsObject])

  implicit val ExerciseCommandFormat: RootJsonFormat[domain.ExerciseCommand[JsValue]] =
    jsonFormat5(domain.ExerciseCommand[JsValue])

  implicit val ExerciseResponseFormat: RootJsonFormat[domain.ExerciseResponse[JsValue]] =
    jsonFormat2(domain.ExerciseResponse[JsValue])

  implicit val StatusCodeFormat: RootJsonFormat[StatusCode] =
    new RootJsonFormat[StatusCode] {
      override def read(json: JsValue): StatusCode = json match {
        case JsNumber(x) => StatusCode.int2StatusCode(x.toIntExact)
        case _ => deserializationError(s"Expected JsNumber, got: $json")
      }

      override def write(obj: StatusCode): JsValue = JsNumber(obj.intValue)
    }

  implicit val OkResponseFormat: RootJsonFormat[domain.OkResponse[JsValue, JsValue]] = jsonFormat3(
    domain.OkResponse[JsValue, JsValue])

  implicit val ErrorResponseFormat: RootJsonFormat[domain.ErrorResponse[JsValue]] = jsonFormat2(
    domain.ErrorResponse[JsValue])

  implicit val ServiceWarningFormat: RootJsonFormat[domain.ServiceWarning] =
    new RootJsonFormat[domain.ServiceWarning] {
      override def read(json: JsValue): domain.ServiceWarning = json match {
        case JsObject(fields) if fields.contains("unknownTemplateIds") =>
          UnknownTemplateIdsFormat.read(json)
        case _ =>
          deserializationError(s"Expected JsObject(unknownTemplateIds -> JsArray(...)), got: $json")
      }

      override def write(obj: domain.ServiceWarning): JsValue = obj match {
        case x: domain.UnknownTemplateIds => UnknownTemplateIdsFormat.write(x)
      }
    }

  implicit val UnknownTemplateIdsFormat: RootJsonFormat[domain.UnknownTemplateIds] = jsonFormat1(
    domain.UnknownTemplateIds)
}
