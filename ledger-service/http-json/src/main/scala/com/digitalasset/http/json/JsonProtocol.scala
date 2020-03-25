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
import scalaz.syntax.std.option._
import scalaz.{-\/, NonEmptyList, \/-}
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

  implicit def NonEmptyListReader[A: JsonReader]: JsonReader[NonEmptyList[A]] = {
    case JsArray(hd +: tl) =>
      NonEmptyList(hd.convertTo[A], tl map (_.convertTo[A]): _*)
    case _ => deserializationError("must be a list with at least 1 element")
  }

  implicit def NonEmptyListWriter[A: JsonWriter]: JsonWriter[NonEmptyList[A]] =
    nela => JsArray(nela.map(_.toJson).list.toVector)

  /** This intuitively pointless extra type is here to give it specificity
    * so this instance will beat [[CollectionFormats#listFormat]].
    * You would normally achieve the conflict resolution by putting this
    * instance in a parent of [[CollectionFormats]], but that kind of
    * extension isn't possible here.
    */
  final class JsonReaderList[A: JsonReader] extends JsonReader[List[A]] {
    override def read(json: JsValue) = json match {
      case JsArray(elements) => elements.iterator.map(_.convertTo[A]).toList
      case _ => deserializationError(s"must be a list, but got $json")
    }
  }

  implicit def `List reader only`[A: JsonReader]: JsonReaderList[A] = new JsonReaderList

  implicit val PartyDetails: JsonFormat[domain.PartyDetails] =
    jsonFormat3(domain.PartyDetails.apply)

  implicit val AllocatePartyRequest: JsonFormat[domain.AllocatePartyRequest] =
    jsonFormat2(domain.AllocatePartyRequest)

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
        deserializationError(s"$what requires either key or contractId field")
    }

  implicit val EnrichedContractKeyFormat: RootJsonFormat[domain.EnrichedContractKey[JsValue]] =
    jsonFormat2(domain.EnrichedContractKey.apply[JsValue])

  implicit val EnrichedContractIdFormat: RootJsonFormat[domain.EnrichedContractId] =
    jsonFormat2(domain.EnrichedContractId)

  private[this] val offsetHintKey = "offsetHint"

  implicit val InitialContractKeyStreamRequest
    : RootJsonReader[domain.ContractKeyStreamRequest[None.type, JsValue]] = { jsv =>
    val ekey = jsv.convertTo[domain.EnrichedContractKey[JsValue]]
    jsv match {
      case JsObject(fields) if fields contains offsetHintKey =>
        deserializationError(
          s"$offsetHintKey is not allowed for WebSocket streams starting at the beginning")
      case _ =>
    }
    domain.ContractKeyStreamRequest(None, ekey)
  }

  implicit val ResumingContractKeyStreamRequest: RootJsonReader[
    domain.ContractKeyStreamRequest[Option[Option[domain.ContractId]], JsValue]] = { jsv =>
    val off = jsv match {
      case JsObject(fields) => fields get offsetHintKey map (_.convertTo[Option[String]])
      case _ => None
    }
    val ekey = jsv.convertTo[domain.EnrichedContractKey[JsValue]]
    type OO[+A] = Option[Option[A]]
    domain.ContractKeyStreamRequest(domain.Offset.tag.subst[OO, String](off), ekey)
  }

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

  /** Derived from autogenerated with 3 extra features:
    *  1. template IDs are required
    *  2. query key may be absent
    *  3. special error if you appear to have queried outside 'query'
    */
  implicit val GetActiveContractsRequestFormat: RootJsonReader[domain.GetActiveContractsRequest] = {
    case class GACR(
        templateIds: Set[domain.TemplateId.OptionalPkg],
        query: Option[Map[String, JsValue]])
    val validKeys = Set("templateIds", "query")
    implicit val primitive: JsonReader[GACR] = jsonFormat2(GACR.apply)
    jsv =>
      {
        val GACR(tids, q) = jsv.convertTo[GACR]
        val extras = jsv.asJsObject.fields.keySet diff validKeys
        if (tids.isEmpty)
          deserializationError("search requires at least one item in 'templateIds'")
        else if (extras.nonEmpty)
          deserializationError(
            s"unsupported query fields ${extras}; likely should be within 'query' subobject")
        domain.GetActiveContractsRequest(tids, q getOrElse Map.empty)
      }
  }

  implicit val SearchForeverRequestFormat: RootJsonReader[domain.SearchForeverRequest] = {
    case multi @ JsArray(_) =>
      domain.SearchForeverRequest(multi.convertTo[NonEmptyList[domain.GetActiveContractsRequest]])
    case single =>
      domain.SearchForeverRequest(NonEmptyList(single.convertTo[domain.GetActiveContractsRequest]))
  }

  implicit val CommandMetaFormat: RootJsonFormat[domain.CommandMeta] = jsonFormat3(
    domain.CommandMeta)

  implicit val CreateCommandFormat: RootJsonFormat[domain.CreateCommand[JsValue]] = jsonFormat3(
    domain.CreateCommand[JsValue])

  implicit val ExerciseCommandFormat
    : RootJsonFormat[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]] =
    new RootJsonFormat[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]] {
      override def write(
          obj: domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]): JsValue = {

        val reference: JsObject =
          ContractLocatorFormat.write(obj.reference).asJsObject("reference must be an object")

        val fields: Vector[(String, JsValue)] =
          reference.fields.toVector ++
            Vector("choice" -> obj.choice.toJson, "argument" -> obj.argument.toJson) ++
            obj.meta.cata(x => Vector("meta" -> x.toJson), Vector.empty)

        JsObject(fields: _*)
      }

      override def read(
          json: JsValue): domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]] = {
        val reference = ContractLocatorFormat.read(json)
        val choice = fromField[domain.Choice](json, "choice")
        val argument = fromField[JsValue](json, "argument")
        val meta = fromField[Option[domain.CommandMeta]](json, "meta")

        domain.ExerciseCommand(
          reference = reference,
          choice = choice,
          argument = argument,
          meta = meta)
      }
    }

  implicit val CreateAndExerciseCommandFormat
    : RootJsonFormat[domain.CreateAndExerciseCommand[JsValue, JsValue]] =
    jsonFormat5(domain.CreateAndExerciseCommand[JsValue, JsValue])

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

  implicit def OkResponseFormat[A: JsonFormat, B: JsonFormat]
    : RootJsonFormat[domain.OkResponse[A, B]] = jsonFormat3(domain.OkResponse[A, B])

  implicit val ErrorResponseFormat: RootJsonFormat[domain.ErrorResponse[JsValue]] = jsonFormat2(
    domain.ErrorResponse[JsValue])

  implicit val ServiceWarningFormat: RootJsonFormat[domain.ServiceWarning] =
    new RootJsonFormat[domain.ServiceWarning] {
      override def read(json: JsValue): domain.ServiceWarning = json match {
        case JsObject(fields) if fields.contains("unknownTemplateIds") =>
          UnknownTemplateIdsFormat.read(json)
        case JsObject(fields) if fields.contains("unknownParties") =>
          UnknownPartiesFormat.read(json)
        case _ =>
          deserializationError(
            s"Expected JsObject(unknownTemplateIds | unknownParties -> JsArray(...)), got: $json")
      }

      override def write(obj: domain.ServiceWarning): JsValue = obj match {
        case x: domain.UnknownTemplateIds => UnknownTemplateIdsFormat.write(x)
        case x: domain.UnknownParties => UnknownPartiesFormat.write(x)
      }
    }

  implicit val UnknownTemplateIdsFormat: RootJsonFormat[domain.UnknownTemplateIds] = jsonFormat1(
    domain.UnknownTemplateIds)

  implicit val UnknownPartiesFormat: RootJsonFormat[domain.UnknownParties] = jsonFormat1(
    domain.UnknownParties)
}
