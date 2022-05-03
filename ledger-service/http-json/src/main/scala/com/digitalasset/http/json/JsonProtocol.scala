// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import akka.http.scaladsl.model.StatusCode
import com.daml.http.domain
import com.daml.http.domain.TemplateId
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.json.ApiCodecCompressed
import scalaz.syntax.std.option._
import scalaz.{-\/, NonEmptyList, OneAnd, \/-}
import spray.json._
import spray.json.derived.Discriminator
import scalaz.syntax.tag._

object JsonProtocol extends JsonProtocolLow {

  implicit val LedgerIdFormat: JsonFormat[lar.LedgerId] = taggedJsonFormat[String, lar.LedgerIdTag]

  implicit val ApplicationIdFormat: JsonFormat[lar.ApplicationId] =
    taggedJsonFormat[String, lar.ApplicationIdTag]

  implicit val PartyFormat: JsonFormat[domain.Party] =
    taggedJsonFormat

  implicit val CommandIdFormat: JsonFormat[lar.CommandId] =
    taggedJsonFormat[String, lar.CommandIdTag]

  implicit val ChoiceFormat: JsonFormat[lar.Choice] = taggedJsonFormat[String, lar.ChoiceTag]

  implicit val DomainContractIdFormat: JsonFormat[domain.ContractId] =
    taggedJsonFormat

  implicit val ContractIdFormat: JsonFormat[ContractId] =
    new JsonFormat[ContractId] {
      override def write(obj: ContractId) =
        JsString(obj.coid)
      override def read(json: JsValue) = json match {
        case JsString(s) =>
          ContractId.fromString(s).fold(deserializationError(_), identity)
        case _ => deserializationError("ContractId must be a string")
      }
    }

  implicit val OffsetFormat: JsonFormat[domain.Offset] =
    taggedJsonFormat

  implicit def NonEmptyListFormat[A: JsonReader: JsonWriter]: JsonFormat[NonEmptyList[A]] =
    jsonFormatFromReaderWriter(NonEmptyListReader, NonEmptyListWriter)

  // Do not design your own open typeclasses like JsonFormat was designed.
  private[this] def jsonFormatFromReaderWriter[A: JsonReader: JsonWriter]: JsonFormat[A] =
    new JsonFormat[A] {
      override def read(json: JsValue) = json.convertTo[A]
      override def write(obj: A) = obj.toJson
    }

  /** This intuitively pointless extra type is here to give it specificity so
    *  this instance will beat CollectionFormats#listFormat. You would normally
    *  achieve the conflict resolution by putting this instance in a parent of
    *  [[https://javadoc.io/static/io.spray/spray-json_2.12/1.3.5/spray/json/CollectionFormats.html CollectionFormats]],
    *  but that kind of extension isn't possible here.
    */
  final class JsonReaderList[A: JsonReader] extends JsonReader[List[A]] {
    override def read(json: JsValue) = json match {
      case JsArray(elements) => elements.iterator.map(_.convertTo[A]).toList
      case _ => deserializationError(s"must be a list, but got $json")
    }
  }

  implicit def `List reader only`[A: JsonReader]: JsonReaderList[A] = new JsonReaderList

  implicit val userDetails: JsonFormat[domain.UserDetails] =
    jsonFormat2(domain.UserDetails.apply)

  import spray.json.derived.semiauto._

  // For whatever reason the annotation detection for the deriveFormat is not working correctly.
  // This fixes it.
  private implicit def annotationFix[T]: shapeless.Annotation[Option[Discriminator], T] =
    shapeless.Annotation.mkAnnotation(None)

  implicit val userRight: JsonFormat[domain.UserRight] = deriveFormat[domain.UserRight]

  implicit val PartyDetails: JsonFormat[domain.PartyDetails] =
    jsonFormat3(domain.PartyDetails.apply)

  implicit val CreateUserRequest: JsonFormat[domain.CreateUserRequest] =
    jsonFormat3(domain.CreateUserRequest)

  implicit val ListUserRightsRequest: JsonFormat[domain.ListUserRightsRequest] =
    jsonFormat1(domain.ListUserRightsRequest)

  implicit val GrantUserRightsRequest: JsonFormat[domain.GrantUserRightsRequest] =
    jsonFormat2(domain.GrantUserRightsRequest)

  implicit val RevokeUserRightsRequest: JsonFormat[domain.RevokeUserRightsRequest] =
    jsonFormat2(domain.RevokeUserRightsRequest)

  implicit val GetUserRequest: JsonFormat[domain.GetUserRequest] =
    jsonFormat1(domain.GetUserRequest)

  implicit val DeleteUserRequest: JsonFormat[domain.DeleteUserRequest] =
    jsonFormat1(domain.DeleteUserRequest)

  implicit val AllocatePartyRequest: JsonFormat[domain.AllocatePartyRequest] =
    jsonFormat2(domain.AllocatePartyRequest)

  object LfValueCodec
      extends ApiCodecCompressed(
        encodeDecimalAsString = true,
        encodeInt64AsString = true,
      )

  // DB *must not* use stringly ints or decimals; see ValuePredicate Range comments
  object LfValueDatabaseCodec
      extends ApiCodecCompressed(
        encodeDecimalAsString = false,
        encodeInt64AsString = false,
      ) {
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
      what: String,
  ): domain.InputContractRef[JsValue] =
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

  private[this] val contractIdAtOffsetKey = "contractIdAtOffset"

  implicit val InitialContractKeyStreamRequest
      : RootJsonReader[domain.ContractKeyStreamRequest[Unit, JsValue]] = { jsv =>
    val ekey = jsv.convertTo[domain.EnrichedContractKey[JsValue]]
    jsv match {
      case JsObject(fields) if fields contains contractIdAtOffsetKey =>
        deserializationError(
          s"$contractIdAtOffsetKey is not allowed for WebSocket streams starting at the beginning"
        )
      case _ =>
    }
    domain.ContractKeyStreamRequest((), ekey)
  }

  implicit val ResumingContractKeyStreamRequest: RootJsonReader[
    domain.ContractKeyStreamRequest[Option[Option[domain.ContractId]], JsValue]
  ] = { jsv =>
    val off = jsv match {
      case JsObject(fields) => fields get contractIdAtOffsetKey map (_.convertTo[Option[String]])
      case _ => None
    }
    val ekey = jsv.convertTo[domain.EnrichedContractKey[JsValue]]
    type OO[+A] = Option[Option[A]]
    domain.ContractKeyStreamRequest(domain.ContractId.subst[OO, String](off), ekey)
  }

  private[http] val ReadersKey = "readers"

  implicit val FetchRequestFormat: RootJsonReader[domain.FetchRequest[JsValue]] =
    new RootJsonFormat[domain.FetchRequest[JsValue]] {
      override def write(obj: domain.FetchRequest[JsValue]): JsValue = {
        val domain.FetchRequest(locator, readAs) = obj
        val lj = locator.toJson
        readAs.cata(rl => JsObject(lj.asJsObject.fields.updated(ReadersKey, rl.toJson)), lj)
      }

      override def read(json: JsValue): domain.FetchRequest[JsValue] = {
        val jo = json.asJsObject("fetch request must be a JSON object").fields
        domain.FetchRequest(
          JsObject(jo - ReadersKey).convertTo[domain.ContractLocator[JsValue]],
          jo.get(ReadersKey).flatMap(_.convertTo[Option[NonEmptyList[domain.Party]]]),
        )
      }
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
              domain.Contract[JsValue](-\/(ArchivedContractFormat.read(archived)))
            case List((`activeKey`, active)) =>
              domain.Contract[JsValue](\/-(ActiveContractFormat.read(active)))
            case _ =>
              deserializationError(
                s"Contract must be either {$archivedKey: obj} or {$activeKey: obj}, got: $fields"
              )
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

  // Like requestJsonReader, but suitable for exactly one extra field, simply
  // parsing it to the supplied extra type if present.
  // Can generalize to >1 field with singleton types and hlists, if you like
  private def requestJsonReaderPlusOne[Extra: JsonReader, Request](validExtraField: String)(
      toRequest: (
          OneAnd[Set, TemplateId.OptionalPkg],
          Map[String, JsValue],
          Option[Extra],
      ) => Request
  ): RootJsonReader[Request] =
    requestJsonReader(Set(validExtraField)) { (tids, query, extra) =>
      toRequest(tids, query, extra get validExtraField map (_.convertTo[Extra]))
    }

  /** Derived from autogenerated with 3 extra features:
    *  1. template IDs are required
    *  2. query key may be absent
    *  3. special error if you appear to have queried outside 'query'
    *
    *  This provides an (almost) consistent behavior when reading the 'templateIds' and
    *  'query' fields. Further extra fields may be added by concrete implementations.
    */
  private[this] def requestJsonReader[Request](validExtraFields: Set[String])(
      toRequest: (
          OneAnd[Set, TemplateId.OptionalPkg],
          Map[String, JsValue],
          Map[String, JsValue],
      ) => Request
  ): RootJsonReader[Request] = {
    final case class BaseRequest(
        templateIds: Set[domain.TemplateId.OptionalPkg],
        query: Option[Map[String, JsValue]],
    )
    val validKeys = Set("templateIds", "query") ++ validExtraFields
    implicit val primitive: JsonReader[BaseRequest] = jsonFormat2(BaseRequest.apply)
    jsv => {
      val BaseRequest(tids, query) = jsv.convertTo[BaseRequest]
      val unsupported = jsv.asJsObject.fields.keySet diff validKeys
      if (unsupported.nonEmpty)
        deserializationError(
          s"unsupported query fields $unsupported; likely should be within 'query' subobject"
        )
      val extraFields = jsv.asJsObject.fields.filter { case (fieldName, _) =>
        validExtraFields(fieldName)
      }
      val nonEmptyTids = tids.headOption.cata(
        h => OneAnd(h, tids - h),
        deserializationError("search requires at least one item in 'templateIds'"),
      )
      toRequest(nonEmptyTids, query.getOrElse(Map.empty), extraFields)
    }
  }

  implicit val GetActiveContractsRequestFormat: RootJsonReader[domain.GetActiveContractsRequest] = {
    requestJsonReaderPlusOne(ReadersKey)(domain.GetActiveContractsRequest)
  }

  implicit val SearchForeverQueryFormat: RootJsonReader[domain.SearchForeverQuery] = {
    val OffsetKey = "offset"
    requestJsonReaderPlusOne(OffsetKey)(domain.SearchForeverQuery)
  }

  implicit val SearchForeverRequestFormat: RootJsonReader[domain.SearchForeverRequest] = {
    case multi @ JsArray(_) =>
      val queriesWithPos = multi.convertTo[NonEmptyList[domain.SearchForeverQuery]].zipWithIndex
      domain.SearchForeverRequest(queriesWithPos)
    case single =>
      domain.SearchForeverRequest(NonEmptyList((single.convertTo[domain.SearchForeverQuery], 0)))
  }

  implicit val CommandMetaFormat: RootJsonFormat[domain.CommandMeta] = jsonFormat3(
    domain.CommandMeta
  )

  implicit val CreateCommandFormat
      : RootJsonFormat[domain.CreateCommand[JsValue, domain.TemplateId.OptionalPkg]] = jsonFormat3(
    domain.CreateCommand[JsValue, domain.TemplateId.OptionalPkg]
  )

  implicit val ExerciseCommandFormat
      : RootJsonFormat[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]] =
    new RootJsonFormat[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]] {
      override def write(
          obj: domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]
      ): JsValue = {

        val reference: JsObject =
          ContractLocatorFormat.write(obj.reference).asJsObject("reference must be an object")

        val fields: Vector[(String, JsValue)] =
          reference.fields.toVector ++
            Vector("choice" -> obj.choice.toJson, "argument" -> obj.argument.toJson) ++
            obj.meta.cata(x => Vector("meta" -> x.toJson), Vector.empty)

        JsObject(fields: _*)
      }

      override def read(
          json: JsValue
      ): domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]] = {
        val reference = ContractLocatorFormat.read(json)
        val choice = fromField[domain.Choice](json, "choice")
        val argument = fromField[JsValue](json, "argument")
        val meta = fromField[Option[domain.CommandMeta]](json, "meta")

        domain.ExerciseCommand(
          reference = reference,
          choice = choice,
          argument = argument,
          meta = meta,
        )
      }
    }

  implicit val CreateAndExerciseCommandFormat: RootJsonFormat[
    domain.CreateAndExerciseCommand[JsValue, JsValue, domain.TemplateId.OptionalPkg]
  ] =
    jsonFormat5(domain.CreateAndExerciseCommand[JsValue, JsValue, domain.TemplateId.OptionalPkg])

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

  implicit val ServiceWarningFormat: RootJsonFormat[domain.ServiceWarning] =
    new RootJsonFormat[domain.ServiceWarning] {
      override def read(json: JsValue): domain.ServiceWarning = json match {
        case JsObject(fields) if fields.contains("unknownTemplateIds") =>
          UnknownTemplateIdsFormat.read(json)
        case JsObject(fields) if fields.contains("unknownParties") =>
          UnknownPartiesFormat.read(json)
        case _ =>
          deserializationError(
            s"Expected JsObject(unknownTemplateIds | unknownParties -> JsArray(...)), got: $json"
          )
      }

      override def write(obj: domain.ServiceWarning): JsValue = obj match {
        case x: domain.UnknownTemplateIds => UnknownTemplateIdsFormat.write(x)
        case x: domain.UnknownParties => UnknownPartiesFormat.write(x)
      }
    }

  implicit val AsyncWarningsWrapperFormat: RootJsonFormat[domain.AsyncWarningsWrapper] =
    jsonFormat1(domain.AsyncWarningsWrapper)

  implicit val UnknownTemplateIdsFormat: RootJsonFormat[domain.UnknownTemplateIds] = jsonFormat1(
    domain.UnknownTemplateIds
  )

  implicit val UnknownPartiesFormat: RootJsonFormat[domain.UnknownParties] = jsonFormat1(
    domain.UnknownParties
  )

  implicit def OkResponseFormat[R: JsonFormat]: RootJsonFormat[domain.OkResponse[R]] =
    jsonFormat3(domain.OkResponse[R])

  implicit val ResourceInfoDetailFormat: RootJsonFormat[domain.ResourceInfoDetail] = jsonFormat2(
    domain.ResourceInfoDetail
  )
  implicit val ErrorInfoDetailFormat: RootJsonFormat[domain.ErrorInfoDetail] = jsonFormat2(
    domain.ErrorInfoDetail
  )

  implicit val durationFormat: JsonFormat[domain.RetryInfoDetailDuration] =
    jsonFormat[domain.RetryInfoDetailDuration](
      JsonReader.func2Reader(
        (LongJsonFormat.read _)
          .andThen(scala.concurrent.duration.Duration.fromNanos)
          .andThen(it => domain.RetryInfoDetailDuration(it: scala.concurrent.duration.Duration))
      ),
      JsonWriter.func2Writer[domain.RetryInfoDetailDuration](duration =>
        LongJsonFormat.write(duration.unwrap.toNanos)
      ),
    )

  implicit val RetryInfoDetailFormat: RootJsonFormat[domain.RetryInfoDetail] =
    jsonFormat1(domain.RetryInfoDetail)

  implicit val RequestInfoDetailFormat: RootJsonFormat[domain.RequestInfoDetail] = jsonFormat1(
    domain.RequestInfoDetail
  )

  implicit val ErrorDetailsFormat: JsonFormat[domain.ErrorDetail] = deriveFormat[domain.ErrorDetail]

  implicit val LedgerApiErrorFormat: RootJsonFormat[domain.LedgerApiError] =
    jsonFormat3(domain.LedgerApiError)

  implicit val ErrorResponseFormat: RootJsonFormat[domain.ErrorResponse] =
    jsonFormat4(domain.ErrorResponse)

  implicit def SyncResponseFormat[R: JsonFormat]: RootJsonFormat[domain.SyncResponse[R]] =
    new RootJsonFormat[domain.SyncResponse[R]] {
      private val resultKey = "result"
      private val errorsKey = "errors"
      private val errorMsg =
        s"Invalid ${domain.SyncResponse.getClass.getSimpleName} format, expected a JSON object with either $resultKey or $errorsKey field"

      override def write(obj: domain.SyncResponse[R]): JsValue = obj match {
        case a: domain.OkResponse[_] => OkResponseFormat[R].write(a)
        case b: domain.ErrorResponse => ErrorResponseFormat.write(b)
      }

      override def read(json: JsValue): domain.SyncResponse[R] = json match {
        case JsObject(fields) =>
          (fields get resultKey, fields get errorsKey) match {
            case (Some(_), None) => OkResponseFormat[R].read(json)
            case (None, Some(_)) => ErrorResponseFormat.read(json)
            case _ => deserializationError(errorMsg)
          }
        case _ => deserializationError(errorMsg)
      }
    }
}

sealed abstract class JsonProtocolLow extends DefaultJsonProtocol with ExtraFormats {
  implicit def NonEmptyListReader[A: JsonReader]: JsonReader[NonEmptyList[A]] = {
    case JsArray(hd +: tl) =>
      NonEmptyList(hd.convertTo[A], tl map (_.convertTo[A]): _*)
    case _ => deserializationError("must be a JSON array with at least 1 element")
  }

  implicit def NonEmptyListWriter[A: JsonWriter]: JsonWriter[NonEmptyList[A]] =
    nela => JsArray(nela.map(_.toJson).list.toVector)
}
