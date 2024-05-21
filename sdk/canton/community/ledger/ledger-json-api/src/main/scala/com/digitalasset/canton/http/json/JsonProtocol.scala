// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.lf.data.Ref.HexString
import com.daml.lf.value.Value.ContractId
import com.daml.nonempty.NonEmpty
import com.daml.struct.spray.StructJsonFormat
import com.digitalasset.canton.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.canton.http.domain
import com.digitalasset.canton.http.domain.*
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.google.protobuf.ByteString
import com.google.protobuf.struct.Struct
import org.apache.pekko.http.scaladsl.model.StatusCode
import scalaz.syntax.std.option.*
import scalaz.syntax.tag.*
import scalaz.{-\/, @@, NonEmptyList, Tag, \/-}
import spray.json.*

import scala.reflect.ClassTag

object JsonProtocol extends JsonProtocolLow {

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
  private[this] def jsonFormatFromReaderWriter[A](implicit
      R: JsonReader[_ <: A],
      W: JsonWriter[_ >: A],
  ): JsonFormat[A] =
    new JsonFormat[A] {
      override def read(json: JsValue) = R read json
      override def write(obj: A) = W write obj
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

  private[this] def baseNFormat[Tag](
      strToBytesThrowsIAE: String => Array[Byte],
      bytesToStrTotal: Array[Byte] => String,
  ): JsonFormat[ByteString @@ Tag] =
    Tag subst xemapStringJsonFormat { s =>
      for {
        arr <-
          try Right(strToBytesThrowsIAE(s))
          catch {
            case e: IllegalArgumentException => Left(e.getMessage)
          }
      } yield ByteString copyFrom arr
    } { bytes => bytesToStrTotal(bytes.toByteArray) }

  implicit val Base64Format: JsonFormat[domain.Base64] = {
    import java.util.Base64.{getDecoder, getEncoder}
    baseNFormat(getDecoder.decode, getEncoder.encodeToString)
  }

  implicit val Base16Format: JsonFormat[domain.Base16] = {
    import com.google.common.io.BaseEncoding.base16

    import java.util.Locale.US
    baseNFormat(s => base16.decode(s toUpperCase US), ba => base16.encode(ba) toLowerCase US)
  }

  implicit val userDetails: JsonFormat[domain.UserDetails] =
    jsonFormat2(domain.UserDetails.apply)

  implicit val participantAdmin: JsonFormat[ParticipantAdmin.type] =
    jsonFormat0(() => ParticipantAdmin)
  implicit val identityProviderAdmin: JsonFormat[IdentityProviderAdmin.type] =
    jsonFormat0(() => IdentityProviderAdmin)
  implicit val canReadAsAnyParty: JsonFormat[CanReadAsAnyParty.type] =
    jsonFormat0(() => CanReadAsAnyParty)
  implicit val canActAs: JsonFormat[CanActAs] = jsonFormat1(CanActAs.apply)
  implicit val canReadAs: JsonFormat[CanReadAs] = jsonFormat1(CanReadAs.apply)

  implicit val userRight: JsonFormat[UserRight] = {
    val participantAdminTypeName = ParticipantAdmin.getClass.getSimpleName.replace("$", "")
    val identityProviderAdminTypeName =
      IdentityProviderAdmin.getClass.getSimpleName.replace("$", "")
    val canActAsTypeName = classOf[CanActAs].getSimpleName
    val canReadAsTypeName = classOf[CanReadAs].getSimpleName
    val canReadAsAnyPartyTypeName = CanReadAsAnyParty.getClass.getSimpleName.replace("$", "")

    jsonFormatFromADT[UserRight](
      {
        case `participantAdminTypeName` => _ => ParticipantAdmin
        case `identityProviderAdminTypeName` => _ => IdentityProviderAdmin
        case `canActAsTypeName` => canActAs.read(_)
        case `canReadAsTypeName` => canReadAs.read(_)
        case `canReadAsAnyPartyTypeName` => _ => CanReadAsAnyParty
        case typeName => deserializationError(s"Unknown user right type: $typeName")
      },
      {
        case ParticipantAdmin => participantAdmin.write(ParticipantAdmin)
        case IdentityProviderAdmin => identityProviderAdmin.write(IdentityProviderAdmin)
        case canActAsObj: CanActAs => canActAs.write(canActAsObj)
        case canReadAsObj: CanReadAs => canReadAs.write(canReadAsObj)
        case CanReadAsAnyParty => canReadAsAnyParty.write(CanReadAsAnyParty)
      },
    )
  }

  private def jsonFormatFromADT[T: ClassTag](
      fromJs: String => JsObject => T,
      toJs: T => JsValue,
  ): JsonFormat[T] =
    new JsonFormat[T] {
      private val typeDiscriminatorKeyName = "type"

      override def read(json: JsValue): T = {
        val fields = json.asJsObject().fields
        val typeValue = fields.getOrElse(
          typeDiscriminatorKeyName,
          deserializationError(
            s"${implicitly[ClassTag[T]].runtimeClass.getSimpleName} must have a `$typeDiscriminatorKeyName` field"
          ),
        ) match {
          case JsString(value) => value
          case other =>
            deserializationError(
              s"field with key name `$typeDiscriminatorKeyName` must be a JsString, got $other"
            )
        }
        val jsValueWithoutType = JsObject(fields - typeDiscriminatorKeyName)
        fromJs(typeValue)(jsValueWithoutType)
      }
      override def write(obj: T): JsObject = {
        val typeName = obj.getClass.getSimpleName.replace("$", "")
        val jsObj = toJs(obj)
        jsObj.asJsObject.copy(fields =
          jsObj.asJsObject.fields + (typeDiscriminatorKeyName -> JsString(typeName))
        )
      }
    }

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
    def asLfValueCodec(jv: JsValue): JsValue = jv match {
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

  implicit def TemplateIdRequiredPkgFormat[CtId[T] <: domain.ContractTypeId[T]](implicit
      CtId: domain.ContractTypeId.Like[CtId]
  ): RootJsonFormat[CtId[String]] =
    new RootJsonFormat[CtId[String]] {
      override def write(a: CtId[String]) =
        JsString(s"${a.packageId: String}:${a.moduleName: String}:${a.entityName: String}")

      override def read(json: JsValue) = json match {
        case JsString(str) =>
          str.split(':') match {
            case Array(p, m, e) => CtId(p, m, e)
            case _ => error(json)
          }
        case _ => error(json)
      }

      private def error(json: JsValue): Nothing =
        deserializationError(s"Expected JsString(<packageId>:<module>:<entity>), got: $json")
    }

  implicit def TemplateIdOptionalPkgFormat[CtId[T] <: domain.ContractTypeId[T]](implicit
      CtId: domain.ContractTypeId.Like[CtId]
  ): RootJsonFormat[CtId[Option[String]]] = {
    import CtId.OptionalPkg as IdO
    new RootJsonFormat[IdO] {

      override def write(a: IdO): JsValue = a.packageId match {
        case Some(p) => JsString(s"${p: String}:${a.moduleName: String}:${a.entityName: String}")
        case None => JsString(s"${a.moduleName: String}:${a.entityName: String}")
      }

      override def read(json: JsValue): IdO = json match {
        case JsString(str) =>
          str.split(':') match {
            case Array(p, m, e) => CtId(Some(p), m, e)
            case Array(m, e) => CtId(None, m, e)
            case _ => error(json)
          }
        case _ => error(json)
      }

      private def error(json: JsValue): Nothing =
        deserializationError(s"Expected JsString([<packageId>:]<module>:<entity>), got: $json")
    }
  }

  private[this] def decodeContractRef(
      fields: Map[String, JsValue],
      what: String,
  ): domain.InputContractRef[JsValue] =
    (fields get "templateId", fields get "key", fields get "contractId") match {
      case (Some(templateId), Some(key), None) =>
        -\/((templateId.convertTo[domain.ContractTypeId.Template.OptionalPkg], key))
      case (otid, None, Some(contractId)) =>
        val a = otid map (_.convertTo[domain.ContractTypeId.OptionalPkg])
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

  val ReadersKey = "readers"

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

  implicit val ActiveContractFormat
      : RootJsonFormat[domain.ActiveContract.ResolvedCtTyId[JsValue]] = {
    implicit val `ctid resolved fmt`: JsonFormat[domain.ContractTypeId.Resolved] =
      jsonFormatFromReaderWriter(
        TemplateIdRequiredPkgFormat[domain.ContractTypeId.Template],
        // we only write (below) in main, but read ^ in tests.  For ^, getting
        // the proper contract type ID right doesn't matter
        TemplateIdRequiredPkgFormat[domain.ContractTypeId],
      )
    jsonFormat6(domain.ActiveContract.apply[ContractTypeId.Resolved, JsValue])
  }

  implicit val ArchivedContractFormat: RootJsonFormat[domain.ArchivedContract] =
    jsonFormat2(domain.ArchivedContract.apply)

  // Like requestJsonReader, but suitable for exactly one extra field, simply
  // parsing it to the supplied extra type if present.
  // Can generalize to >1 field with singleton types and hlists, if you like
  private def requestJsonReaderPlusOne[Extra: JsonReader, TpId: JsonFormat, Request](
      validExtraField: String
  )(
      toRequest: (
          NonEmpty[Set[TpId]],
          Option[Extra],
      ) => Request
  ): RootJsonReader[Request] =
    requestJsonReader(Set(validExtraField)) { (tids: NonEmpty[Set[TpId]], extra) =>
      toRequest(tids, extra get validExtraField map (_.convertTo[Extra]))
    }

  /** Derived from autogenerated with 2 extra features:
    *  1. template IDs are required
    *  2. error out on unsupported request fields
    */
  private[this] def requestJsonReader[TpId: JsonFormat, Request](validExtraFields: Set[String])(
      toRequest: (
          NonEmpty[Set[TpId]],
          Map[String, JsValue],
      ) => Request
  ): RootJsonReader[Request] = {
    final case class BaseRequest(templateIds: Set[TpId])
    val validKeys = Set("templateIds") ++ validExtraFields
    implicit val primitive: JsonReader[BaseRequest] = jsonFormat1(BaseRequest.apply)
    jsv => {
      val BaseRequest(tids) = jsv.convertTo[BaseRequest]
      val unsupported = jsv.asJsObject.fields.keySet diff validKeys
      if (unsupported.nonEmpty) deserializationError(s"unsupported request fields $unsupported")
      val extraFields = jsv.asJsObject.fields.filter { case (fieldName, _) =>
        validExtraFields(fieldName)
      }
      val nonEmptyTids = NonEmpty from tids getOrElse {
        deserializationError("search requires at least one item in 'templateIds'")
      }
      toRequest(nonEmptyTids, extraFields)
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

  implicit def DisclosedContractFormat[TmplId: JsonFormat]
      : JsonFormat[domain.DisclosedContract[TmplId]] = {
    val rawJsonFormat = jsonFormat3(domain.DisclosedContract[TmplId].apply)

    new JsonFormat[DisclosedContract[TmplId]] {
      override def read(json: JsValue): DisclosedContract[TmplId] = {
        val raw = rawJsonFormat.read(json)
        if ((Base64 unwrap raw.createdEventBlob).isEmpty)
          deserializationError("DisclosedContract.createdEventBlob must not be empty")
        else raw
      }

      override def write(obj: DisclosedContract[TmplId]): JsValue = rawJsonFormat.write(obj)
    }
  }

  implicit val hexStringFormat: JsonFormat[HexString] =
    xemapStringJsonFormat(HexString.fromString)(identity)

  implicit val deduplicationPeriodOffset: JsonFormat[DeduplicationPeriod.Offset] = jsonFormat1(
    DeduplicationPeriod.Offset.apply
  )
  implicit val deduplicationPeriodDuration: JsonFormat[DeduplicationPeriod.Duration] = jsonFormat1(
    DeduplicationPeriod.Duration.apply
  )

  implicit val DeduplicationPeriodFormat: JsonFormat[DeduplicationPeriod] = {
    val deduplicationPeriodOffsetTypeName =
      classOf[DeduplicationPeriod.Offset].getSimpleName
    val deduplicationPeriodDurationTypeName =
      classOf[DeduplicationPeriod.Duration].getSimpleName

    jsonFormatFromADT(
      {
        case `deduplicationPeriodOffsetTypeName` => deduplicationPeriodOffset.read(_)
        case `deduplicationPeriodDurationTypeName` => deduplicationPeriodDuration.read(_)
        case typeName => deserializationError(s"Unknown deduplication period type: $typeName")
      },
      {
        case obj: DeduplicationPeriod.Offset => deduplicationPeriodOffset.write(obj)
        case obj: DeduplicationPeriod.Duration => deduplicationPeriodDuration.write(obj)
      },
    )
  }

  implicit val SubmissionIdFormat: JsonFormat[domain.SubmissionId] = taggedJsonFormat

  implicit val WorkflowIdFormat: JsonFormat[domain.WorkflowId] = taggedJsonFormat

  implicit def CommandMetaFormat[TmplId: JsonFormat]: JsonFormat[domain.CommandMeta[TmplId]] =
    jsonFormat8(domain.CommandMeta.apply[TmplId])

  // exposed for testing
  private[json] implicit val CommandMetaNoDisclosedFormat
      : RootJsonFormat[domain.CommandMeta.NoDisclosed] = {
    type NeverDC = domain.DisclosedContract[Nothing]
    implicit object alwaysEmptyList extends JsonFormat[List[NeverDC]] {
      override def write(obj: List[NeverDC]): JsValue = JsArray()
      override def read(json: JsValue): List[NeverDC] = List.empty
    }
    implicit object noDisclosed extends OptionFormat[List[NeverDC]] {
      override def write(obj: Option[List[NeverDC]]): JsValue = JsNull
      override def read(json: JsValue): Option[List[NeverDC]] = None
      override def readSome(value: JsValue): Some[List[NeverDC]] = Some(List.empty)
    }
    jsonFormat8(domain.CommandMeta.apply)
  }

  implicit val CreateCommandFormat: RootJsonFormat[
    domain.CreateCommand[JsValue, domain.ContractTypeId.Template.OptionalPkg]
  ] =
    jsonFormat3(
      domain.CreateCommand[JsValue, domain.ContractTypeId.Template.OptionalPkg]
    )

  implicit val ExerciseCommandFormat: RootJsonFormat[
    domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]
  ] =
    new RootJsonFormat[
      domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]
    ] {
      override def write(
          obj: domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]
      ): JsValue = {

        val reference: JsObject =
          ContractLocatorFormat.write(obj.reference).asJsObject("reference must be an object")

        val fields =
          reference.fields ++
            Iterable("choice" -> obj.choice.toJson, "argument" -> obj.argument.toJson) ++
            Iterable(
              "meta" -> obj.meta.map(_.toJson),
              "choiceInterfaceId" -> obj.choiceInterfaceId.map(_.toJson),
            ).collect { case (k, Some(v)) => (k, v) }

        JsObject(fields)
      }

      override def read(
          json: JsValue
      ): domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]] = {
        val reference = ContractLocatorFormat.read(json)
        val choice = fromField[domain.Choice](json, "choice")
        val argument = fromField[JsValue](json, "argument")
        val meta =
          fromField[Option[domain.CommandMeta[ContractTypeId.Template.OptionalPkg]]](
            json,
            "meta",
          )

        domain.ExerciseCommand(
          reference = reference,
          choice = choice,
          argument = argument,
          choiceInterfaceId =
            fromField[Option[domain.ContractTypeId.OptionalPkg]](json, "choiceInterfaceId"),
          meta = meta,
        )
      }
    }

  implicit val CreateAndExerciseCommandFormat: RootJsonFormat[
    domain.CreateAndExerciseCommand[
      JsValue,
      JsValue,
      domain.ContractTypeId.Template.OptionalPkg,
      domain.ContractTypeId.OptionalPkg,
    ]
  ] =
    jsonFormat6(
      domain.CreateAndExerciseCommand[
        JsValue,
        JsValue,
        domain.ContractTypeId.Template.OptionalPkg,
        domain.ContractTypeId.OptionalPkg,
      ]
    )

  implicit val CompletionOffsetFormat: JsonFormat[domain.CompletionOffset] =
    taggedJsonFormat[String, domain.CompletionOffsetTag]

  implicit val ExerciseResponseFormat: RootJsonFormat[domain.ExerciseResponse[JsValue]] =
    jsonFormat3(domain.ExerciseResponse[JsValue])

  implicit val CreateCommandResponseFormat: RootJsonFormat[domain.CreateCommandResponse[JsValue]] =
    jsonFormat7(domain.CreateCommandResponse[JsValue])

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

  implicit val ErrorDetailsFormat: JsonFormat[domain.ErrorDetail] = {
    val resourceInfoDetailTypeName = classOf[ResourceInfoDetail].getSimpleName
    val errorInfoDetailTypeName = classOf[ErrorInfoDetail].getSimpleName
    val retryInfoDetailTypeName = classOf[RetryInfoDetail].getSimpleName
    val requestInfoDetailTypeName = classOf[RequestInfoDetail].getSimpleName

    jsonFormatFromADT(
      {
        case `resourceInfoDetailTypeName` => ResourceInfoDetailFormat.read(_)
        case `errorInfoDetailTypeName` => ErrorInfoDetailFormat.read(_)
        case `retryInfoDetailTypeName` => RetryInfoDetailFormat.read(_)
        case `requestInfoDetailTypeName` => RequestInfoDetailFormat.read(_)
        case typeName => deserializationError(s"Unknown error detail type: $typeName")
      },
      {
        case resourceInfoDetail: ResourceInfoDetail =>
          ResourceInfoDetailFormat.write(resourceInfoDetail)
        case errorInfoDetail: ErrorInfoDetail => ErrorInfoDetailFormat.write(errorInfoDetail)
        case retryInfoDetail: RetryInfoDetail => RetryInfoDetailFormat.write(retryInfoDetail)
        case requestInfoDetail: RequestInfoDetail =>
          RequestInfoDetailFormat.write(requestInfoDetail)
      },
    )
  }

  implicit val LedgerApiErrorFormat: RootJsonFormat[domain.LedgerApiError] =
    jsonFormat3(domain.LedgerApiError)

  implicit val ErrorResponseFormat: RootJsonFormat[domain.ErrorResponse] =
    jsonFormat4(domain.ErrorResponse)

  implicit val StructFormat: RootJsonFormat[Struct] = StructJsonFormat

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

  // xmap with an error case for StringJsonFormat
  def xemapStringJsonFormat[A](readFn: String => Either[String, A])(
      writeFn: A => String
  ): RootJsonFormat[A] = new RootJsonFormat[A] {
    private[this] val base = implicitly[JsonFormat[String]]
    override def write(obj: A): JsValue = base.write(writeFn(obj))
    override def read(json: JsValue): A =
      readFn(base.read(json)).fold(deserializationError(_), identity)
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
