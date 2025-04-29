// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.nonempty.NonEmpty
import com.daml.struct.spray.StructJsonFormat
import com.digitalasset.canton.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.canton.http
import com.digitalasset.canton.http.*
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import com.google.protobuf.struct.Struct
import org.apache.pekko.http.scaladsl.model.StatusCode
import scalaz.syntax.std.option.*
import scalaz.syntax.tag.*
import scalaz.{-\/, @@, NonEmptyList, Tag, \/-}
import spray.json.*

import scala.reflect.ClassTag

object JsonProtocol extends JsonProtocolLow {

  implicit val UserIdFormat: JsonFormat[lar.UserId] =
    taggedJsonFormat[String, lar.UserIdTag]

  implicit val PartyFormat: JsonFormat[http.Party] =
    taggedJsonFormat

  implicit val CommandIdFormat: JsonFormat[lar.CommandId] =
    taggedJsonFormat[String, lar.CommandIdTag]

  implicit val ChoiceFormat: JsonFormat[lar.Choice] = taggedJsonFormat[String, lar.ChoiceTag]

  implicit val HttpContractIdFormat: JsonFormat[http.ContractId] =
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

  implicit val OffsetFormat: JsonFormat[http.Offset] =
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

  /** This intuitively pointless extra type is here to give it specificity so this instance will
    * beat CollectionFormats#listFormat. You would normally achieve the conflict resolution by
    * putting this instance in a parent of
    * [[https://javadoc.io/static/io.spray/spray-json_2.12/1.3.5/spray/json/CollectionFormats.html CollectionFormats]],
    * but that kind of extension isn't possible here.
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
    }(bytes => bytesToStrTotal(bytes.toByteArray))

  implicit val Base64Format: JsonFormat[http.Base64] = {
    import java.util.Base64.{getDecoder, getEncoder}
    baseNFormat(getDecoder.decode, getEncoder.encodeToString)
  }

  implicit val Base16Format: JsonFormat[http.Base16] = {
    import com.google.common.io.BaseEncoding.base16

    import java.util.Locale.US
    baseNFormat(s => base16.decode(s toUpperCase US), ba => base16.encode(ba) toLowerCase US)
  }

  implicit val userDetails: JsonFormat[http.UserDetails] =
    jsonFormat2(http.UserDetails.apply)

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

  implicit val PartyDetails: JsonFormat[http.PartyDetails] =
    jsonFormat2(http.PartyDetails.apply)

  implicit val CreateUserRequest: JsonFormat[http.CreateUserRequest] =
    jsonFormat3(http.CreateUserRequest.apply)

  implicit val ListUserRightsRequest: JsonFormat[http.ListUserRightsRequest] =
    jsonFormat1(http.ListUserRightsRequest.apply)

  implicit val GrantUserRightsRequest: JsonFormat[http.GrantUserRightsRequest] =
    jsonFormat2(http.GrantUserRightsRequest.apply)

  implicit val RevokeUserRightsRequest: JsonFormat[http.RevokeUserRightsRequest] =
    jsonFormat2(http.RevokeUserRightsRequest.apply)

  implicit val GetUserRequest: JsonFormat[http.GetUserRequest] =
    jsonFormat1(http.GetUserRequest.apply)

  implicit val DeleteUserRequest: JsonFormat[http.DeleteUserRequest] =
    jsonFormat1(http.DeleteUserRequest.apply)

  implicit val AllocatePartyRequest: JsonFormat[http.AllocatePartyRequest] =
    jsonFormat2(http.AllocatePartyRequest.apply)

  object LfValueCodec
      extends ApiCodecCompressed(
        encodeDecimalAsString = true,
        encodeInt64AsString = true,
      )

  implicit def TemplateIdRequiredPkgIdFormat[CtId[T] <: http.ContractTypeId[T]](implicit
      CtId: http.ContractTypeId.Like[CtId]
  ): RootJsonFormat[CtId[Ref.PackageId]] = new TemplateIdFormat(CtId, Ref.PackageId.fromString)

  implicit def TemplateIdRequiredPkgFormat[CtId[T] <: http.ContractTypeId[T]](implicit
      CtId: http.ContractTypeId.Like[CtId]
  ): RootJsonFormat[CtId[Ref.PackageRef]] = new TemplateIdFormat(CtId, Ref.PackageRef.fromString)

  class TemplateIdFormat[P, CtId[T] <: http.ContractTypeId[T]](
      CtId: http.ContractTypeId.Like[CtId],
      readPkg: (String => Either[String, P]),
  ) extends RootJsonFormat[CtId[P]] {
    override def write(a: CtId[P]) =
      JsString(s"${a.packageId.toString: String}:${a.moduleName: String}:${a.entityName: String}")

    override def read(json: JsValue) = json match {
      case JsString(str) =>
        str.split(':') match {
          case Array(p, m, e) =>
            readPkg(p) match {
              case Left(reason) => error(json, reason)
              case Right(pkgRef) => CtId(pkgRef, m, e)
            }
          case _ => error(json, "did not have two ':' chars")
        }
      case _ => error(json, "not JsString")
    }

    private def error(json: JsValue, reason: String): Nothing =
      deserializationError(s"Expected JsString(<packageId>:<module>:<entity>), got: $json. $reason")
  }

  private[this] def decodeContractRef(
      fields: Map[String, JsValue],
      what: String,
  ): http.InputContractRef[JsValue] =
    (fields get "templateId", fields get "key", fields get "contractId") match {
      case (Some(templateId), Some(key), None) =>
        -\/((templateId.convertTo[http.ContractTypeId.Template.RequiredPkg], key))
      case (otid, None, Some(contractId)) =>
        val a = otid map (_.convertTo[http.ContractTypeId.RequiredPkg])
        val b = contractId.convertTo[http.ContractId]
        \/-((a, b))
      case (None, Some(_), None) =>
        deserializationError(s"$what requires key to be accompanied by a templateId")
      case (_, None, None) | (_, Some(_), Some(_)) =>
        deserializationError(s"$what requires either key or contractId field")
    }

  implicit val EnrichedContractKeyFormat: RootJsonFormat[http.EnrichedContractKey[JsValue]] =
    jsonFormat2(http.EnrichedContractKey.apply[JsValue])

  implicit val EnrichedContractIdFormat: RootJsonFormat[http.EnrichedContractId] =
    jsonFormat2(http.EnrichedContractId.apply)

  private[this] val contractIdAtOffsetKey = "contractIdAtOffset"

  implicit val InitialContractKeyStreamRequest
      : RootJsonReader[http.ContractKeyStreamRequest[Unit, JsValue]] = { jsv =>
    val ekey = jsv.convertTo[http.EnrichedContractKey[JsValue]]
    jsv match {
      case JsObject(fields) if fields contains contractIdAtOffsetKey =>
        deserializationError(
          s"$contractIdAtOffsetKey is not allowed for WebSocket streams starting at the beginning"
        )
      case _ =>
    }
    http.ContractKeyStreamRequest((), ekey)
  }

  implicit val ResumingContractKeyStreamRequest: RootJsonReader[
    http.ContractKeyStreamRequest[Option[Option[http.ContractId]], JsValue]
  ] = { jsv =>
    val off = jsv match {
      case JsObject(fields) => fields get contractIdAtOffsetKey map (_.convertTo[Option[String]])
      case _ => None
    }
    val ekey = jsv.convertTo[http.EnrichedContractKey[JsValue]]
    type OO[+A] = Option[Option[A]]
    http.ContractKeyStreamRequest(http.ContractId.subst[OO, String](off), ekey)
  }

  val ReadersKey = "readers"

  implicit val FetchRequestFormat: RootJsonReader[http.FetchRequest[JsValue]] =
    new RootJsonFormat[http.FetchRequest[JsValue]] {
      override def write(obj: http.FetchRequest[JsValue]): JsValue = {
        val http.FetchRequest(locator, readAs) = obj
        val lj = locator.toJson
        readAs.cata(rl => JsObject(lj.asJsObject.fields.updated(ReadersKey, rl.toJson)), lj)
      }

      override def read(json: JsValue): http.FetchRequest[JsValue] = {
        val jo = json.asJsObject("fetch request must be a JSON object").fields
        http.FetchRequest(
          JsObject(jo - ReadersKey).convertTo[http.ContractLocator[JsValue]],
          jo.get(ReadersKey).flatMap(_.convertTo[Option[NonEmptyList[http.Party]]]),
        )
      }
    }

  implicit val ContractLocatorFormat: RootJsonFormat[http.ContractLocator[JsValue]] =
    new RootJsonFormat[http.ContractLocator[JsValue]] {
      override def write(obj: http.ContractLocator[JsValue]): JsValue = obj match {
        case a: http.EnrichedContractKey[JsValue] => EnrichedContractKeyFormat.write(a)
        case b: http.EnrichedContractId => EnrichedContractIdFormat.write(b)
      }

      override def read(json: JsValue): http.ContractLocator[JsValue] = json match {
        case JsObject(fields) =>
          http.ContractLocator.structure.from(decodeContractRef(fields, "ContractLocator"))
        case _ =>
          deserializationError(s"Cannot read ContractLocator from json: $json")
      }
    }

  implicit val ContractFormat: RootJsonFormat[http.Contract[JsValue]] =
    new RootJsonFormat[http.Contract[JsValue]] {
      private val archivedKey = "archived"
      private val activeKey = "created"

      override def read(json: JsValue): http.Contract[JsValue] = json match {
        case JsObject(fields) =>
          fields.toList match {
            case List((`archivedKey`, archived)) =>
              http.Contract[JsValue](-\/(ArchivedContractFormat.read(archived)))
            case List((`activeKey`, active)) =>
              http.Contract[JsValue](\/-(ActiveContractFormat.read(active)))
            case _ =>
              deserializationError(
                s"Contract must be either {$archivedKey: obj} or {$activeKey: obj}, got: $fields"
              )
          }
        case _ => deserializationError("Contract must be an object")
      }

      override def write(obj: http.Contract[JsValue]): JsValue = obj.value match {
        case -\/(archived) => JsObject(archivedKey -> ArchivedContractFormat.write(archived))
        case \/-(active) => JsObject(activeKey -> ActiveContractFormat.write(active))
      }
    }

  implicit val ActiveContractFormat: RootJsonFormat[http.ActiveContract.ResolvedCtTyId[JsValue]] = {
    implicit val `ctid resolved fmt`: JsonFormat[http.ContractTypeId.ResolvedPkgId] =
      jsonFormatFromReaderWriter(
        TemplateIdRequiredPkgIdFormat[http.ContractTypeId.Template],
        // we only write (below) in main, but read ^ in tests.  For ^, getting
        // the proper contract type ID right doesn't matter
        TemplateIdRequiredPkgIdFormat[http.ContractTypeId],
      )
    jsonFormat6(http.ActiveContract.apply[ContractTypeId.ResolvedPkgId, JsValue])
  }

  implicit val ArchivedContractFormat: RootJsonFormat[http.ArchivedContract] =
    jsonFormat2(http.ArchivedContract.apply)

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
    *   1. template IDs are required
    *   1. error out on unsupported request fields
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

  implicit val GetActiveContractsRequestFormat: RootJsonReader[http.GetActiveContractsRequest] =
    requestJsonReaderPlusOne(ReadersKey)(http.GetActiveContractsRequest.apply)

  implicit val SearchForeverQueryFormat: RootJsonReader[http.SearchForeverQuery] = {
    val OffsetKey = "offset"
    requestJsonReaderPlusOne(OffsetKey)(http.SearchForeverQuery.apply)
  }

  implicit val SearchForeverRequestFormat: RootJsonReader[http.SearchForeverRequest] = {
    case multi @ JsArray(_) =>
      val queriesWithPos = multi.convertTo[NonEmptyList[http.SearchForeverQuery]].zipWithIndex
      http.SearchForeverRequest(queriesWithPos)
    case single =>
      http.SearchForeverRequest(NonEmptyList((single.convertTo[http.SearchForeverQuery], 0)))
  }

  implicit def DisclosedContractFormat[TmplId: JsonFormat]
      : JsonFormat[http.DisclosedContract[TmplId]] = {
    val rawJsonFormat = jsonFormat3(http.DisclosedContract[TmplId].apply)

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

  implicit val hexStringFormat: JsonFormat[Ref.HexString] =
    xemapStringJsonFormat(Ref.HexString.fromString)(identity)

  implicit val PackageIdFormat: JsonFormat[Ref.PackageId] =
    xemapStringJsonFormat(Ref.PackageId.fromString)(identity)

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

  implicit val SubmissionIdFormat: JsonFormat[http.SubmissionId] = taggedJsonFormat

  implicit val WorkflowIdFormat: JsonFormat[http.WorkflowId] = taggedJsonFormat

  implicit def CommandMetaFormat[TmplId: JsonFormat]: JsonFormat[http.CommandMeta[TmplId]] =
    jsonFormat9(http.CommandMeta.apply[TmplId])

  // exposed for testing
  private[json] implicit val CommandMetaNoDisclosedFormat
      : RootJsonFormat[http.CommandMeta.NoDisclosed] = {
    type NeverDC = http.DisclosedContract[Nothing]
    implicit object alwaysEmptyList extends JsonFormat[List[NeverDC]] {
      override def write(obj: List[NeverDC]): JsValue = JsArray()
      override def read(json: JsValue): List[NeverDC] = List.empty
    }
    implicit object noDisclosed extends OptionFormat[List[NeverDC]] {
      override def write(obj: Option[List[NeverDC]]): JsValue = JsNull
      override def read(json: JsValue): Option[List[NeverDC]] = None
      override def readSome(value: JsValue): Some[List[NeverDC]] = Some(List.empty)
    }
    jsonFormat9(http.CommandMeta.apply)
  }

  implicit val CreateCommandFormat: RootJsonFormat[
    http.CreateCommand[JsValue, http.ContractTypeId.Template.RequiredPkg]
  ] =
    jsonFormat3(
      http.CreateCommand[JsValue, http.ContractTypeId.Template.RequiredPkg]
    )

  implicit val ExerciseCommandFormat: RootJsonFormat[
    http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]]
  ] =
    new RootJsonFormat[
      http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]]
    ] {
      override def write(
          obj: http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]]
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
      ): http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]] = {
        val reference = ContractLocatorFormat.read(json)
        val choice = fromField[http.Choice](json, "choice")
        val argument = fromField[JsValue](json, "argument")
        val meta =
          fromField[Option[http.CommandMeta[ContractTypeId.Template.RequiredPkg]]](
            json,
            "meta",
          )

        http.ExerciseCommand(
          reference = reference,
          choice = choice,
          argument = argument,
          choiceInterfaceId =
            fromField[Option[http.ContractTypeId.RequiredPkg]](json, "choiceInterfaceId"),
          meta = meta,
        )
      }
    }

  implicit val CreateAndExerciseCommandFormat: RootJsonFormat[
    http.CreateAndExerciseCommand[
      JsValue,
      JsValue,
      http.ContractTypeId.Template.RequiredPkg,
      http.ContractTypeId.RequiredPkg,
    ]
  ] =
    jsonFormat6(
      http.CreateAndExerciseCommand[
        JsValue,
        JsValue,
        http.ContractTypeId.Template.RequiredPkg,
        http.ContractTypeId.RequiredPkg,
      ]
    )

  implicit val CompletionOffsetFormat: JsonFormat[http.CompletionOffset] =
    taggedJsonFormat[String, http.CompletionOffsetTag]

  implicit val ExerciseResponseFormat: RootJsonFormat[http.ExerciseResponse[JsValue]] =
    jsonFormat3(http.ExerciseResponse[JsValue])

  implicit val CreateCommandResponseFormat: RootJsonFormat[http.CreateCommandResponse[JsValue]] =
    jsonFormat7(http.CreateCommandResponse[JsValue])

  implicit val StatusCodeFormat: RootJsonFormat[StatusCode] =
    new RootJsonFormat[StatusCode] {
      override def read(json: JsValue): StatusCode = json match {
        case JsNumber(x) => StatusCode.int2StatusCode(x.toIntExact)
        case _ => deserializationError(s"Expected JsNumber, got: $json")
      }

      override def write(obj: StatusCode): JsValue = JsNumber(obj.intValue)
    }

  implicit val ServiceWarningFormat: RootJsonFormat[http.ServiceWarning] =
    new RootJsonFormat[http.ServiceWarning] {
      override def read(json: JsValue): http.ServiceWarning = json match {
        case JsObject(fields) if fields.contains("unknownTemplateIds") =>
          UnknownTemplateIdsFormat.read(json)
        case JsObject(fields) if fields.contains("unknownParties") =>
          UnknownPartiesFormat.read(json)
        case _ =>
          deserializationError(
            s"Expected JsObject(unknownTemplateIds | unknownParties -> JsArray(...)), got: $json"
          )
      }

      override def write(obj: http.ServiceWarning): JsValue = obj match {
        case x: http.UnknownTemplateIds => UnknownTemplateIdsFormat.write(x)
        case x: http.UnknownParties => UnknownPartiesFormat.write(x)
      }
    }

  implicit val AsyncWarningsWrapperFormat: RootJsonFormat[http.AsyncWarningsWrapper] =
    jsonFormat1(http.AsyncWarningsWrapper.apply)

  implicit val UnknownTemplateIdsFormat: RootJsonFormat[http.UnknownTemplateIds] = jsonFormat1(
    http.UnknownTemplateIds.apply
  )

  implicit val UnknownPartiesFormat: RootJsonFormat[http.UnknownParties] = jsonFormat1(
    http.UnknownParties.apply
  )

  implicit def OkResponseFormat[R: JsonFormat]: RootJsonFormat[http.OkResponse[R]] =
    jsonFormat3(http.OkResponse[R])

  implicit val ResourceInfoDetailFormat: RootJsonFormat[http.ResourceInfoDetail] = jsonFormat2(
    http.ResourceInfoDetail.apply
  )
  implicit val ErrorInfoDetailFormat: RootJsonFormat[http.ErrorInfoDetail] = jsonFormat2(
    http.ErrorInfoDetail.apply
  )

  implicit val durationFormat: JsonFormat[http.RetryInfoDetailDuration] =
    jsonFormat[http.RetryInfoDetailDuration](
      JsonReader.func2Reader(
        (LongJsonFormat.read _)
          .andThen(scala.concurrent.duration.Duration.fromNanos)
          .andThen(it => http.RetryInfoDetailDuration(it: scala.concurrent.duration.Duration))
      ),
      JsonWriter.func2Writer[http.RetryInfoDetailDuration](duration =>
        LongJsonFormat.write(duration.unwrap.toNanos)
      ),
    )

  implicit val RetryInfoDetailFormat: RootJsonFormat[http.RetryInfoDetail] =
    jsonFormat1(http.RetryInfoDetail.apply)

  implicit val RequestInfoDetailFormat: RootJsonFormat[http.RequestInfoDetail] = jsonFormat1(
    http.RequestInfoDetail.apply
  )

  implicit val ErrorDetailsFormat: JsonFormat[http.ErrorDetail] = {
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

  implicit val LedgerApiErrorFormat: RootJsonFormat[http.LedgerApiError] =
    jsonFormat3(http.LedgerApiError.apply)

  implicit val ErrorResponseFormat: RootJsonFormat[http.ErrorResponse] =
    jsonFormat4(http.ErrorResponse.apply)

  implicit val StructFormat: RootJsonFormat[Struct] = StructJsonFormat

  implicit def SyncResponseFormat[R: JsonFormat]: RootJsonFormat[http.SyncResponse[R]] =
    new RootJsonFormat[http.SyncResponse[R]] {
      private val resultKey = "result"
      private val errorsKey = "errors"
      private val errorMsg =
        s"Invalid ${http.SyncResponse.getClass.getSimpleName} format, expected a JSON object with either $resultKey or $errorsKey field"

      override def write(obj: http.SyncResponse[R]): JsValue = obj match {
        case a: http.OkResponse[_] => OkResponseFormat[R].write(a)
        case b: http.ErrorResponse => ErrorResponseFormat.write(b)
      }

      override def read(json: JsValue): http.SyncResponse[R] = json match {
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
