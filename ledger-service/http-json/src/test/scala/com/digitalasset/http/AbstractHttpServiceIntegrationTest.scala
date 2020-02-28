// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.util.ByteString
import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.daml.bazeltools.BazelRunfiles.requiredResource
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.HttpServiceTestFixture.jsonCodecs
import com.digitalasset.http.domain.ContractId
import com.digitalasset.http.domain.TemplateId.OptionalPkg
import com.digitalasset.http.json.SprayJson.{decode2, objectField}
import com.digitalasset.http.json._
import com.digitalasset.http.util.ClientUtil.boxedRecord
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.TestUtil
import com.digitalasset.jwt.JwtSigner
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.value.Record
import com.digitalasset.ledger.api.v1.{value => v}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.service.MetadataReader
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-}
import spray.json._

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

object AbstractHttpServiceIntegrationTestFuns {
  private val dar1 = requiredResource("docs/quickstart-model.dar")

  private val dar2 = requiredResource("ledger-service/http-json/Account.dar")
}

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
trait AbstractHttpServiceIntegrationTestFuns extends StrictLogging {
  this: AsyncFreeSpec with Matchers with Inside with StrictLogging =>
  import AbstractHttpServiceIntegrationTestFuns._
  import json.JsonProtocol._

  def jdbcConfig: Option[JdbcConfig]

  def staticContentConfig: Option[StaticContentConfig]

  protected def testId: String = this.getClass.getSimpleName

  protected val metdata2: MetadataReader.LfMetadata =
    MetadataReader.readFromDar(dar2).valueOr(e => fail(s"Cannot read dar2 metadata: $e"))

  protected val jwt: Jwt = {
    val decodedJwt = DecodedJwt(
      """{"alg": "HS256", "typ": "JWT"}""",
      s"""{"ledgerId": "${testId: String}", "applicationId": "ledger-service-test", "party": "Alice"}"""
    )
    JwtSigner.HMAC256
      .sign(decodedJwt, "secret")
      .fold(e => fail(s"cannot sign a JWT: ${e.shows}"), identity)
  }

  implicit val `AHS asys`: ActorSystem = ActorSystem(testId)
  implicit val `AHS mat`: Materializer = Materializer(`AHS asys`)
  implicit val `AHS aesf`: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool(testId)(`AHS asys`)
  import shapeless.tag, tag.@@ // used for subtyping to make `AHS ec` beat executionContext
  implicit val `AHS ec`: ExecutionContext @@ this.type = tag[this.type](`AHS asys`.dispatcher)

  protected def withHttpServiceAndClient[A]
    : ((Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerClient) => Future[A]) => Future[A] =
    HttpServiceTestFixture
      .withHttpService[A](testId, List(dar1, dar2), jdbcConfig, staticContentConfig)

  protected def withHttpService[A](
      f: (Uri, DomainJsonEncoder, DomainJsonDecoder) => Future[A]): Future[A] =
    withHttpServiceAndClient((a, b, c, _) => f(a, b, c))

  protected def withLedger[A]: (LedgerClient => Future[A]) => Future[A] =
    HttpServiceTestFixture.withLedger[A](List(dar1, dar2), testId)

  protected def accountCreateCommand(
      owner: domain.Party,
      number: String,
      time: v.Value.Sum.Timestamp = TimestampConversion.instantToMicros(Instant.now))
    : domain.CreateCommand[v.Record] = {
    val templateId = domain.TemplateId(None, "Account", "Account")
    val timeValue = v.Value(time)
    val enabledVariantValue =
      v.Value(v.Value.Sum.Variant(v.Variant(None, "Enabled", Some(timeValue))))
    val arg: Record = v.Record(
      fields = List(
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party(owner.unwrap)))),
        v.RecordField("number", Some(v.Value(v.Value.Sum.Text(number)))),
        v.RecordField("status", Some(enabledVariantValue))
      ))

    domain.CreateCommand(templateId, arg, None)
  }

  private val headersWithAuth = List(Authorization(OAuth2BearerToken(jwt.value)))

  protected def postJsonStringRequest(
      uri: Uri,
      jsonString: String
  ): Future[(StatusCode, JsValue)] =
    TestUtil.postJsonStringRequest(uri, jsonString, headersWithAuth)

  protected def postJsonRequest(
      uri: Uri,
      json: JsValue,
      headers: List[HttpHeader] = headersWithAuth): Future[(StatusCode, JsValue)] =
    TestUtil.postJsonStringRequest(uri, json.prettyPrint, headers)

  protected def getRequest(
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth): Future[(StatusCode, JsValue)] = {
    Http()
      .singleRequest(
        HttpRequest(method = HttpMethods.GET, uri = uri, headers = headers)
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => (resp.status, body.parseJson))
      }
  }

  protected def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

  protected def postCreateCommand(
      cmd: domain.CreateCommand[v.Record],
      encoder: DomainJsonEncoder,
      uri: Uri): Future[(StatusCode, JsValue)] =
    for {
      json <- toFuture(encoder.encodeR(cmd)): Future[JsObject]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/create")), json)
    } yield result

  protected def postArchiveCommand(
      templateId: domain.TemplateId.OptionalPkg,
      contractId: domain.ContractId,
      encoder: DomainJsonEncoder,
      uri: Uri): Future[(StatusCode, JsValue)] = {
    val ref = domain.EnrichedContractId(Some(templateId), contractId)
    val cmd = archiveCommand(ref)
    for {
      json <- toFuture(encoder.encodeExerciseCommand(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), json)
    } yield result
  }

  protected def lookupContractAndAssert(contractLocator: domain.ContractLocator[JsValue])(
      contractId: ContractId,
      create: domain.CreateCommand[v.Record],
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      uri: Uri): Future[Assertion] =
    postContractsLookup(contractLocator, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val result = getResult(output)
        contractId shouldBe getContractId(result)
        assertActiveContract(result)(create, encoder, decoder)
    }

  protected def removeRecordId(a: v.Value): v.Value = a match {
    case v.Value(v.Value.Sum.Record(r)) if r.recordId.isDefined =>
      v.Value(v.Value.Sum.Record(removeRecordId(r)))
    case _ =>
      a
  }

  protected def removeRecordId(a: v.Record): v.Record = a.copy(recordId = None)

  protected def iouCreateCommand(
      amount: String = "999.9900000000",
      currency: String = "USD"): domain.CreateCommand[v.Record] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val arg = v.Record(
      fields = List(
        v.RecordField("issuer", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("currency", Some(v.Value(v.Value.Sum.Text(currency)))),
        v.RecordField("amount", Some(v.Value(v.Value.Sum.Numeric(amount)))),
        v.RecordField("observers", Some(v.Value(v.Value.Sum.List(v.List()))))
      ))

    domain.CreateCommand(templateId, arg, None)
  }

  protected def iouExerciseTransferCommand(
      contractId: lar.ContractId): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
    val templateId = domain.TemplateId(None, "Iou", "Iou")
    val reference = domain.EnrichedContractId(Some(templateId), contractId)
    val arg =
      v.Record(fields = List(v.RecordField("newOwner", Some(v.Value(v.Value.Sum.Party("Bob"))))))
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
  }

  protected def iouCreateAndExerciseTransferCommand(
      amount: String = "999.9900000000",
      currency: String = "USD"): domain.CreateAndExerciseCommand[v.Record, v.Value] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val payload = v.Record(
      fields = List(
        v.RecordField("issuer", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("currency", Some(v.Value(v.Value.Sum.Text(currency)))),
        v.RecordField("amount", Some(v.Value(v.Value.Sum.Numeric(amount)))),
        v.RecordField("observers", Some(v.Value(v.Value.Sum.List(v.List()))))
      ))

    val arg =
      v.Record(fields = List(v.RecordField("newOwner", Some(v.Value(v.Value.Sum.Party("Bob"))))))
    val choice = lar.Choice("Iou_Transfer")

    domain.CreateAndExerciseCommand(
      templateId = templateId,
      payload = payload,
      choice = choice,
      argument = boxedRecord(arg),
      meta = None)
  }

  protected def archiveCommand[Ref](reference: Ref): domain.ExerciseCommand[v.Value, Ref] = {
    val arg: Record = v.Record()
    val choice = lar.Choice("Archive")
    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
  }

  protected def assertStatus(jsObj: JsValue, expectedStatus: StatusCode): Assertion = {
    inside(jsObj) {
      case JsObject(fields) =>
        inside(fields.get("status")) {
          case Some(JsNumber(status)) => status shouldBe BigDecimal(expectedStatus.intValue)
        }
    }
  }

  protected def expectedOneErrorMessage(output: JsValue): String =
    inside(output) {
      case JsObject(fields) =>
        inside(fields.get("errors")) {
          case Some(JsArray(Vector(JsString(errorMsg)))) => errorMsg
        }
    }

  protected def postContractsLookup(
      cmd: domain.ContractLocator[JsValue],
      uri: Uri): Future[(StatusCode, JsValue)] =
    for {
      json <- toFuture(SprayJson.encode(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), json)
    } yield result

  protected def activeContractList(output: JsValue): List[domain.ActiveContract[JsValue]] = {
    val result = getResult(output)
    SprayJson
      .decode[List[domain.ActiveContract[JsValue]]](result)
      .valueOr(e => fail(e.shows))
  }

  protected def activeContract(output: JsValue): domain.ActiveContract[JsValue] = {
    val result = getResult(output)
    SprayJson
      .decode[domain.ActiveContract[JsValue]](result)
      .valueOr(e => fail(e.shows))
  }

  protected def getResult(output: JsValue): JsValue = getChild(output, "result")

  protected def getWarnings(output: JsValue): JsValue = getChild(output, "warnings")

  protected def getChild(output: JsValue, field: String): JsValue = {
    def errorMsg = s"Expected JsObject with '$field' field, got: $output"
    output
      .asJsObject(errorMsg)
      .fields
      .getOrElse(field, fail(errorMsg))
  }

  protected def getContractId(result: JsValue): domain.ContractId =
    inside(result.asJsObject.fields.get("contractId")) {
      case Some(JsString(contractId)) => domain.ContractId(contractId)
    }

  protected def asContractId(a: JsValue): domain.ContractId = inside(a) {
    case JsString(x) => domain.ContractId(x)
  }

  protected def encodeExercise(encoder: DomainJsonEncoder)(
      exercise: domain.ExerciseCommand[v.Value, domain.ContractLocator[v.Value]]): JsValue =
    encoder.encodeExerciseCommand(exercise).getOrElse(fail(s"Cannot encode: $exercise"))

  protected def decodeExercise(decoder: DomainJsonDecoder)(
      jsVal: JsValue): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {

    import scalaz.syntax.bifunctor._

    val cmd = decoder.decodeExerciseCommand(jsVal).getOrElse(fail(s"Cannot decode $jsVal"))
    cmd.bimap(
      lfToApi,
      enrichedContractIdOnly
    )
  }

  protected def enrichedContractIdOnly(x: domain.ContractLocator[_]): domain.EnrichedContractId =
    x match {
      case a: domain.EnrichedContractId => a
      case _: domain.EnrichedContractKey[_] =>
        fail(s"Expected domain.EnrichedContractId, got: $x")
    }

  protected def lfToApi(lfVal: domain.LfValue): v.Value =
    LfEngineToApi.lfValueToApiValue(verbose = true, lfVal).fold(e => fail(e), identity)

  protected def assertActiveContract(
      decoder: DomainJsonDecoder,
      actual: domain.ActiveContract[JsValue],
      create: domain.CreateCommand[v.Record],
      exercise: domain.ExerciseCommand[v.Value, _]): Assertion = {

    val expectedContractFields: Seq[v.RecordField] = create.payload.fields
    val expectedNewOwner: v.Value = exercise.argument.sum.record
      .flatMap(_.fields.headOption)
      .flatMap(_.value)
      .getOrElse(fail("Cannot extract expected newOwner"))

    val active: domain.ActiveContract[v.Value] =
      decoder.decodeUnderlyingValues(actual).valueOr(e => fail(e.shows))

    inside(active.payload.sum.record.map(_.fields)) {
      case Some(
          Seq(
            v.RecordField("iou", Some(contractRecord)),
            v.RecordField("newOwner", Some(newOwner)))) =>
        val contractFields: Seq[v.RecordField] =
          contractRecord.sum.record.map(_.fields).getOrElse(Seq.empty)
        (contractFields: Seq[v.RecordField]) shouldBe (expectedContractFields: Seq[v.RecordField])
        (newOwner: v.Value) shouldBe (expectedNewOwner: v.Value)
    }
  }

  protected def assertActiveContract(jsVal: JsValue)(
      command: domain.CreateCommand[Record],
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder): Assertion = {

    inside(SprayJson.decode[domain.ActiveContract[JsValue]](jsVal)) {
      case \/-(activeContract) =>
        val expectedPayload: JsValue =
          encoder.encodeUnderlyingRecord(command).map(_.payload).getOrElse(fail)
        (activeContract.payload: JsValue) shouldBe expectedPayload
    }
  }

  protected def assertTemplateId(
      actual: domain.TemplateId.RequiredPkg,
      expected: domain.TemplateId.OptionalPkg): Assertion = {
    expected.packageId.foreach(x => actual.packageId shouldBe x)
    actual.moduleName shouldBe expected.moduleName
    actual.entityName shouldBe expected.entityName
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns {
  import json.JsonProtocol._

  "query GET empty results" in withHttpService { (uri: Uri, _, _) =>
    getRequest(uri = uri.withPath(Uri.Path("/v1/query")))
      .flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          inside(output) {
            case JsObject(fields) =>
              inside(fields.get("result")) {
                case Some(JsArray(vector)) => vector should have size 0L
              }
          }
      }: Future[Assertion]
  }

  protected val searchDataSet: List[domain.CreateCommand[v.Record]] = List(
    iouCreateCommand(amount = "111.11", currency = "EUR"),
    iouCreateCommand(amount = "222.22", currency = "EUR"),
    iouCreateCommand(amount = "333.33", currency = "GBP"),
    iouCreateCommand(amount = "444.44", currency = "BTC"),
  )

  "query GET" in withHttpService { (uri: Uri, encoder, _) =>
    searchDataSet.traverse(c => postCreateCommand(c, encoder, uri)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

      getRequest(uri = uri.withPath(Uri.Path("/v1/query")))
        .flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            inside(output) {
              case JsObject(fields) =>
                inside(fields.get("result")) {
                  case Some(JsArray(vector)) =>
                    vector should have size searchDataSet.size.toLong
                }
            }
        }: Future[Assertion]
    }
  }

  "query POST with empty query" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"]}"""),
      uri,
      encoder
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe searchDataSet.size
    }
  }

  "query with query, one field" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
      uri,
      encoder
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 2
      acl.map(a => objectField(a.payload, "currency")) shouldBe List.fill(2)(Some(JsString("EUR")))
    }
  }

  "query returns unknown Template IDs as warnings" in withHttpService { (uri, _, _) =>
    val query =
      jsObject(
        """{"templateIds": ["Iou:Iou", "UnknownModule:UnknownEntity"], "query": {"currency": "EUR"}}""")

    postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query)
      .map {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getResult(output) shouldBe JsArray.empty
          getWarnings(output) shouldBe JsObject(
            "unknownTemplateIds" -> JsArray(Vector(JsString("UnknownModule:UnknownEntity"))))
      }
  }

  "query with query, can use number or string for numeric field" in withHttpService {
    (uri, encoder, _) =>
      import scalaz.std.scalaFuture._

      searchDataSet.traverse(c => postCreateCommand(c, encoder, uri)).flatMap {
        rs: List[(StatusCode, JsValue)] =>
          rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

          def queryAmountAs(s: String) =
            jsObject(s"""{"templateIds": ["Iou:Iou"], "query": {"amount": $s}}""")
          val queryAmountAsString = queryAmountAs("\"111.11\"")
          val queryAmountAsNumber = queryAmountAs("111.11")

          List(
            postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsString),
            postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsNumber),
          ).sequence.flatMap { rs: List[(StatusCode, JsValue)] =>
            rs.map(_._1) shouldBe List.fill(2)(StatusCodes.OK)
            inside(rs.map(_._2)) {
              case List(jsVal1, jsVal2) =>
                jsVal1 shouldBe jsVal2
                val acl1: List[domain.ActiveContract[JsValue]] = activeContractList(jsVal1)
                val acl2: List[domain.ActiveContract[JsValue]] = activeContractList(jsVal2)
                acl1 shouldBe acl2
                inside(acl1) {
                  case List(ac) =>
                    objectField(ac.payload, "amount") shouldBe Some(JsString("111.11"))
                }
            }
          }
      }: Future[Assertion]
  }

  "query with query, two fields" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject(
        """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""),
      uri,
      encoder).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 1
      acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
      acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
    }
  }

  "query with query, no results" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject(
        """{"templateIds": ["Iou:Iou"], "query": {"currency": "RUB", "amount": "666.66"}}"""),
      uri,
      encoder
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 0
    }
  }

  "query with invalid JSON query should return error" in withHttpService { (uri, _, _) =>
    postJsonStringRequest(uri.withPath(Uri.Path("/v1/query")), "{NOT A VALID JSON OBJECT")
      .flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
      }: Future[Assertion]
  }

  protected def jsObject(s: String): JsObject = {
    val r: JsonError \/ JsObject = for {
      jsVal <- SprayJson.parse(s).leftMap(e => JsonError(e.shows))
      jsObj <- SprayJson.mustBeJsObject(jsVal)
    } yield jsObj
    r.valueOr(e => fail(e.shows))
  }

  protected def searchWithQuery(
      commands: List[domain.CreateCommand[v.Record]],
      query: JsObject,
      uri: Uri,
      encoder: DomainJsonEncoder): Future[List[domain.ActiveContract[JsValue]]] = {
    commands.traverse(c => postCreateCommand(c, encoder, uri)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(commands.size)(StatusCodes.OK)
      postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query).map {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          output.asJsObject.fields.get("warnings") shouldBe None
          activeContractList(output)
      }
    }
  }

  "create IOU" in withHttpService { (uri, encoder, decoder) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand()

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val activeContract = getResult(output)
        assertActiveContract(activeContract)(command, encoder, decoder)
    }: Future[Assertion]
  }

  "create IOU should fail if authorization header is missing" in withHttpService {
    (uri, encoder, _) =>
      val command: domain.CreateCommand[v.Record] = iouCreateCommand()
      val input: JsObject = encoder.encodeR(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input, List()).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          expectedOneErrorMessage(output) should include(
            "missing Authorization header with OAuth 2.0 Bearer Token")
      }: Future[Assertion]
  }

  "create IOU with unsupported templateId should return proper error" in withHttpService {
    (uri, encoder, _) =>
      val command: domain.CreateCommand[v.Record] =
        iouCreateCommand().copy(templateId = domain.TemplateId(None, "Iou", "Dummy"))
      val input: JsObject = encoder.encodeR(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
          val unknownTemplateId: domain.TemplateId.OptionalPkg =
            domain.TemplateId(None, command.templateId.moduleName, command.templateId.entityName)
          expectedOneErrorMessage(output) should include(
            s"Cannot resolve template ID, given: ${unknownTemplateId: domain.TemplateId.OptionalPkg}")
      }: Future[Assertion]
  }

  "exercise IOU_Transfer" in withHttpService { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap {
        case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(getResult(createOutput))
          val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
            iouExerciseTransferCommand(contractId)
          val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

          postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
            .flatMap {
              case (exerciseStatus, exerciseOutput) =>
                exerciseStatus shouldBe StatusCodes.OK
                assertStatus(exerciseOutput, StatusCodes.OK)
                assertExerciseResponseNewActiveContract(
                  getResult(exerciseOutput),
                  create,
                  exercise,
                  encoder,
                  decoder,
                  uri)
            }
      }: Future[Assertion]
  }

  "create-and-exercise IOU_Transfer" in withHttpService { (uri, encoder, _) =>
    import encoder.implicits._

    val cmd: domain.CreateAndExerciseCommand[v.Record, v.Value] =
      iouCreateAndExerciseTransferCommand()

    val json: JsValue = SprayJson.encode2(cmd).valueOr(e => fail(e.shows))

    postJsonRequest(uri.withPath(Uri.Path("/v1/create-and-exercise")), json)
      .flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          inside(
            decode2[domain.OkResponse, domain.ExerciseResponse[JsValue], Unit](output)
          ) {
            case \/-(response) =>
              response.status shouldBe StatusCodes.OK
              (response.warnings: Option[Unit]) shouldBe Option.empty[Unit]
              inside(response.result.events) {
                case List(
                    domain.Contract(\/-(created0)),
                    domain.Contract(-\/(archived0)),
                    domain.Contract(\/-(created1))) =>
                  assertTemplateId(created0.templateId, cmd.templateId)
                  assertTemplateId(archived0.templateId, cmd.templateId)
                  archived0.contractId shouldBe created0.contractId
                  assertTemplateId(
                    created1.templateId,
                    domain.TemplateId(None, "Iou", "IouTransfer"))
                  asContractId(response.result.exerciseResult) shouldBe created1.contractId
              }
          }
      }: Future[Assertion]
  }

  private def assertExerciseResponseNewActiveContract(
      exerciseResponse: JsValue,
      createCmd: domain.CreateCommand[v.Record],
      exerciseCmd: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      uri: Uri
  ): Future[Assertion] = {
    inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](exerciseResponse)) {
      case \/-(domain.ExerciseResponse(JsString(exerciseResult), List(contract1, contract2))) => {
        // checking contracts
        inside(contract1) {
          case domain.Contract(-\/(archivedContract)) =>
            (archivedContract.contractId.unwrap: String) shouldBe (exerciseCmd.reference.contractId.unwrap: String)
        }
        inside(contract2) {
          case domain.Contract(\/-(activeContract)) =>
            assertActiveContract(decoder, activeContract, createCmd, exerciseCmd)
        }
        // checking exerciseResult
        exerciseResult.length should be > (0)
        val newContractLocator = domain.EnrichedContractId(
          Some(domain.TemplateId(None, "Iou", "IouTransfer")),
          domain.ContractId(exerciseResult)
        )
        postContractsLookup(newContractLocator, uri).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            getContractId(getResult(output)) shouldBe newContractLocator.contractId
        }: Future[Assertion]
      }
    }
  }

  "exercise IOU_Transfer with unknown contractId should return proper error" in withHttpService {
    (uri, encoder, _) =>
      val contractId = lar.ContractId("NonExistentContractId")
      val exerciseJson: JsValue = encodeExercise(encoder)(iouExerciseTransferCommand(contractId))
      postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
        .flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.InternalServerError
            assertStatus(output, StatusCodes.InternalServerError)
            expectedOneErrorMessage(output) should include(
              "couldn't find contract AbsoluteContractId(NonExistentContractId)")
        }: Future[Assertion]
  }

  "exercise Archive" in withHttpService { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap {
        case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(getResult(createOutput))
          val templateId = domain.TemplateId(None, "Iou", "Iou")
          val reference = domain.EnrichedContractId(Some(templateId), contractId)
          val exercise = archiveCommand(reference)
          val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

          postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
            .flatMap {
              case (exerciseStatus, exerciseOutput) =>
                exerciseStatus shouldBe StatusCodes.OK
                assertStatus(exerciseOutput, StatusCodes.OK)
                val exercisedResponse: JsObject = getResult(exerciseOutput).asJsObject
                assertExerciseResponseArchivedContract(decoder, exercisedResponse, exercise)
            }
      }: Future[Assertion]
  }

  private def assertExerciseResponseArchivedContract(
      decoder: DomainJsonDecoder,
      exerciseResponse: JsValue,
      exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId]
  ): Assertion = {
    inside(exerciseResponse) {
      case result @ JsObject(_) =>
        inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](result)) {
          case \/-(domain.ExerciseResponse(exerciseResult, List(contract1))) =>
            exerciseResult shouldBe JsObject()
            inside(contract1) {
              case domain.Contract(-\/(archivedContract)) =>
                (archivedContract.contractId.unwrap: String) shouldBe (exercise.reference.contractId.unwrap: String)
            }
        }
    }
  }

  "should be able to serialize and deserialize domain commands" in withLedger { client =>
    jsonCodecs(client).map {
      case (encoder, decoder) =>
        testCreateCommandEncodingDecoding(encoder, decoder)
        testExerciseCommandEncodingDecoding(encoder, decoder)
    }: Future[Assertion]
  }

  private def testCreateCommandEncodingDecoding(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder): Assertion = {
    import json.JsonProtocol._

    val command0: domain.CreateCommand[v.Record] = iouCreateCommand()

    val x = for {
      jsonObj <- encoder.encodeR(command0)
      command1 <- decoder.decodeR[domain.CreateCommand](jsonObj)
    } yield command1.map(removeRecordId) should ===(command0)

    x.fold(e => fail(e.shows), identity)
  }

  private def testExerciseCommandEncodingDecoding(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder): Assertion = {
    val command0 = iouExerciseTransferCommand(lar.ContractId("a-contract-ID"))
    val jsVal: JsValue = encodeExercise(encoder)(command0)
    val command1 = decodeExercise(decoder)(jsVal)
    command1.bimap(removeRecordId, identity) should ===(command0)
  }

  "request non-existent endpoint should return 404 with errors" in withHttpService {
    (uri: Uri, _, _) =>
      val badUri = uri.withPath(Uri.Path("/contracts/does-not-exist"))
      getRequest(uri = badUri)
        .flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.NotFound
            assertStatus(output, StatusCodes.NotFound)
            expectedOneErrorMessage(output) shouldBe s"${HttpMethods.GET: HttpMethod}, uri: ${badUri: Uri}"
        }: Future[Assertion]
  }

  "parties endpoint should return all known parties" in withHttpServiceAndClient {
    (uri, _, _, client) =>
      import scalaz.std.vector._
      val partyIds = Vector("Alice", "Bob", "Charlie", "Dave")
      val partyManagement = client.partyManagementClient

      partyIds
        .traverse { p =>
          partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          getRequest(uri = uri.withPath(Uri.Path("/v1/parties"))).flatMap {
            case (status, output) =>
              status shouldBe StatusCodes.OK
              inside(
                decode2[domain.OkResponse, List[domain.PartyDetails], Unit](output)
              ) {
                case \/-(response) =>
                  response.status shouldBe StatusCodes.OK
                  response.warnings shouldBe None
                  val actualIds: Set[domain.Party] = response.result.map(_.identifier)(breakOut)
                  actualIds shouldBe domain.Party.subst(partyIds.toSet)
                  response.result.toSet shouldBe
                    allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
              }
          }
        }: Future[Assertion]
  }

  "parties endpoint should return only requested parties, unknown parties returned as warnings" in withHttpServiceAndClient {
    (uri, _, _, client) =>
      import scalaz.std.vector._

      val charlie = domain.Party("Charlie")
      val knownParties = domain.Party.subst(Vector("Alice", "Bob", "Dave")) :+ charlie
      val erin = domain.Party("Erin")
      val requestedPartyIds: Vector[domain.Party] = knownParties.filterNot(_ == charlie) :+ erin

      val partyManagement = client.partyManagementClient

      knownParties
        .traverse { p =>
          partyManagement.allocateParty(Some(p.unwrap), Some(s"${p.unwrap} & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          postJsonRequest(
            uri = uri.withPath(Uri.Path("/v1/parties")),
            JsArray(requestedPartyIds.map(x => JsString(x.unwrap)))
          ).flatMap {
            case (status, output) =>
              status shouldBe StatusCodes.OK
              inside(
                decode2[domain.OkResponse, List[domain.PartyDetails], domain.UnknownParties](output)
              ) {
                case \/-(response) =>
                  response.status shouldBe StatusCodes.OK
                  response.warnings shouldBe Some(domain.UnknownParties(List(erin)))
                  val actualIds: Set[domain.Party] = response.result.map(_.identifier)(breakOut)
                  actualIds shouldBe requestedPartyIds.toSet - erin // Erin is not known
                  val expected: Set[domain.PartyDetails] = allocatedParties.toSet
                    .map(domain.PartyDetails.fromLedgerApi)
                    .filterNot(_.identifier == charlie)
                  response.result.toSet shouldBe expected
              }
          }
        }: Future[Assertion]
  }

  "parties endpoint should error if empty array passed as input" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(Vector.empty)
      ).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
          val errorMsg = expectedOneErrorMessage(output)
          errorMsg should include("Cannot read JSON: <[]>")
          errorMsg should include("must be a list with at least 1 element")
      }: Future[Assertion]
  }

  "fetch by contractId" in withHttpService { (uri, encoder, decoder) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand()

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))
        val locator = domain.EnrichedContractId(None, contractId)
        lookupContractAndAssert(locator)(contractId, command, encoder, decoder, uri)
    }: Future[Assertion]
  }

  "fetch returns {status:200, result:null} when contract is not found" in withHttpService {
    (uri, _, _) =>
      val owner = domain.Party("Alice")
      val accountNumber = "abc123"
      val locator = domain.EnrichedContractKey(
        domain.TemplateId(None, "Account", "Account"),
        JsArray(JsString(owner.unwrap), JsString(accountNumber))
      )
      postContractsLookup(locator, uri.withPath(Uri.Path("/v1/fetch"))).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          output
            .asJsObject(s"expected JsObject, got: $output")
            .fields
            .get("result") shouldBe Some(JsNull)
      }: Future[Assertion]
  }

  "fetch by key" in withHttpService { (uri, encoder, decoder) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val command: domain.CreateCommand[v.Record] = accountCreateCommand(owner, accountNumber)

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))
        val locator = domain.EnrichedContractKey(
          domain.TemplateId(None, "Account", "Account"),
          JsArray(JsString(owner.unwrap), JsString(accountNumber))
        )
        lookupContractAndAssert(locator)(contractId, command, encoder, decoder, uri)
    }: Future[Assertion]
  }

  "commands/exercise Archive by key" in withHttpService { (uri, encoder, decoder) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val create: domain.CreateCommand[v.Record] = accountCreateCommand(owner, accountNumber)

    val keyRecord = v.Record(
      fields = Seq(
        v.RecordField(value = Some(v.Value(v.Value.Sum.Party(owner.unwrap)))),
        v.RecordField(value = Some(v.Value(v.Value.Sum.Text(accountNumber)))),
      ))
    val locator = domain.EnrichedContractKey[v.Value](
      domain.TemplateId(None, "Account", "Account"),
      v.Value(v.Value.Sum.Record(keyRecord))
    )
    val archive: domain.ExerciseCommand[v.Value, domain.EnrichedContractKey[v.Value]] =
      archiveCommand(locator)
    val archiveJson: JsValue = encodeExercise(encoder)(archive)

    postCreateCommand(create, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)

        postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), archiveJson).flatMap {
          case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
        }
    }: Future[Assertion]
  }

  "fetch by key containing variant and record, encoded as array" in withHttpService { (uri, _, _) =>
    testFetchByCompositeKey(
      uri,
      jsObject("""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": [
              "Alice",
              {"tag": "Baz", "value": {"baz": "baz value"}},
              {"baz": "another baz value"}
            ]
          }""")
    )
  }

  "fetch by key containing variant and record, encoded as record" in withHttpService {
    (uri, _, _) =>
      testFetchByCompositeKey(
        uri,
        jsObject("""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": {
              "_1": "Alice",
              "_2": {"tag": "Baz", "value": {"baz": "baz value"}},
              "_3": {"baz": "another baz value"}
            }
          }""")
      )
  }

  private def testFetchByCompositeKey(uri: Uri, request: JsObject) = {
    val createCommand = jsObject("""{
        "templateId": "Account:KeyedByVariantAndRecord",
        "payload": {
          "name": "ABC DEF",
          "party": "Alice",
          "age": 123,
          "fooVariant": {"tag": "Baz", "value": {"baz": "baz value"}},
          "bazRecord": {"baz": "another baz value"}
        }
      }""")

    postJsonRequest(uri.withPath(Uri.Path("/v1/create")), createCommand).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), request).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            activeContract(output).contractId shouldBe contractId
        }
    }: Future[Assertion]
  }

  "query by a variant field" in withHttpService { (uri, encoder, decoder) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val now = TimestampConversion.instantToMicros(Instant.now)
    val nowStr = TimestampConversion.microsToInstant(now).toString
    val command: domain.CreateCommand[v.Record] = accountCreateCommand(owner, accountNumber, now)

    val packageId: Ref.PackageId = MetadataReader
      .templateByName(metdata2)(Ref.QualifiedName.assertFromString("Account:Account"))
      .headOption
      .map(_._1)
      .getOrElse(fail(s"Cannot retrieve packageId"))

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        val query = jsObject(s"""{
             "templateIds": ["$packageId:Account:Account"],
             "query": {
                 "number" : "abc123",
                 "status" : {"tag": "Enabled", "value": "${nowStr: String}"}
             }
          }""")

        postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query).map {
          case (searchStatus, searchOutput) =>
            searchStatus shouldBe StatusCodes.OK
            assertStatus(searchOutput, StatusCodes.OK)
            inside(activeContractList(searchOutput)) {
              case List(ac) =>
                ac.contractId shouldBe contractId
            }
        }
    }: Future[Assertion]
  }
}
