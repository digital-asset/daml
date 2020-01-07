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
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.HttpServiceTestFixture.jsonCodecs
import com.digitalasset.http.domain.ContractId
import com.digitalasset.http.domain.TemplateId.OptionalPkg
import com.digitalasset.http.json.SprayJson.objectField
import com.digitalasset.http.json._
import com.digitalasset.http.util.ClientUtil.boxedRecord
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.jwt.JwtSigner
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.value.Record
import com.digitalasset.ledger.api.v1.{value => v}
import com.digitalasset.ledger.client.LedgerClient
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-}
import spray.json._

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging {

  def jdbcConfig: Option[JdbcConfig]

  def staticContentConfig: Option[StaticContentConfig]

  import json.JsonProtocol._

  private val dar1 = requiredResource("docs/quickstart-model.dar")

  private val dar2 = requiredResource("ledger-service/http-json/Account.dar")

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: Materializer = Materializer(asys)
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  private val jwt: Jwt = {
    val decodedJwt = DecodedJwt(
      """{"alg": "HS256", "typ": "JWT"}""",
      s"""{"ledgerId": "${testId: String}", "applicationId": "ledger-service-test", "party": "Alice"}"""
    )
    JwtSigner.HMAC256
      .sign(decodedJwt, "secret")
      .fold(e => fail(s"cannot sign a JWT: ${e.shows}"), identity)
  }

  private val headersWithAuth = List(Authorization(OAuth2BearerToken(jwt.value)))

  protected def withHttpService[A]
    : ((Uri, DomainJsonEncoder, DomainJsonDecoder) => Future[A]) => Future[A] =
    HttpServiceTestFixture
      .withHttpService[A](testId, List(dar1, dar2), jdbcConfig, staticContentConfig)

  protected def withLedger[A]: (LedgerClient => Future[A]) => Future[A] =
    HttpServiceTestFixture.withLedger[A](List(dar1, dar2), testId)

  "contracts/search GET empty results" in withHttpService { (uri: Uri, _, _) =>
    getRequest(uri = uri.withPath(Uri.Path("/contracts/search")))
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

  "contracts/search GET" in withHttpService { (uri: Uri, encoder, _) =>
    searchDataSet.traverse(c => postCreateCommand(c, encoder, uri)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

      getRequest(uri = uri.withPath(Uri.Path("/contracts/search")))
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

  "contracts/search POST with empty query" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject("""{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}]}"""),
      uri,
      encoder
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe searchDataSet.size
    }
  }

  "contracts/search with query, one field" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject(
        """{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}], "currency": "EUR"}"""),
      uri,
      encoder
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 2
      acl.map(a => objectField(a.argument, "currency")) shouldBe List.fill(2)(Some(JsString("EUR")))
    }
  }

  "contracts/search with query, can use number or string for numeric field" in withHttpService {
    (uri, encoder, _) =>
      import scalaz.std.scalaFuture._

      searchDataSet.traverse(c => postCreateCommand(c, encoder, uri)).flatMap {
        rs: List[(StatusCode, JsValue)] =>
          rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

          val queryAmountAsString = jsObject(
            """{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}], "amount": "111.11"}""")

          val queryAmountAsNumber = jsObject(
            """{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}], "amount": 111.11}""")

          List(
            postJsonRequest(uri.withPath(Uri.Path("/contracts/search")), queryAmountAsString),
            postJsonRequest(uri.withPath(Uri.Path("/contracts/search")), queryAmountAsNumber),
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
                    objectField(ac.argument, "amount") shouldBe Some(JsString("111.11"))
                }
            }
          }
      }: Future[Assertion]
  }

  "contracts/search with query, two fields" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject(
        """{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}], "currency": "EUR", "amount": "111.11"}"""),
      uri,
      encoder).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 1
      acl.map(a => objectField(a.argument, "currency")) shouldBe List(Some(JsString("EUR")))
      acl.map(a => objectField(a.argument, "amount")) shouldBe List(Some(JsString("111.11")))
    }
  }

  "contracts/search with query, no results" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject(
        """{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}], "currency": "RUB", "amount": "666.66"}"""),
      uri,
      encoder
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 0
    }
  }

  "contracts/search with invalid JSON query should return error" in withHttpService { (uri, _, _) =>
    postJsonStringRequest(uri.withPath(Uri.Path("/contracts/search")), "{NOT A VALID JSON OBJECT")
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
      postJsonRequest(uri.withPath(Uri.Path("/contracts/search")), query).map {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          activeContractList(output)
      }
    }
  }

  "command/create IOU" in withHttpService { (uri, encoder, decoder) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand()

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val activeContract: JsObject = getResult(output)
        assertActiveContract(activeContract)(command, encoder, decoder)
    }: Future[Assertion]
  }

  "command/create IOU should fail if authorization header is missing" in withHttpService {
    (uri, encoder, _) =>
      val command: domain.CreateCommand[v.Record] = iouCreateCommand()
      val input: JsObject = encoder.encodeR(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/command/create")), input, List()).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          expectedOneErrorMessage(output) should include(
            "missing Authorization header with OAuth 2.0 Bearer Token")
      }: Future[Assertion]
  }

  "command/create IOU with unsupported templateId should return proper error" in withHttpService {
    (uri, encoder, _) =>
      val command: domain.CreateCommand[v.Record] =
        iouCreateCommand().copy(templateId = domain.TemplateId(None, "Iou", "Dummy"))
      val input: JsObject = encoder.encodeR(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/command/create")), input).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
          val unknownTemplateId: domain.TemplateId.NoPkg =
            domain.TemplateId((), command.templateId.moduleName, command.templateId.entityName)
          expectedOneErrorMessage(output) should include(
            s"Cannot resolve template ID, given: ${unknownTemplateId: domain.TemplateId.NoPkg}")
      }: Future[Assertion]
  }

  "command/exercise IOU_Transfer" in withHttpService { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap {
        case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(getResult(createOutput))
          val exercise: domain.ExerciseCommand[v.Value] = iouExerciseTransferCommand(contractId)
          val exerciseJson: JsValue = encoder.encodeV(exercise).valueOr(e => fail(e.shows))

          postJsonRequest(uri.withPath(Uri.Path("/command/exercise")), exerciseJson)
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

  private def assertExerciseResponseNewActiveContract(
      exerciseResponse: JsValue,
      createCmd: domain.CreateCommand[v.Record],
      exerciseCmd: domain.ExerciseCommand[v.Value],
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      uri: Uri
  ): Future[Assertion] = {
    inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](exerciseResponse)) {
      case \/-(domain.ExerciseResponse(JsString(exerciseResult), List(contract1, contract2))) => {
        // checking contracts
        inside(contract1) {
          case domain.Contract(-\/(archivedContract)) =>
            (archivedContract.contractId.unwrap: String) shouldBe (exerciseCmd.contractId.unwrap: String)
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

  "command/exercise IOU_Transfer with unknown contractId should return proper error" in withHttpService {
    (uri, encoder, _) =>
      val contractId = lar.ContractId("NonExistentContractId")
      val exercise: domain.ExerciseCommand[v.Value] = iouExerciseTransferCommand(contractId)
      val exerciseJson: JsValue = encoder.encodeV(exercise).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/command/exercise")), exerciseJson)
        .flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.InternalServerError
            assertStatus(output, StatusCodes.InternalServerError)
            expectedOneErrorMessage(output) should include(
              "couldn't find contract AbsoluteContractId(NonExistentContractId)")
        }: Future[Assertion]
  }

  "command/exercise Archive" in withHttpService { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap {
        case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(getResult(createOutput))
          val exercise: domain.ExerciseCommand[v.Value] = iouArchiveCommand(contractId)
          val exerciseJson: JsValue = encoder.encodeV(exercise).valueOr(e => fail(e.shows))

          postJsonRequest(uri.withPath(Uri.Path("/command/exercise")), exerciseJson)
            .flatMap {
              case (exerciseStatus, exerciseOutput) =>
                exerciseStatus shouldBe StatusCodes.OK
                assertStatus(exerciseOutput, StatusCodes.OK)
                val exercisedResponse: JsObject = getResult(exerciseOutput)
                assertExerciseResponseArchivedContract(decoder, exercisedResponse, exercise)
            }
      }: Future[Assertion]
  }

  private def assertExerciseResponseArchivedContract(
      decoder: DomainJsonDecoder,
      exerciseResponse: JsValue,
      exercise: domain.ExerciseCommand[v.Value]
  ): Assertion = {
    inside(exerciseResponse) {
      case result @ JsObject(_) =>
        inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](result)) {
          case \/-(domain.ExerciseResponse(exerciseResult, List(contract1))) =>
            exerciseResult shouldBe JsObject()
            inside(contract1) {
              case domain.Contract(-\/(archivedContract)) =>
                (archivedContract.contractId.unwrap: String) shouldBe (exercise.contractId.unwrap: String)
            }
        }
    }
  }

  private def assertActiveContract(
      decoder: DomainJsonDecoder,
      actual: domain.ActiveContract[JsValue],
      create: domain.CreateCommand[v.Record],
      exercise: domain.ExerciseCommand[v.Value]): Assertion = {

    val expectedContractFields: Seq[v.RecordField] = create.argument.fields
    val expectedNewOwner: v.Value = exercise.argument.sum.record
      .flatMap(_.fields.headOption)
      .flatMap(_.value)
      .getOrElse(fail("Cannot extract expected newOwner"))

    val active: domain.ActiveContract[v.Value] =
      decoder.decodeUnderlyingValues(actual).valueOr(e => fail(e.shows))

    inside(active.argument.sum.record.map(_.fields)) {
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

  private def assertActiveContract(jsObject: JsObject)(
      command: domain.CreateCommand[Record],
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder) = {

    inside(SprayJson.decode[domain.ActiveContract[JsValue]](jsObject)) {
      case \/-(activeContract) =>
        val expectedArgument: JsValue =
          encoder.encodeUnderlyingRecord(command).map(_.argument).getOrElse(fail)
        (activeContract.argument: JsValue) shouldBe expectedArgument
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
    import json.JsonProtocol._

    val command0: domain.ExerciseCommand[v.Value] = iouExerciseTransferCommand(
      lar.ContractId("a-contract-ID"))

    val x = for {
      jsVal <- encoder.encodeV(command0)
      command1 <- decoder.decodeV[domain.ExerciseCommand](jsVal)
    } yield command1.map(removeRecordId) should ===(command0)

    x.fold(e => fail(e.shows), identity)
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

  "parties endpoint should return all known parties" in withHttpService { (uri, encoder, _) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap {
        case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)
          getRequest(uri = uri.withPath(Uri.Path("/parties")))
            .flatMap {
              case (status, output) =>
                status shouldBe StatusCodes.OK
                assertStatus(output, StatusCodes.OK)
                inside(output) {
                  case JsObject(fields) =>
                    inside(fields.get("result")) {
                      case Some(jsArray) =>
                        inside(SprayJson.decode[List[domain.PartyDetails]](jsArray)) {
                          case \/-(partyDetails) =>
                            val partyNames: Set[String] =
                              partyDetails.map(_.party.unwrap)(breakOut)
                            partyNames should contain("Alice")
                        }
                    }
                }
            }
      }: Future[Assertion]
  }

  "contracts/lookup by contractId" in withHttpService { (uri, encoder, decoder) =>
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

  "contracts/lookup returns {status:200, result:null} when contract is not found" in withHttpService {
    (uri, _, _) =>
      val owner = domain.Party("Alice")
      val accountNumber = "abc123"
      val locator = domain.EnrichedContractKey(
        domain.TemplateId(None, "Account", "Account"),
        JsArray(JsString(owner.unwrap), JsString(accountNumber))
      )
      postContractsLookup(locator, uri.withPath(Uri.Path("/contracts/lookup"))).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          output
            .asJsObject(s"expected JsObject, got: $output")
            .fields
            .get("result") shouldBe Some(JsNull)
      }: Future[Assertion]
  }

  "contracts/lookup by contractKey" in withHttpService { (uri, encoder, decoder) =>
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

  "contracts/lookup by contractKey where Key contains variant and record" in withHttpService {
    (uri, _, _) =>
      val createCommand = jsObject("""{
        "templateId": {
          "moduleName": "Account",
          "entityName": "KeyedByVariantAndRecord"
        },
        "argument": {
          "name": "ABC DEF",
          "party": "Alice",
          "age": 123,
          "fooVariant": {"tag": "Baz", "value": {"baz": "baz value"}},
          "bazRecord": {"baz": "another baz value"}
        }
      }""")

      val lookupRequest =
        jsObject(
          """{
        "templateId": {"moduleName": "Account", "entityName": "KeyedByVariantAndRecord"},
        "key": ["Alice", {"tag": "Baz", "value": {"baz": "baz value"}}, {"baz": "another baz value"}]
      }""")

      postJsonRequest(uri.withPath(Uri.Path("/command/create")), createCommand).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          val contractId: ContractId = getContractId(getResult(output))

          postJsonRequest(uri.withPath(Uri.Path("/contracts/lookup")), lookupRequest).flatMap {
            case (status, output) =>
              status shouldBe StatusCodes.OK
              assertStatus(output, StatusCodes.OK)
              activeContract(output).contractId shouldBe contractId
          }
      }: Future[Assertion]
  }

  "contracts/search by a variant field" in withHttpService { (uri, encoder, decoder) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val now = TimestampConversion.instantToMicros(Instant.now)
    val nowStr = TimestampConversion.microsToInstant(now).toString
    val command: domain.CreateCommand[v.Record] = accountCreateCommand(owner, accountNumber, now)

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        val query = jsObject(s"""{
             "%templates": [{"moduleName": "Account", "entityName": "Account"}],
             "number" : "abc123",
             "status" : {"tag": "Enabled", "value": "${nowStr: String}"}
          }""")

        postJsonRequest(uri.withPath(Uri.Path("/contracts/search")), query).map {
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

  private def lookupContractAndAssert(contractLocator: domain.ContractLocator[JsValue])(
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

  protected def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

  private def removeRecordId(a: v.Value): v.Value = a match {
    case v.Value(v.Value.Sum.Record(r)) if r.recordId.isDefined =>
      v.Value(v.Value.Sum.Record(removeRecordId(r)))
    case _ =>
      a
  }

  private def removeRecordId(a: v.Record): v.Record = a.copy(recordId = None)

  private def iouCreateCommand(
      amount: String = "999.9900000000",
      currency: String = "USD"): domain.CreateCommand[v.Record] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val arg: Record = v.Record(
      fields = List(
        v.RecordField("issuer", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("currency", Some(v.Value(v.Value.Sum.Text(currency)))),
        v.RecordField("amount", Some(v.Value(v.Value.Sum.Numeric(amount)))),
        v.RecordField("observers", Some(v.Value(v.Value.Sum.List(v.List()))))
      ))

    domain.CreateCommand(templateId, arg, None)
  }

  private def iouExerciseTransferCommand(
      contractId: lar.ContractId): domain.ExerciseCommand[v.Value] = {
    val templateId = domain.TemplateId(None, "Iou", "Iou")
    val arg: Record = v.Record(
      fields = List(v.RecordField("newOwner", Some(v.Value(v.Value.Sum.Party("Alice")))))
    )
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(templateId, contractId, choice, boxedRecord(arg), None)
  }

  private def iouArchiveCommand(contractId: lar.ContractId): domain.ExerciseCommand[v.Value] = {
    val templateId = domain.TemplateId(None, "Iou", "Iou")
    val arg: Record = v.Record()
    val choice = lar.Choice("Archive")
    domain.ExerciseCommand(templateId, contractId, choice, boxedRecord(arg), None)
  }

  private def accountCreateCommand(
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

  private def postJsonStringRequest(
      uri: Uri,
      jsonString: String,
      headers: List[HttpHeader] = headersWithAuth): Future[(StatusCode, JsValue)] = {
    logger.info(s"postJson: ${uri.toString} json: ${jsonString: String}")
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, jsonString))
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => (resp.status, body.parseJson))
      }
  }

  private def postJsonRequest(
      uri: Uri,
      json: JsValue,
      headers: List[HttpHeader] = headersWithAuth): Future[(StatusCode, JsValue)] =
    postJsonStringRequest(uri, json.prettyPrint, headers)

  private def getRequest(
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

  private def assertStatus(jsObj: JsValue, expectedStatus: StatusCode): Assertion = {
    inside(jsObj) {
      case JsObject(fields) =>
        inside(fields.get("status")) {
          case Some(JsNumber(status)) => status shouldBe BigDecimal(expectedStatus.intValue)
        }
    }
  }

  private def expectedOneErrorMessage(output: JsValue): String =
    inside(output) {
      case JsObject(fields) =>
        inside(fields.get("errors")) {
          case Some(JsArray(Vector(JsString(errorMsg)))) => errorMsg
        }
    }

  private def postCreateCommand(
      cmd: domain.CreateCommand[v.Record],
      encoder: DomainJsonEncoder,
      uri: Uri): Future[(StatusCode, JsValue)] =
    for {
      json <- toFuture(encoder.encodeR(cmd)): Future[JsObject]
      result <- postJsonRequest(uri.withPath(Uri.Path("/command/create")), json)
    } yield result

  private def postContractsLookup(
      cmd: domain.ContractLocator[JsValue],
      uri: Uri): Future[(StatusCode, JsValue)] =
    for {
      json <- toFuture(SprayJson.encode(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/contracts/lookup")), json)
    } yield result

  private def activeContractList(output: JsValue): List[domain.ActiveContract[JsValue]] = {
    val result = SprayJson
      .objectField(output, "result")
      .getOrElse(fail(s"output: $output is missing result element"))

    SprayJson
      .decode[List[domain.ActiveContract[JsValue]]](result)
      .valueOr(e => fail(e.shows))
  }

  private def activeContract(output: JsValue): domain.ActiveContract[JsValue] = {
    val result = SprayJson
      .objectField(output, "result")
      .getOrElse(fail(s"output: $output is missing result element"))

    SprayJson
      .decode[domain.ActiveContract[JsValue]](result)
      .valueOr(e => fail(e.shows))
  }

  private def getResult(output: JsValue): JsObject = {
    def errorMsg = s"Expected JsObject with 'result' field, got: $output"

    output
      .asJsObject(errorMsg)
      .fields
      .getOrElse("result", fail(errorMsg))
      .asJsObject(errorMsg)
  }

  private def getContractId(result: JsObject): domain.ContractId =
    inside(result.fields.get("contractId")) {
      case Some(JsString(contractId)) => domain.ContractId(contractId)
    }
}
