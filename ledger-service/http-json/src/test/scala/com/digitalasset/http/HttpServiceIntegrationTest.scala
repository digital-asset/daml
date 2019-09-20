// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.HttpServiceTestFixture.{jsonCodecs, withHttpService, withLedger}
import com.digitalasset.http.domain.TemplateId.OptionalPkg
import com.digitalasset.http.json.SprayJson.objectField
import com.digitalasset.http.json._
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.TestUtil.requiredFile
import com.digitalasset.jwt.JwtSigner
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.value.Record
import com.digitalasset.ledger.api.v1.{value => v}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{\/, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class HttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging {

  import json.JsonProtocol._

  private val dar = requiredFile(rlocation("docs/quickstart-model.dar"))
    .fold(e => throw new IllegalStateException(e), identity)

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: ActorMaterializer = ActorMaterializer()
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

  "contracts/search without query" in withHttpService(dar, testId) { (uri: Uri, _, _) =>
    getRequest(uri = uri.withPath(Uri.Path("/contracts/search")))
      .flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          inside(output) {
            case JsObject(fields) =>
              inside(fields.get("result")) {
                case Some(JsArray(result)) => result.length should be > 0
              }
          }
      }: Future[Assertion]
  }

  private val searchDataSet: List[domain.CreateCommand[v.Record]] = List(
    iouCreateCommand(amount = "111.11", currency = "EUR"),
    iouCreateCommand(amount = "222.22", currency = "EUR"),
    iouCreateCommand(amount = "333.33", currency = "GBP"),
    iouCreateCommand(amount = "444.44", currency = "BTC"),
  )

  "contracts/search with query, one field" in withHttpService(dar, testId) { (uri, encoder, _) =>
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

  "contracts/search with query, can use number or string for numeric field" in
    withHttpService(dar, testId) { (uri, encoder, _) =>
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
            }
          }
      }
    }

  "contracts/search with query, two fields" in withHttpService(dar, testId) { (uri, encoder, _) =>
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

  "contracts/search with query, no results" in withHttpService(dar, testId) { (uri, encoder, _) =>
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

  private def jsObject(s: String): JsObject = {
    val r: JsonError \/ JsObject = for {
      jsVal <- SprayJson.parse(s).leftMap(e => JsonError(e.shows))
      jsObj <- SprayJson.mustBeJsObject(jsVal)
    } yield jsObj
    r.valueOr(e => fail(e.shows))
  }

  private def searchWithQuery(
      commands: List[domain.CreateCommand[v.Record]],
      query: JsObject,
      uri: Uri,
      encoder: DomainJsonEncoder): Future[List[domain.ActiveContract[JsValue]]] = {
    import scalaz.std.scalaFuture._

    commands.traverse(c => postCreateCommand(c, encoder, uri)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(commands.size)(StatusCodes.OK)
      postJsonRequest(uri.withPath(Uri.Path("/contracts/search")), query).map {
        case (status, output) =>
          status shouldBe StatusCodes.OK

          val result = SprayJson
            .objectField(output, "result")
            .getOrElse(fail(s"output: $output is missing result element"))

          val searchResponse = SprayJson
            .decode[List[domain.GetActiveContractsResponse[JsValue]]](result)
            .valueOr(e => fail(e.shows))

          activeContractList(searchResponse): List[domain.ActiveContract[JsValue]]
      }
    }
  }

  "command/create IOU" in withHttpService(dar, testId) { (uri, encoder, decoder) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand()

    postCreateCommand(command, encoder, uri).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        inside(output) {
          case JsObject(fields) =>
            inside(fields.get("result")) {
              case Some(activeContract: JsObject) =>
                assertActiveContract(decoder, activeContract, command)
            }
        }
    }: Future[Assertion]
  }

  "command/create IOU should fail if authorization header is missing" in withHttpService(
    dar,
    testId) { (uri, encoder, _) =>
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

  "command/create IOU with unsupported templateId should return proper error" in withHttpService(
    dar,
    testId) { (uri, encoder, _) =>
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
          s"Cannot resolve ${unknownTemplateId: domain.TemplateId.NoPkg}")
    }: Future[Assertion]
  }

  "command/exercise IOU_Transfer" in withHttpService(dar, testId) { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap {
        case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(createOutput)
          val exercise: domain.ExerciseCommand[v.Record] = iouExerciseTransferCommand(contractId)
          val exerciseJson: JsObject = encoder.encodeR(exercise).valueOr(e => fail(e.shows))

          postJsonRequest(uri.withPath(Uri.Path("/command/exercise")), exerciseJson)
            .flatMap {
              case (exerciseStatus, exerciseOutput) =>
                exerciseStatus shouldBe StatusCodes.OK
                assertStatus(exerciseOutput, StatusCodes.OK)
                println(s"----- exerciseOutput: $exerciseOutput")
                inside(exerciseOutput) {
                  case JsObject(fields) =>
                    inside(fields.get("result")) {
                      case Some(JsArray(Vector(activeContract: JsObject))) =>
                        assertActiveContract(decoder, activeContract, create, exercise)
                    }
                }
            }
      }: Future[Assertion]
  }

  "command/exercise IOU_Transfer with unknown contractId should return proper error" in withHttpService(
    dar,
    testId) { (uri, encoder, _) =>
    val contractId = lar.ContractId("NonExistentContractId")
    val exercise: domain.ExerciseCommand[v.Record] = iouExerciseTransferCommand(contractId)
    val exerciseJson: JsObject = encoder.encodeR(exercise).valueOr(e => fail(e.shows))

    postJsonRequest(uri.withPath(Uri.Path("/command/exercise")), exerciseJson)
      .flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.InternalServerError
          assertStatus(output, StatusCodes.InternalServerError)
          expectedOneErrorMessage(output) should include(
            "couldn't find contract AbsoluteContractId(NonExistentContractId)")
      }: Future[Assertion]
  }

  private def assertActiveContract(
      decoder: DomainJsonDecoder,
      jsObject: JsObject,
      create: domain.CreateCommand[v.Record],
      exercise: domain.ExerciseCommand[v.Record]): Assertion = {

    // TODO(Leo): check the jsObject.argument is the same as createCommand.argument
    println(s"------- jsObject: $jsObject")
    println(s"------- create: $create")
    println(s"------- exercise: $exercise")

    inside(jsObject.fields.get("argument")) {
      case Some(JsObject(fields)) =>
        fields.size shouldBe (exercise.argument.fields.size + 1) // +1 for the original "iou" from create
    }
  }

  private def assertActiveContract(
      decoder: DomainJsonDecoder,
      jsObject: JsObject,
      command: domain.CreateCommand[v.Record]): Assertion = {

    inside(decoder.decodeV[domain.ActiveContract](jsObject)) {
      case \/-(activeContract) =>
        inside(activeContract.argument.sum.record) {
          case Some(argument) => removeRecordId(argument) shouldBe command.argument
        }
    }
  }

  "should be able to serialize and deserialize domain commands" in withLedger(dar, testId) {
    client =>
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

    val command0: domain.ExerciseCommand[v.Record] = iouExerciseTransferCommand(
      lar.ContractId("a-contract-ID"))

    val x = for {
      jsonObj <- encoder.encodeR(command0)
      command1 <- decoder.decodeR[domain.ExerciseCommand](jsonObj)
    } yield command1.map(removeRecordId) should ===(command0)

    x.fold(e => fail(e.shows), identity)
  }

  "request non-existent endpoint should return 404 with errors" in withHttpService(dar, testId) {
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

  private def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
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
      contractId: lar.ContractId): domain.ExerciseCommand[v.Record] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val arg: Record = v.Record(
      fields = List(v.RecordField("newOwner", Some(v.Value(v.Value.Sum.Party("Alice")))))
    )
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(templateId, contractId, choice, arg, None)
  }

  private def postJsonRequest(
      uri: Uri,
      json: JsValue,
      headers: List[HttpHeader] = headersWithAuth): Future[(StatusCode, JsValue)] = {
    logger.info(s"postJson: $uri json: $json")
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, json.prettyPrint))
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => (resp.status, body.parseJson))
      }
  }

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

  private def getContractId(output: JsValue): lar.ContractId =
    inside(output) {
      case JsObject(topFields) =>
        inside(topFields.get("result")) {
          case Some(JsObject(fields)) =>
            inside(fields.get("contractId")) {
              case Some(JsString(contractId)) => lar.ContractId(contractId)
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

  private def activeContractList[A](
      response: List[domain.GetActiveContractsResponse[A]]): List[domain.ActiveContract[A]] =
    response.flatMap(_.activeContracts)
}
