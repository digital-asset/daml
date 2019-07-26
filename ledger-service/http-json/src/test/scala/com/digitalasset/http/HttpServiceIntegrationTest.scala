// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.HttpServiceTestFixture.{jsonCodecs, withHttpService, withLedger}
import com.digitalasset.http.domain.TemplateId.OptionalPkg
import com.digitalasset.http.json._
import com.digitalasset.http.util.TestUtil.requiredFile
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.value.Record
import com.digitalasset.ledger.api.v1.{value => v}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import scalaz.\/-
import scalaz.syntax.functor._
import scalaz.syntax.show._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

class HttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with BeforeAndAfterAll
    with StrictLogging {

  import json.JsonProtocol._

  private val dar = requiredFile("./docs/quickstart-model.dar")
    .fold(e => throw new IllegalStateException(e), identity)

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  "contracts/search test" in withHttpService(dar, testId) { (uri: Uri, _, _) =>
    Http().singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/search")))).flatMap {
      resp =>
        resp.status shouldBe StatusCodes.OK
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.flatMap { body =>
          val jsonAst: JsValue = body.parseJson
          inside(jsonAst) {
            case JsObject(fields) =>
              inside(fields.get("status")) {
                case Some(JsNumber(status)) => status shouldBe BigDecimal("200")
              }
              inside(fields.get("result")) {
                case Some(JsArray(result)) => result.length should be > 0
              }
          }
        }
    }: Future[Assertion]
  }

  "command/create IOU" in withHttpService(dar, testId) { (uri, encoder, decoder) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand
    val input: JsObject = encoder.encodeR(command).valueOr(e => fail(e.shows))

    postJson(uri.withPath(Uri.Path("/command/create")), input).flatMap {
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

  "command/exercise IOU_Transfer" in withHttpService(dar, testId) { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand
    val createJson: JsObject = encoder.encodeR(create).valueOr(e => fail(e.shows))

    postJson(uri.withPath(Uri.Path("/command/create")), createJson).flatMap {
      case (createStatus, createOutput) =>
        createStatus shouldBe StatusCodes.OK
        assertStatus(createOutput, StatusCodes.OK)

        val contractId = getContractId(createOutput)
        val exercise: domain.ExerciseCommand[v.Record] = iouExerciseTransferCommand(contractId)
        val exerciseJson: JsObject = encoder.encodeR(exercise).valueOr(e => fail(e.shows))

        postJson(uri.withPath(Uri.Path("/command/exercise")), exerciseJson).flatMap {
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

    val command0: domain.CreateCommand[v.Record] = iouCreateCommand

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

  "request non-existent endpoint should return 404 with no data" in withHttpService(dar, testId) {
    (uri: Uri, _, _) =>
      Http()
        .singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/does-not-exist"))))
        .flatMap { resp =>
          resp.status shouldBe StatusCodes.NotFound
          val bodyF: Future[String] = getResponseDataBytes(resp)
          bodyF.flatMap { body =>
            body should have length 0
          }
        }: Future[Assertion]
  }

  private def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

  private def removeRecordId(a: v.Record): v.Record = a.copy(recordId = None)

  private def iouCreateCommand: domain.CreateCommand[v.Record] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val arg: Record = v.Record(
      fields = List(
        v.RecordField("issuer", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("currency", Some(v.Value(v.Value.Sum.Text("USD")))),
        v.RecordField("amount", Some(v.Value(v.Value.Sum.Decimal("999.99")))),
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

  private def postJson(uri: Uri, json: JsValue): Future[(StatusCode, JsValue)] = {
    logger.info(s"postJson: $uri json: $json")
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          entity = HttpEntity(ContentTypes.`application/json`, json.prettyPrint))
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
          case Some(JsNumber(status)) => status shouldBe BigDecimal("200")
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
}
