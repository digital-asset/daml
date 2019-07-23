// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.HttpServiceTestFixture.{withHttpService, withLedger}
import com.digitalasset.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter,
  SprayJson
}
import com.digitalasset.http.util.FutureUtil.{stripLeft, toFuture}
import com.digitalasset.http.util.TestUtil.requiredFile
import com.digitalasset.http.util.{ApiValueToLfValueConverter, LedgerIds}
import com.digitalasset.ledger.api.v1.{value => v}
import com.digitalasset.ledger.service.LedgerReader
import com.typesafe.scalalogging.StrictLogging
//import com.digitalasset.ledger.service.LedgerReader.PackageStore
//import scalaz.{\/, \/-}
//import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import org.scalatest._
import scalaz.std.string._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

class HttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with BeforeAndAfterAll
    with StrictLogging {

  private val dar = requiredFile("./docs/quickstart-model.dar")
    .fold(e => throw new IllegalStateException(e), identity)

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  "contracts/search test" in withHttpService(dar, testId) { uri: Uri =>
    Http().singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/search")))).flatMap {
      resp =>
        resp.status shouldBe StatusCodes.OK
        val bodyF: Future[ByteString] = getResponseDataBytes(resp)
        bodyF.flatMap { body =>
          val jsonAst: JsValue = body.utf8String.parseJson
          inside(jsonAst) {
            case JsObject(fields) =>
              inside(fields.get("status")) {
                case Some(JsNumber(status)) => status shouldBe BigDecimal("200")
              }
              inside(fields.get("result")) {
                case Some(JsString(result)) => result.length should be > 0
              }
          }
        }
    }: Future[Assertion]
  }

  "Should be able to serialize and deserialize domain.CreateCommand" in withLedger(dar, testId) {
    client =>
      {
        import json.JsonProtocol._

        val templateId: domain.TemplateId.OptionalPkg = domain.TemplateId(None, "Iou", "Iou")

        val args: domain.Record[v.Value] = List(
          ("issuer", v.Value(v.Value.Sum.Party("Alice"))),
          ("owner", v.Value(v.Value.Sum.Party("Alice"))),
          ("currency", v.Value(v.Value.Sum.Text("USD"))),
          ("amount", v.Value(v.Value.Sum.Decimal("999.99"))),
          ("observers", v.Value(v.Value.Sum.List(v.List())))
        )

        val ledgerId: lar.LedgerId = LedgerIds.convertLedgerId(client.ledgerId)
        val command0: domain.CreateCommand[v.Value] =
          domain.CreateCommand(templateId, Some(args), None)

        println(s"------------------ command0: $command0")

        for {
          packageStore <- stripLeft(LedgerReader.createPackageStore(client.packageClient))

          templateIdMap = PackageService.getTemplateIdMap(packageStore)

          resolveTemplateId = PackageService.resolveTemplateId(templateIdMap) _

          lfTypeLookup = LedgerReader.damlLfTypeLookup(packageStore) _

          jsValueToApiValue = JsValueToApiValueConverter.jsValueToApiValue(lfTypeLookup) _

          apiValueToLfValue = ApiValueToLfValueConverter.apiValueToLfValue(ledgerId, packageStore)

          apiValueToJsValue = ApiValueToJsValueConverter.apiValueToJsValue(apiValueToLfValue) _

          decoder = new DomainJsonDecoder(resolveTemplateId, jsValueToApiValue)
          encoder = new DomainJsonEncoder(apiValueToJsValue)

          command1 <- toFuture(encoder.encodeValues(command0)): Future[
            domain.CreateCommand[JsValue]]

          _ = println(s"------------------ command1: $command1")

          jsValue = command1.toJson: JsValue

          _ = println(s"------------------ jsValue: ${jsValue.prettyPrint}")

          command2 <- toFuture(SprayJson.parse[domain.CreateCommand[JsValue]](jsValue)): Future[
            domain.CreateCommand[JsValue]]

          _ = println(s"------------------ command2: $command2")

          _ <- Future(command1 shouldBe command2)

          command3 <- toFuture(decoder.decodeValues(command2)): Future[
            domain.CreateCommand[v.Value]]

          _ = println(s"------------------ command3: $command3")

        } yield command3 shouldBe command0
      }: Future[Assertion]
  }

  "command/create IOU" ignore withHttpService(dar, testId) { uri: Uri =>
    val templateId: domain.TemplateId.OptionalPkg = domain.TemplateId(None, "Iou", "Iou")

    val args: domain.Record[v.Value] = List(
      ("issuer", v.Value(v.Value.Sum.Party("Alice"))),
      ("owner", v.Value(v.Value.Sum.Party("Alice"))),
      ("currency", v.Value(v.Value.Sum.Text("USD"))),
      ("amount", v.Value(v.Value.Sum.Decimal("999.99"))),
      ("observers", v.Value(v.Value.Sum.List(v.List())))
    )

    val command: domain.CreateCommand[v.Value] = domain.CreateCommand(templateId, Some(args), None)

    Http()
      .singleRequest(
        HttpRequest(method = HttpMethods.POST, uri = uri.withPath(Uri.Path("/command/create"))))
      .flatMap { resp =>
        resp.status shouldBe StatusCodes.OK
        val bodyF: Future[ByteString] = getResponseDataBytes(resp)
        bodyF.flatMap { body =>
          val jsonAst: JsValue = body.utf8String.parseJson
          inside(jsonAst) {
            case JsObject(fields) =>
              inside(fields.get("status")) {
                case Some(JsNumber(status)) => status shouldBe BigDecimal("200")
              }
              inside(fields.get("result")) {
                case Some(JsString(result)) => result.length should be > 0
              }
          }
        }
      }: Future[Assertion]
  }

  "request non-existent endpoint should return 404 with no data" in withHttpService(dar, testId) {
    uri: Uri =>
      Http()
        .singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/does-not-exist"))))
        .flatMap { resp =>
          resp.status shouldBe StatusCodes.NotFound
          val bodyF: Future[ByteString] = getResponseDataBytes(resp)
          bodyF.flatMap { body =>
            body.utf8String should have length 0
          }
        }: Future[Assertion]
  }

  private def getResponseDataBytes(
      resp: HttpResponse,
      debug: Boolean = false): Future[ByteString] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a)
    if (debug) fb.foreach(x => logger.info(s"---- response data: ${x.utf8String}"))
    fb
  }
}
