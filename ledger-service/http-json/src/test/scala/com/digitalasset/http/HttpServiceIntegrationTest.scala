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
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.TestUtil.requiredFile
import com.digitalasset.ledger.api.v1.value.Record
import com.digitalasset.ledger.api.v1.{value => v}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import scalaz.syntax.functor._
import spray.json._
import scalaz.syntax.show._

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

  "contracts/search test" in withHttpService(dar, testId) { (uri: Uri, _, _) =>
    Http().singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/search")))).flatMap {
      resp =>
        resp.status shouldBe StatusCodes.OK
        val bodyF: Future[String] = getResponseDataBytes(resp)
        bodyF.flatMap { body =>
          val jsonAst: JsValue = body.parseJson
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

  "should be able to serialize and deserialize domain.CreateCommand" in withLedger(dar, testId) {
    client =>
      {
        import json.JsonProtocol._

        val command0: domain.CreateCommand[v.Record] = iouCreateCommand

        for {
          (encoder, decoder) <- jsonCodecs(client)

          command1 <- toFuture(encoder.encodeUnderlyingRecord(command0)): Future[
            domain.CreateCommand[JsObject]]

          jsValue = command1.toJson: JsValue

          command2 <- toFuture(SprayJson.decode[domain.CreateCommand[JsObject]](jsValue)): Future[
            domain.CreateCommand[JsObject]]

          _ <- Future(command1 shouldBe command2)

          command3 <- toFuture(decoder.decodeUnderlyingRecords(command2)): Future[
            domain.CreateCommand[Record]]

          _ <- Future(command3.map(removeRecordId) shouldBe command0)

          jsObject <- toFuture(encoder.encodeR(command0))

          command4 <- toFuture(decoder.decodeR[domain.CreateCommand](jsObject)): Future[
            domain.CreateCommand[Record]]

          x <- Future(command4 shouldBe command3)

        } yield x

      }: Future[Assertion]
  }

  "command/create IOU" in withHttpService(dar, testId) { (uri, encoder, _) =>
    import json.JsonProtocol._

    val command: domain.CreateCommand[v.Record] = iouCreateCommand
    val input: String = encoder.encodeR(command).map(x => x.prettyPrint).valueOr(e => fail(e.shows))
    println(s"----- input: $input")

    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri.withPath(Uri.Path("/command/create")),
          entity = HttpEntity(ContentTypes.`application/json`, input)))
      .flatMap { resp =>
        resp.status shouldBe StatusCodes.OK
        val bodyF: Future[String] = getResponseDataBytes(resp, true)
        bodyF.flatMap { body =>
          println(s"----- body: $body")
          val jsonAst: JsValue = body.parseJson
          println(s"----- jsonAst: $jsonAst")
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
}
