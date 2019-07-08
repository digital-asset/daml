// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.HttpServiceTestFixture.withHttpService
import com.digitalasset.http.util.TestUtil.requiredFile
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
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
