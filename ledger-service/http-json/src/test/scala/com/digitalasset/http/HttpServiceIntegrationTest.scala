// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.util.TestUtil.requiredFile
import org.scalatest._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

class HttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with BeforeAndAfterAll {

  private val dar = requiredFile("./docs/quickstart-model.dar")
    .fold(e => throw new IllegalStateException(e), identity)

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  "contracts/search test" in {
    HttpServiceTestFixture.withHttpService(dar, testId)(contractsSearchTest)
  }

  private def contractsSearchTest(uri: Uri): Future[Assertion] =
    Http().singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/search")))).flatMap {
      resp =>
        resp.status shouldBe StatusCodes.OK
        val bodyF: Future[ByteString] =
          resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a)
        bodyF.foreach(x => println(s"---- body: ${x.utf8String}"))
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
    }
}
