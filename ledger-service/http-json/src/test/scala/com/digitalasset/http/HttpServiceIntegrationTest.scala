// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.http.util.TestUtil.requiredFile
import org.scalatest.{AsyncFreeSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.ExecutionContext

class HttpServiceIntegrationTest extends AsyncFreeSpec with Matchers with BeforeAndAfterAll {

  private val dar = requiredFile("./docs/quickstart-model.dar")
    .fold(e => throw new IllegalStateException(e), identity)

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  "test" in {
    HttpServiceTestFixture
      .withHttpService(dar, testId) { uri: Uri =>
        Http().singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/contracts/search"))))
      }
      .flatMap { httpResponse: HttpResponse =>
        httpResponse.status shouldBe StatusCodes.OK
        httpResponse.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a)
      }
      .map { byteString =>
        println(s"---- ${byteString.utf8String}")
        byteString.utf8String.length should be >= 0
      }
  }
}
