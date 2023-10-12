// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import cats.implicits.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import io.grpc.stub.AbstractStub
import io.grpc.{CallOptions, Channel, ManagedChannel}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.*

class GrpcCtlRunnerTest extends AsyncWordSpec with BaseTest {

  "Runner" when {
    "running a successful command" should {
      val (channel, command) = defaultMocks()

      "run successfully" in {
        new GrpcCtlRunner(1000, 1000, loggerFactory).run(
          "participant1",
          command,
          channel,
          None,
          1000.milliseconds,
        ) map { result =>
          result shouldBe "result"
        }
      }
    }
  }

  class TestAbstractStub(channel: Channel) extends AbstractStub[TestAbstractStub](channel) {
    override def build(channel: Channel, callOptions: CallOptions): TestAbstractStub = this
  }

  private def defaultMocks(): (ManagedChannel, GrpcAdminCommand[String, String, String]) = {
    val channel = mock[ManagedChannel]
    val service = new TestAbstractStub(channel)
    val command = new GrpcAdminCommand[String, String, String] {
      override type Svc = TestAbstractStub
      override def createService(channel: ManagedChannel): Svc = service
      override def createRequest(): Either[String, String] = Right("request")
      override def submitRequest(service: Svc, request: String): Future[String] =
        if (service == service && request == "request") Future.successful("response")
        else Future.failed(new Exception("Invalid"))
      override def handleResponse(response: String): Either[String, String] = Right("result")
    }

    (channel, command)
  }
}
