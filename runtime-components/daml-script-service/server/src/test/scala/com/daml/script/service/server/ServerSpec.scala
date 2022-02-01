// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.service.server

import java.net.InetSocketAddress

import akka.http.scaladsl.Http
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.engine.script.test.SandboxParticipantFixture
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Authority
import akka.http.scaladsl.model._
import org.scalatest.Assertion
import spray.json.{JsString, JsNumber, JsValue}

import scala.concurrent.Future

final class ServerSpec
    extends AsyncFlatSpec
    with Matchers
    with SandboxParticipantFixture
    with SuiteResourceManagementAroundAll {

  behavior of "Server"

  // this test is taking advantage of implicit party allocation
  // TODO absolutely do not rely on implicit party allocation
  // TODO always treat party identifiers as opaque references
  it should "serve" in {
    Server.owner(serverPort).map(toService).use { service =>
      val endpoint = service(HttpMethods.POST, Uri.Path / "script" / "ScriptTest:jsonBasic")
      endpoint(JsString("Alice")) { response =>
        response shouldBe JsNumber(42)
      }
    }
  }

  override protected val timeMode: ScriptTimeMode = ScriptTimeMode.WallClock

  def toService(
      address: InetSocketAddress
  )(method: HttpMethod, path: Uri.Path)(
      input: JsValue
  )(test: JsValue => Assertion): Future[Assertion] = {
    val authority = Authority(Uri.Host(address.getHostName), address.getPort)
    val uri = Uri("http", authority, path)
    for {
      entity <- Marshal(input).to[MessageEntity]
      request = HttpRequest(method, uri, entity = entity)
      response <- Http().singleRequest(request)
      json <- Unmarshal(response).to[JsValue]
    } yield test(json)
  }

}
