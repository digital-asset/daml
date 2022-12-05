// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServiceTestFixture.UseTls
import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.SecurityTest
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest.{Assertion, Inside}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json.JsValue

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class TlsTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with AbstractHttpServiceIntegrationTestFuns {
  import json.JsonProtocol._

  val authenticationSecurity: SecurityTest = SecurityTest(property = Authenticity, asset = "HTTP JSON API Service")

  override def jdbcConfig = None

  override def staticContentConfig = None

  override def useTls = UseTls.Tls

  override def wsConfig: Option[WebsocketConfig] = None

  "connect normally with tls on" taggedAs authenticationSecurity.setHappyCase(
    "A client request returns OK with enabled TLS"
  ) in withHttpService { fixture =>
    fixture
      .getRequestWithMinimumAuth[Vector[JsValue]](Uri.Path("/v1/query"))
      .map(inside(_) { case domain.OkResponse(vector, None, StatusCodes.OK) =>
        vector should have size 0L
      }): Future[Assertion]
  }
}

final class TlsTestCustomToken
    extends TlsTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
