// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http
import com.digitalasset.canton.http.json.JsonProtocol.*
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.UseTls
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import org.scalatest.Assertion
import spray.json.JsValue

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class TlsTest
    extends AbstractHttpServiceIntegrationTestFuns
    with AbstractHttpServiceIntegrationTestFunsUserToken {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "HTTP JSON API Service")

  override def useTls = UseTls.Tls

  "JSON API" should {
    "connect normally with tls on" taggedAs authenticationSecurity.setHappyCase(
      "A client request returns OK with enabled TLS"
    ) in withHttpService() { fixture =>
      fixture
        .getRequestWithMinimumAuth[Vector[JsValue]](Uri.Path("/v1/query"))
        .map(inside(_) { case http.OkResponse(vector, None, StatusCodes.OK) =>
          vector should have size 0L
        }): Future[Assertion]
    }
  }
}
