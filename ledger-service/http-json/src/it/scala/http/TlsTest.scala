// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServiceTestFixture.UseTls
import akka.http.scaladsl.model.{StatusCodes, Uri}
import org.scalatest.{Assertion, Inside}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsArray, JsObject}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class TlsTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with AbstractHttpServiceIntegrationTestFuns {

  override def jdbcConfig = None

  override def staticContentConfig = None

  override def useTls = UseTls.Tls

  override def wsConfig: Option[WebsocketConfig] = None

  "connect normally with tls on" in withHttpService { (uri: Uri, _, _, _) =>
    getRequest(uri = uri.withPath(Uri.Path("/v1/query")))
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        inside(output) { case JsObject(fields) =>
          inside(fields.get("result")) { case Some(JsArray(vector)) =>
            vector should have size 0L
          }
        }
      }: Future[Assertion]
  }
}
