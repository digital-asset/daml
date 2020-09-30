// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Location, `Set-Cookie`}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.oauth.server.{Response => OAuthResponse}
import java.util.Base64
import org.scalatest.AsyncWordSpec
import spray.json._

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  import com.daml.oauth.server.JsonProtocol._
  "the middleware" should {
    "redirect and set cookie after login" in {
      lazy val middlewareBinding = suiteResource.value._2.localAddress
      lazy val middlewareUri =
        Uri()
          .withScheme("http")
          .withAuthority(middlewareBinding.getHostString, middlewareBinding.getPort)
      val claims = "actAs:Alice"
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("login"))
          .withQuery(Query(("redirect_uri", "http://CALLBACK"), ("claims", claims))))
      for {
        resp <- Http().singleRequest(req)
        // Redirect to /authorize on authorization server
        resp <- {
          assert(resp.status == StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].get.uri)
          Http().singleRequest(req)
        }
        // Redirect to /cb on middleware
        resp <- {
          assert(resp.status == StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].get.uri)
          Http().singleRequest(req)
        }
      } yield {
        // Redirect to CALLBACK
        assert(resp.status == StatusCodes.Found)
        assert(resp.header[Location].get.uri == Uri("http://CALLBACK"))
        // Store token in cookie
        val cookie = resp.header[`Set-Cookie`].get.cookie
        assert(cookie.name == "daml-ledger-token")
        val decoder = Base64.getUrlDecoder()
        val token = new String(decoder.decode(cookie.value)).parseJson.convertTo[OAuthResponse.Token]
        assert(token.tokenType == "bearer")
      }
    }
  }
}
