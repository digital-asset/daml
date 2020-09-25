// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Location, `Set-Cookie`}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import org.scalatest.AsyncWordSpec

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  "the middleware" should {
    "redirect and set cookie after login" in {
      lazy val middlewareBinding = suiteResource.value._2.localAddress
      lazy val middlewareUri =
        Uri()
          .withScheme("http")
          .withAuthority(middlewareBinding.getHostString, middlewareBinding.getPort)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("login"))
          .withQuery(Query(("redirect_uri", "http://CALLBACK"), ("claims", "actAs:Alice"))))
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
        assert(resp.header[`Set-Cookie`].get.cookie.name == "token")
      }
    }
  }
}
