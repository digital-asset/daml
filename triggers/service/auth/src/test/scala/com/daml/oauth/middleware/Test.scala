// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Cookie, Location, `Set-Cookie`}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref.Party
import com.daml.oauth.server.{Response => OAuthResponse}
import org.scalatest.AsyncWordSpec

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  import JsonProtocol._
  lazy private val middlewareUri = {
    lazy val middlewareBinding = suiteResource.value._2.localAddress
    Uri()
      .withScheme("http")
      .withAuthority(middlewareBinding.getHostString, middlewareBinding.getPort)
  }
  private def makeToken(claims: Request.Claims): OAuthResponse.Token = {
    val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
    val jwtPayload = AuthServiceJWTPayload(
      ledgerId = Some("test-ledger"),
      applicationId = Some("test-application"),
      participantId = None,
      exp = None,
      admin = claims.admin,
      actAs = claims.actAs,
      readAs = claims.readAs
    )
    OAuthResponse.Token(
      accessToken = JwtSigner.HMAC256
        .sign(DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(jwtPayload)), "secret")
        .getOrElse(
          throw new IllegalArgumentException("Failed to sign a token")
        )
        .value,
      tokenType = "bearer",
      expiresIn = None,
      refreshToken = None,
      scope = Some(claims.toQueryString())
    )
  }
  "the /auth endpoint" should {
    "return unauthorized without cookie" in {
      val claims = "actAs:Alice"
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(Query(("claims", claims))))
      for {
        resp <- Http().singleRequest(req)
      } yield {
        assert(resp.status == StatusCodes.Unauthorized)
      }
    }
    "return the token from a cookie" in {
      val claims = Request.Claims(actAs = List(Party.assertFromString("Alice")))
      val token = makeToken(claims)
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(Query(("claims", claims.toQueryString()))),
        headers = List(cookieHeader))
      for {
        resp <- Http().singleRequest(req)
        auth <- {
          assert(resp.status == StatusCodes.OK)
          Unmarshal(resp).to[Response.Authorize]
        }
      } yield {
        assert(auth.accessToken == token.accessToken)
        assert(auth.refreshToken == token.refreshToken)
      }
    }
    "return unauthorized on insufficient claims" in {
      val token = makeToken(Request.Claims(actAs = List(Party.assertFromString("Alice"))))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(Query(
            ("claims", Request.Claims(actAs = List(Party.assertFromString("Bob"))).toQueryString))),
        headers = List(cookieHeader)
      )
      for {
        resp <- Http().singleRequest(req)
      } yield {
        assert(resp.status == StatusCodes.Unauthorized)
      }
    }
  }
  "the /login endpoint" should {
    "redirect and set cookie" in {
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
        val token = OAuthResponse.Token.fromCookieValue(cookie.value).get
        assert(token.tokenType == "bearer")
      }
    }
  }
}
