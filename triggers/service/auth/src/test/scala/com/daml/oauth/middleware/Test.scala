// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import java.time.Duration

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Cookie, Location, `Set-Cookie`}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.oauth.server.{Response => OAuthResponse}
import org.scalatest.AsyncWordSpec

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  import JsonProtocol._
  lazy private val middlewareUri = {
    lazy val middlewareBinding = suiteResource.value._3.localAddress
    Uri()
      .withScheme("http")
      .withAuthority(middlewareBinding.getHostString, middlewareBinding.getPort)
  }
  private def makeToken(
      claims: Request.Claims,
      secret: String = "secret",
      expiresIn: Option[Duration] = None): OAuthResponse.Token = {
    val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
    val jwtPayload = AuthServiceJWTPayload(
      ledgerId = Some("test-ledger"),
      applicationId = Some("test-application"),
      participantId = None,
      exp = expiresIn.map(in => suiteResource.value._1.instant.plus(in)),
      admin = claims.admin,
      actAs = claims.actAs.map(ApiTypes.Party.unwrap(_)),
      readAs = claims.readAs.map(ApiTypes.Party.unwrap(_))
    )
    OAuthResponse.Token(
      accessToken = JwtSigner.HMAC256
        .sign(DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(jwtPayload)), secret)
        .getOrElse(
          throw new IllegalArgumentException("Failed to sign a token")
        )
        .value,
      tokenType = "bearer",
      expiresIn = expiresIn.map(in => in.getSeconds.toInt),
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
      val claims = Request.Claims(actAs = List(ApiTypes.Party("Alice")))
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
    "return unauthorized on insufficient party claims" in {
      val token = makeToken(Request.Claims(actAs = List(ApiTypes.Party("Alice"))))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(
            Query(("claims", Request.Claims(actAs = List(ApiTypes.Party("Bob"))).toQueryString))),
        headers = List(cookieHeader)
      )
      for {
        resp <- Http().singleRequest(req)
      } yield {
        assert(resp.status == StatusCodes.Unauthorized)
      }
    }
    "return unauthorized on insufficient app id claims" in {
      val token = makeToken(
        Request.Claims(
          actAs = List(ApiTypes.Party("Alice")),
          applicationId = Some(ApiTypes.ApplicationId("my-app-id"))))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(
            Query(
              (
                "claims",
                Request
                  .Claims(
                    actAs = List(ApiTypes.Party("Alice")),
                    applicationId = Some(ApiTypes.ApplicationId("other-id")))
                  .toQueryString))),
        headers = List(cookieHeader)
      )
      for {
        resp <- Http().singleRequest(req)
      } yield {
        assert(resp.status == StatusCodes.Unauthorized)
      }
    }
    "return unauthorized on an invalid token" in {
      val claims = Request.Claims(actAs = List(ApiTypes.Party("Alice")))
      val token = makeToken(claims, "wrong-secret")
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(Query(("claims", claims.toQueryString))),
        headers = List(cookieHeader)
      )
      for {
        resp <- Http().singleRequest(req)
      } yield {
        assert(resp.status == StatusCodes.Unauthorized)
      }
    }
    "return unauthorized on an expired token" in {
      val claims = Request.Claims(actAs = List(ApiTypes.Party("Alice")))
      val token = makeToken(claims, expiresIn = Some(Duration.ZERO))
      val _ = suiteResource.value._1.fastForward(Duration.ofSeconds(1))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      val req = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("auth"))
          .withQuery(Query(("claims", claims.toQueryString))),
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
    "not authorize unauthorized parties" in {
      val claims = "actAs:Eve"
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
        assert(resp.header[Location].get.uri.withQuery(Uri.Query()) == Uri("http://CALLBACK"))
        // with error parameter set
        assert(resp.header[Location].get.uri.query().toMap.get("error") == Some("access_denied"))
        // Without token in cookie
        val cookie = resp.header[`Set-Cookie`]
        assert(cookie == None)
      }
    }
  }
  "the /refresh endpoint" should {
    "return a new access token" in {
      val claims = "actAs:Alice"
      val loginReq = HttpRequest(
        uri = middlewareUri
          .withPath(Path./("login"))
          .withQuery(Query(("redirect_uri", "http://CALLBACK"), ("claims", claims))))
      for {
        resp <- Http().singleRequest(loginReq)
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
        // Extract token from cookie
        (token1, refreshToken) = {
          val cookie = resp.header[`Set-Cookie`].get.cookie
          val token = OAuthResponse.Token.fromCookieValue(cookie.value).get
          (token.accessToken, token.refreshToken.get)
        }
        // Advance time
        _ = suiteResource.value._1.fastForward(Duration.ofSeconds(1))
        // Request /refresh
        refreshEntity <- Marshal(Request.Refresh(refreshToken)).to[RequestEntity]
        refreshReq = HttpRequest(
          method = HttpMethods.POST,
          uri = middlewareUri.withPath(Path./("refresh")),
          entity = refreshEntity,
        )
        resp <- Http().singleRequest(refreshReq)
        authorize <- {
          assert(resp.status == StatusCodes.OK)
          Unmarshal(resp.entity).to[Response.Authorize]
        }
      } yield {
        // Test that we got a new access token
        assert(authorize.accessToken != token1)
        // Test that we got a new refresh token
        assert(authorize.refreshToken.get != refreshToken)
      }
    }
    "fail on an invalid refresh token" in {
      for {
        refreshEntity <- Marshal(Request.Refresh("made-up-token")).to[RequestEntity]
        refreshReq = HttpRequest(
          method = HttpMethods.POST,
          uri = middlewareUri.withPath(Path./("refresh")),
          entity = refreshEntity,
        )
        resp <- Http().singleRequest(refreshReq)
      } yield {
        assert(resp.status.isInstanceOf[StatusCodes.ClientError])
      }
    }
  }
}
