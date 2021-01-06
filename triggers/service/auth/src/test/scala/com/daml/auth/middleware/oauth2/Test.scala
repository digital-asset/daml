// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.time.Duration

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Cookie, Location, `Set-Cookie`}
import com.daml.auth.middleware.api.{Client, Request, RequestStore}
import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.auth.oauth2.api.{Response => OAuthResponse}
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.{Failure, Success}
import scala.concurrent.duration._

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  private def makeToken(
      claims: Request.Claims,
      secret: String = "secret",
      expiresIn: Option[Duration] = None): OAuthResponse.Token = {
    val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
    val jwtPayload = AuthServiceJWTPayload(
      ledgerId = Some("test-ledger"),
      applicationId = Some("test-application"),
      participantId = None,
      exp = expiresIn.map(in => clock.instant.plus(in)),
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
      val claims = Request.Claims(actAs = List(Party("Alice")))
      for {
        result <- middlewareClient.requestAuth(claims, Nil)
      } yield {
        assert(result == None)
      }
    }
    "return the token from a cookie" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val token = makeToken(claims)
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
        auth = result.get
      } yield {
        assert(auth.accessToken == token.accessToken)
        assert(auth.refreshToken == token.refreshToken)
      }
    }
    "return unauthorized on insufficient party claims" in {
      val claims = Request.Claims(actAs = List(Party("Bob")))
      val token = makeToken(Request.Claims(actAs = List(Party("Alice"))))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        assert(result == None)
      }
    }
    "return unauthorized on insufficient app id claims" in {
      val claims = Request.Claims(
        actAs = List(ApiTypes.Party("Alice")),
        applicationId = Some(ApiTypes.ApplicationId("other-id")))
      val token = makeToken(
        Request.Claims(
          actAs = List(ApiTypes.Party("Alice")),
          applicationId = Some(ApiTypes.ApplicationId("my-app-id"))))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        assert(result == None)
      }
    }
    "return unauthorized on an invalid token" in {
      val claims = Request.Claims(actAs = List(ApiTypes.Party("Alice")))
      val token = makeToken(claims, "wrong-secret")
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        assert(result == None)
      }
    }
    "return unauthorized on an expired token" in {
      val claims = Request.Claims(actAs = List(ApiTypes.Party("Alice")))
      val token = makeToken(claims, expiresIn = Some(Duration.ZERO))
      val _ = clock.fastForward(Duration.ofSeconds(1))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        assert(result == None)
      }
    }
  }
  "the /login endpoint" should {
    "redirect and set cookie" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val req = HttpRequest(uri = middlewareClient.loginUri(claims, None))
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
        // Redirect to client callback
        assert(resp.status == StatusCodes.Found)
        assert(resp.header[Location].get.uri == middlewareClientCallbackUri)
        // Store token in cookie
        val cookie = resp.header[`Set-Cookie`].get.cookie
        assert(cookie.name == "daml-ledger-token")
        val token = OAuthResponse.Token.fromCookieValue(cookie.value).get
        assert(token.tokenType == "bearer")
      }
    }
    "not authorize unauthorized parties" in {
      server.revokeParty(Party("Eve"))
      val claims = Request.Claims(actAs = List(Party("Eve")))
      val req = HttpRequest(uri = middlewareClient.loginUri(claims, None))
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
        // Redirect to client callback
        assert(resp.status == StatusCodes.Found)
        assert(resp.header[Location].get.uri.withQuery(Uri.Query()) == middlewareClientCallbackUri)
        // with error parameter set
        assert(resp.header[Location].get.uri.query().toMap.get("error") == Some("access_denied"))
        // Without token in cookie
        val cookie = resp.header[`Set-Cookie`]
        assert(cookie == None)
      }
    }
    "not authorize disallowed admin claims" in {
      server.revokeAdmin()
      val claims = Request.Claims(admin = true)
      val req = HttpRequest(uri = middlewareClient.loginUri(claims, None))
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
        // Redirect to client callback
        assert(resp.status == StatusCodes.Found)
        assert(resp.header[Location].get.uri.withQuery(Uri.Query()) == middlewareClientCallbackUri)
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
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val loginReq = HttpRequest(uri = middlewareClient.loginUri(claims, None))
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
          (AccessToken(token.accessToken), RefreshToken(token.refreshToken.get))
        }
        // Advance time
        _ = clock.fastForward(Duration.ofSeconds(1))
        // Request /refresh
        authorize <- middlewareClient.requestRefresh(refreshToken)
      } yield {
        // Test that we got a new access token
        assert(authorize.accessToken != token1)
        // Test that we got a new refresh token
        assert(authorize.refreshToken.get != refreshToken)
      }
    }
    "fail on an invalid refresh token" in {
      for {
        exception <- middlewareClient.requestRefresh(RefreshToken("made-up-token")).transform {
          case Failure(exception: Client.RefreshException) => Success(exception)
          case value => fail(s"Expected failure with RefreshException but got $value")
        }
      } yield {
        assert(exception.status.isInstanceOf[StatusCodes.ClientError])
      }
    }
  }
}

class TestCallbackUriOverride
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val middlewareCallbackUri = Some(Uri("http://localhost/MIDDLEWARE_CALLBACK"))
  "the /login endpoint" should {
    "redirect to the configured middleware callback URI" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val req = HttpRequest(uri = middlewareClient.loginUri(claims, None))
      for {
        resp <- Http().singleRequest(req)
        // Redirect to /authorize on authorization server
        resp <- {
          assert(resp.status == StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].get.uri)
          Http().singleRequest(req)
        }
      } yield {
        // Redirect to configured callback URI on middleware
        assert(resp.status == StatusCodes.Found)
        assert(resp.header[Location].get.uri.withQuery(Uri.Query()) == middlewareCallbackUri.get)
      }
    }
  }
}

class TestLimitedMiddlewareCallbackStore
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val maxMiddlewareLogins = 2
  "the /login endpoint" should {
    "refuse requests when max capacity is reached" in {
      def login(actAs: Party) = {
        val claims = Request.Claims(actAs = List(actAs))
        val uri = middlewareClient.loginUri(claims, None)
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      def followRedirect(resp: HttpResponse) = {
        assert(resp.status == StatusCodes.Found)
        val uri = resp.header[Location].get.uri
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      for {
        // Follow login flows up to redirect to middleware callback.
        redirectAlice <- login(Party("Alice"))
          .flatMap(followRedirect)
        redirectBob <- login(Party("Bob"))
          .flatMap(followRedirect)
        // The store should be full
        refusedCarol <- login(Party("Carol"))
        _ = assert(refusedCarol.status == StatusCodes.ServiceUnavailable)
        // Follow first redirect to middleware callback.
        resultAlice <- followRedirect(redirectAlice)
        _ = assert(resultAlice.status == StatusCodes.Found)
        // The store should have space again
        redirectCarol <- login(Party("Carol"))
          .flatMap(followRedirect)
        // Follow redirects to middleware callback.
        resultBob <- followRedirect(redirectBob)
        resultCarol <- followRedirect(redirectCarol)
      } yield {
        assert(resultBob.status == StatusCodes.Found)
        assert(resultCarol.status == StatusCodes.Found)
      }
    }
  }
}

class TestLimitedClientCallbackStore
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val maxClientAuthCallbacks = 2
  "the /login client" should {
    "refuse requests when max capacity is reached" in {
      def login(actAs: Party) = {
        val claims = Request.Claims(actAs = List(actAs))
        val host = middlewareClientBinding.localAddress
        val uri = Uri()
          .withScheme("http")
          .withAuthority(host.getHostName, host.getPort)
          .withPath(Uri.Path./("login"))
          .withQuery(Uri.Query("claims" -> claims.toQueryString))
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      def followRedirect(resp: HttpResponse) = {
        assert(resp.status == StatusCodes.Found)
        val uri = resp.header[Location].get.uri
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      for {
        // Follow login flows up to last redirect to middleware client.
        redirectAlice <- login(Party("Alice"))
          .flatMap(followRedirect)
          .flatMap(followRedirect)
          .flatMap(followRedirect)
        redirectBob <- login(Party("Bob"))
          .flatMap(followRedirect)
          .flatMap(followRedirect)
          .flatMap(followRedirect)
        // The store should be full
        refusedCarol <- login(Party("Carol"))
        _ = assert(refusedCarol.status == StatusCodes.ServiceUnavailable)
        // Follow first redirect to middleware client.
        resultAlice <- followRedirect(redirectAlice)
        _ = assert(resultAlice.status == StatusCodes.OK)
        // The store should have space again
        redirectCarol <- login(Party("Carol"))
          .flatMap(followRedirect)
          .flatMap(followRedirect)
          .flatMap(followRedirect)
        resultBob <- followRedirect(redirectBob)
        resultCarol <- followRedirect(redirectCarol)
      } yield {
        assert(resultBob.status == StatusCodes.OK)
        assert(resultCarol.status == StatusCodes.OK)
      }
    }
  }
}

class TestRequestStore extends AsyncWordSpec {
  "return None on missing element" in {
    val store = new RequestStore[Int, String](1, 1.day)
    assert(store.pop(0) == None)
  }

  "return previously put element" in {
    val store = new RequestStore[Int, String](1, 1.day)
    store.put(0, "zero")
    assert(store.pop(0) == Some("zero"))
  }

  "return None on previously popped element" in {
    val store = new RequestStore[Int, String](1, 1.day)
    store.put(0, "zero")
    store.pop(0)
    assert(store.pop(0) == None)
  }

  "store multiple elements" in {
    val store = new RequestStore[Int, String](3, 1.day)
    store.put(0, "zero")
    store.put(1, "one")
    store.put(2, "two")
    assert(store.pop(0) == Some("zero"))
    assert(store.pop(1) == Some("one"))
    assert(store.pop(2) == Some("two"))
  }

  "store no more than max capacity" in {
    val store = new RequestStore[Int, String](2, 1.day)
    assert(store.put(0, "zero"))
    assert(store.put(1, "one"))
    assert(!store.put(2, "two"))
    assert(store.pop(0) == Some("zero"))
    assert(store.pop(1) == Some("one"))
    assert(store.pop(2) == None)
  }

  "return None on timed out element" in {
    var time: Long = 0
    val store = new RequestStore[Int, String](1, 1.day, () => time)
    store.put(0, "zero")
    time += 1.day.toNanos
    assert(store.pop(0) == None)
  }

  "free capacity for timed out elements" in {
    var time: Long = 0
    val store = new RequestStore[Int, String](1, 1.day, () => time)
    assert(store.put(0, "zero"))
    assert(!store.put(1, "one"))
    time += 1.day.toNanos
    assert(store.put(2, "two"))
    assert(store.pop(2) == Some("two"))
  }
}
