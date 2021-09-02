// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.time.Duration

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Cookie, Location, `Set-Cookie`}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.auth.middleware.api.{Client, Request, Response}
import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.auth.oauth2.api.{Response => OAuthResponse}
import org.scalatest.{OptionValues, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import scala.collection.immutable
import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.util.{Failure, Success}

class TestMiddleware extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  private def makeToken(
      claims: Request.Claims,
      secret: String = "secret",
      expiresIn: Option[Duration] = None,
  ): OAuthResponse.Token = {
    val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
    val jwtPayload = AuthServiceJWTPayload(
      ledgerId = Some("test-ledger"),
      applicationId = Some("test-application"),
      participantId = None,
      exp = expiresIn.map(in => clock.instant.plus(in)),
      admin = claims.admin,
      actAs = claims.actAs.map(ApiTypes.Party.unwrap(_)),
      readAs = claims.readAs.map(ApiTypes.Party.unwrap(_)),
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
      scope = Some(claims.toQueryString()),
    )
  }
  "the port file" should {
    "list the HTTP port" in {
      val bindingPort = middlewareBinding.localAddress.getPort.toString
      val filePort = {
        val source = Source.fromFile(middlewarePortFile)
        try {
          source.mkString.stripLineEnd
        } finally {
          source.close()
        }
      }
      assert(bindingPort == filePort)
    }
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
        applicationId = Some(ApiTypes.ApplicationId("other-id")),
      )
      val token = makeToken(
        Request.Claims(
          actAs = List(ApiTypes.Party("Alice")),
          applicationId = Some(ApiTypes.ApplicationId("my-app-id")),
        )
      )
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
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
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
    "return OK and set cookie without redirectUri" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None, redirect = false))
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
        // Return OK
        assert(resp.status == StatusCodes.OK)
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
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
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
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
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
      val loginReq = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
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

class TestMiddlewareCallbackUriOverride
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val middlewareCallbackUri = Some(Uri("http://localhost/MIDDLEWARE_CALLBACK"))
  "the /login endpoint" should {
    "redirect to the configured middleware callback URI" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
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

class TestMiddlewareLimitedCallbackStore
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val maxMiddlewareLogins = 2
  "the /login endpoint" should {
    "refuse requests when max capacity is reached" in {
      def login(actAs: Party) = {
        val claims = Request.Claims(actAs = List(actAs))
        val uri = middlewareClientRoutes.loginUri(claims, None)
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

class TestMiddlewareClientLimitedCallbackStore
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
          .withQuery(Uri.Query("claims" -> claims.toQueryString()))
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

class TestMiddlewareClientNoRedirectToLogin
    extends AsyncWordSpec
    with Matchers
    with OptionValues
    with TryValues
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.No
  "the authorize client" should {
    "not redirect to /login" in {
      import com.daml.auth.middleware.api.JsonProtocol.responseAuthenticateChallengeFormat
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val host = middlewareClientBinding.localAddress
      val uri = Uri()
        .withScheme("http")
        .withAuthority(host.getHostName, host.getPort)
        .withPath(Uri.Path./("authorize"))
        .withQuery(Uri.Query("claims" -> claims.toQueryString()))
      val req = HttpRequest(uri = uri)
      for {
        resp <- Http().singleRequest(req)
        // Unauthorized with WWW-Authenticate header
        _ = assert(resp.status == StatusCodes.Unauthorized)
        wwwAuthenticate = resp.header[headers.`WWW-Authenticate`].value
        challenge = wwwAuthenticate.challenges
          .find(_.scheme == Response.authenticateChallengeName)
          .value
        _ = challenge.params.keys should contain.allOf("auth", "login")
        authUri = challenge.params.get("auth").value
        loginUri = challenge.params.get("login").value
        headerChallenge = Response.AuthenticateChallenge(
          Request.Claims(challenge.realm),
          loginUri,
          authUri,
        )
        // The body should include the same challenge
        bodyChallenge <- Unmarshal(resp).to[Response.AuthenticateChallenge]
      } yield {
        assert(headerChallenge.auth == middlewareClient.authUri(claims))
        assert(
          headerChallenge.login.withQuery(Uri.Query.Empty) == middlewareClientRoutes
            .loginUri(claims)
            .withQuery(Uri.Query.Empty)
        )
        assert(headerChallenge.login.query().get("claims").value == claims.toQueryString())
        assert(bodyChallenge == headerChallenge)
      }
    }
  }
}

class TestMiddlewareClientYesRedirectToLogin
    extends AsyncWordSpec
    with OptionValues
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.Yes
  "the authorize client" should {
    "redirect to /login" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val host = middlewareClientBinding.localAddress
      val uri = Uri()
        .withScheme("http")
        .withAuthority(host.getHostName, host.getPort)
        .withPath(Uri.Path./("authorize"))
        .withQuery(Uri.Query("claims" -> claims.toQueryString()))
      val req = HttpRequest(uri = uri)
      for {
        resp <- Http().singleRequest(req)
      } yield {
        // Redirect to /login on middleware
        assert(resp.status == StatusCodes.Found)
        val loginUri = resp.header[headers.Location].value.uri
        assert(
          loginUri.withQuery(Uri.Query.Empty) == middlewareClientRoutes
            .loginUri(claims)
            .withQuery(Uri.Query.Empty)
        )
        assert(loginUri.query().get("claims").value == claims.toQueryString())
      }
    }
  }
}

class TestMiddlewareClientAutoRedirectToLogin
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll {
  override protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.Auto
  "the authorize client" should {
    "redirect to /login for HTML request" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val host = middlewareClientBinding.localAddress
      val uri = Uri()
        .withScheme("http")
        .withAuthority(host.getHostName, host.getPort)
        .withPath(Uri.Path./("authorize"))
        .withQuery(Uri.Query("claims" -> claims.toQueryString()))
      val acceptHtml: HttpHeader = headers.Accept(MediaTypes.`text/html`)
      val req = HttpRequest(uri = uri, headers = immutable.Seq(acceptHtml))
      for {
        resp <- Http().singleRequest(req)
      } yield {
        // Redirect to /login on middleware
        assert(resp.status == StatusCodes.Found)
      }
    }
    "not redirect to /login for JSON request" in {
      val claims = Request.Claims(actAs = List(Party("Alice")))
      val host = middlewareClientBinding.localAddress
      val uri = Uri()
        .withScheme("http")
        .withAuthority(host.getHostName, host.getPort)
        .withPath(Uri.Path./("authorize"))
        .withQuery(Uri.Query("claims" -> claims.toQueryString()))
      val acceptHtml: HttpHeader = headers.Accept(MediaTypes.`application/json`)
      val req = HttpRequest(uri = uri, headers = immutable.Seq(acceptHtml))
      for {
        resp <- Http().singleRequest(req)
      } yield {
        // Unauthorized with WWW-Authenticate header
        assert(resp.status == StatusCodes.Unauthorized)
      }
    }
  }
}

class TestMiddlewareClientLoginCallbackUri
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest {
  private val client = Client(
    Client.Config(
      authMiddlewareUri = Uri("http://auth.domain"),
      redirectToLogin = Client.RedirectToLogin.Yes,
      maxAuthCallbacks = 1000,
      authCallbackTimeout = FiniteDuration(1, duration.MINUTES),
      maxHttpEntityUploadSize = 4194304,
      httpEntityUploadTimeout = FiniteDuration(1, duration.MINUTES),
    )
  )
  "fixed callback URI" should {
    "be absolute" in {
      an[AssertionError] should be thrownBy client.routes(Uri().withPath(Uri.Path./("cb")))
    }
    "be used in login URI" in {
      val routes = client.routes(Uri("http://client.domain/cb"))
      val claims = Request.Claims(actAs = List(Party("Alice")))
      routes.loginUri(claims = claims) shouldBe
        Uri(
          s"http://auth.domain/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
        )
    }
  }
  "callback URI from request" should {
    "be used in login URI" in {
      val routes = client.routesFromRequestAuthority(Uri.Path./("cb"))
      val claims = Request.Claims(actAs = List(Party("Alice")))
      import akka.http.scaladsl.server.directives.RouteDirectives._
      Get("http://client.domain") ~> routes { routes =>
        complete(routes.loginUri(claims).toString)
      } ~> check {
        responseAs[String] shouldEqual
          s"http://auth.domain/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
      }
    }
  }
  "automatic callback URI" should {
    "be fixed when absolute" in {
      val routes = client.routesAuto(Uri("http://client.domain/cb"))
      val claims = Request.Claims(actAs = List(Party("Alice")))
      import akka.http.scaladsl.server.directives.RouteDirectives._
      Get() ~> routes { routes => complete(routes.loginUri(claims).toString) } ~> check {
        responseAs[String] shouldEqual
          s"http://auth.domain/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
      }
    }
    "be from request when relative" in {
      val routes = client.routesAuto(Uri().withPath(Uri.Path./("cb")))
      val claims = Request.Claims(actAs = List(Party("Alice")))
      import akka.http.scaladsl.server.directives.RouteDirectives._
      Get("http://client.domain") ~> routes { routes =>
        complete(routes.loginUri(claims).toString)
      } ~> check {
        responseAs[String] shouldEqual
          s"http://auth.domain/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
      }
    }
  }
}
