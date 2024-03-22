// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.time.Duration
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, Location, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import com.daml.auth.middleware.api.{Client, Request, Response}
import com.daml.auth.middleware.api.Request.Claims
import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  CustomDamlJWTPayload,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref
import com.daml.auth.oauth2.api.{Response => OAuthResponse}
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Authenticity, Authorization}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest.{OptionValues, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.util.{Failure, Success}

abstract class TestMiddleware
    extends AsyncWordSpec
    with TestFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with OptionValues {

  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  val authorizationSecurity: SecurityTest =
    SecurityTest(property = Authorization, asset = "OAuth2 Middleware")

  protected[this] def makeJwt(
      claims: Request.Claims,
      expiresIn: Option[Duration],
  ): AuthServiceJWTPayload

  protected[this] def makeToken(
      claims: Request.Claims,
      secret: String = "secret",
      expiresIn: Option[Duration] = None,
  ): OAuthResponse.Token = {
    val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
    val jwtPayload = makeJwt(claims, expiresIn)
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
      bindingPort should ===(filePort)
    }
  }
  "the /auth endpoint" should {
    "return unauthorized without cookie" taggedAs authorizationSecurity in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      for {
        result <- middlewareClient.requestAuth(claims, Nil)
      } yield {
        result should ===(None)
      }
    }
    "return the token from a cookie" taggedAs authorizationSecurity in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val token = makeToken(claims)
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
        auth = result.value
      } yield {
        auth.accessToken should ===(token.accessToken)
        auth.refreshToken should ===(token.refreshToken)
      }
    }
    "return unauthorized on insufficient app id claims" taggedAs authorizationSecurity in {
      val claims = Request.Claims(
        actAs = List(Ref.Party.assertFromString("Alice")),
        applicationId = Some(Ref.ApplicationId.assertFromString("other-id")),
      )
      val token = makeToken(
        Request.Claims(
          actAs = List(Ref.Party.assertFromString("Alice")),
          applicationId = Some(Ref.ApplicationId.assertFromString("my-app-id")),
        )
      )
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        result should ===(None)
      }
    }
    "return unauthorized on an invalid token" taggedAs authorizationSecurity in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val token = makeToken(claims, "wrong-secret")
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        result should ===(None)
      }
    }
    "return unauthorized on an expired token" taggedAs authorizationSecurity in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val token = makeToken(claims, expiresIn = Some(Duration.ZERO))
      val _ = clock.fastForward(Duration.ofSeconds(1))
      val cookieHeader = Cookie("daml-ledger-token", token.toCookieValue)
      for {
        result <- middlewareClient.requestAuth(claims, List(cookieHeader))
      } yield {
        result should ===(None)
      }
    }

    "accept user tokens" taggedAs authorizationSecurity in {
      import com.daml.auth.middleware.oauth2.Server.rightsProvideClaims
      rightsProvideClaims(
        StandardJWTPayload(
          None,
          "foo",
          None,
          None,
          StandardJWTTokenFormat.Scope,
          List.empty,
          Some("daml_ledger_api"),
        ),
        Claims(
          admin = true,
          actAs = List(Ref.Party.assertFromString("Alice")),
          readAs = List(Ref.Party.assertFromString("Bob")),
          applicationId = Some(Ref.ApplicationId.assertFromString("foo")),
        ),
      ) should ===(true)
    }
  }
  "the /login endpoint" should {
    "redirect and set cookie" taggedAs authenticationSecurity.setHappyCase(
      "A valid request to /login redirects to client callback and sets cookie"
    ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
      for {
        resp <- Http().singleRequest(req)
        // Redirect to /authorize on authorization server
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
        // Redirect to /cb on middleware
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
      } yield {
        // Redirect to client callback
        resp.status should ===(StatusCodes.Found)
        resp.header[Location].value.uri should ===(middlewareClientCallbackUri)
        // Store token in cookie
        val cookie = resp.header[`Set-Cookie`].value.cookie
        cookie.name should ===("daml-ledger-token")
        val token = OAuthResponse.Token.fromCookieValue(cookie.value).value
        token.tokenType should ===("bearer")
      }
    }
    "return OK and set cookie without redirectUri" taggedAs authenticationSecurity.setHappyCase(
      "A valid request to /login returns OK and sets cookie when redirect is off"
    ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None, redirect = false))
      for {
        resp <- Http().singleRequest(req)
        // Redirect to /authorize on authorization server
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
        // Redirect to /cb on middleware
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
      } yield {
        // Return OK
        resp.status should ===(StatusCodes.OK)
        // Store token in cookie
        val cookie = resp.header[`Set-Cookie`].value.cookie
        cookie.name should ===("daml-ledger-token")
        val token = OAuthResponse.Token.fromCookieValue(cookie.value).value
        token.tokenType should ===("bearer")
      }
    }
  }
  "the /refresh endpoint" should {
    "return a new access token" taggedAs authorizationSecurity.setHappyCase(
      "A valid request to /refresh returns a new access token"
    ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val loginReq = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
      for {
        resp <- Http().singleRequest(loginReq)
        // Redirect to /authorize on authorization server
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
        // Redirect to /cb on middleware
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
        // Extract token from cookie
        (token1, refreshToken) = {
          val cookie = resp.header[`Set-Cookie`].value.cookie
          val token = OAuthResponse.Token.fromCookieValue(cookie.value).value
          (AccessToken(token.accessToken), RefreshToken(token.refreshToken.value))
        }
        // Advance time
        _ = clock.fastForward(Duration.ofSeconds(1))
        // Request /refresh
        authorize <- middlewareClient.requestRefresh(refreshToken)
      } yield {
        // Test that we got a new access token
        authorize.accessToken should !==(token1)
        // Test that we got a new refresh token
        authorize.refreshToken.value should !==(refreshToken)
      }
    }
    "fail on an invalid refresh token" taggedAs authorizationSecurity.setAttack(
      Attack("HTTP Client", "Presents an invalid refresh token", "refuse request with CLIENT_ERROR")
    ) in {
      for {
        exception <- middlewareClient.requestRefresh(RefreshToken("made-up-token")).transform {
          case Failure(exception: Client.RefreshException) => Success(exception)
          case value => fail(s"Expected failure with RefreshException but got $value")
        }
      } yield {
        exception.status shouldBe a[StatusCodes.ClientError]
      }
    }
  }
}

class TestMiddlewareClaimsToken extends TestMiddleware {
  override protected[this] def oauthYieldsUserTokens = false
  override protected[this] def makeJwt(
      claims: Request.Claims,
      expiresIn: Option[Duration],
  ): AuthServiceJWTPayload =
    CustomDamlJWTPayload(
      ledgerId = Some("test-ledger"),
      applicationId = Some("test-application"),
      participantId = None,
      exp = expiresIn.map(in => clock.instant.plus(in)),
      admin = claims.admin,
      actAs = claims.actAs,
      readAs = claims.readAs,
    )

  "the /auth endpoint given claim token" should {
    "return unauthorized on insufficient party claims" taggedAs authorizationSecurity in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Bob")))
      def r(actAs: String*)(readAs: String*) =
        middlewareClient
          .requestAuth(
            claims,
            Seq(
              Cookie(
                "daml-ledger-token",
                makeToken(
                  Request.Claims(
                    actAs = actAs.map(Ref.Party.assertFromString).toList,
                    readAs = readAs.map(Ref.Party.assertFromString).toList,
                  )
                ).toCookieValue,
              )
            ),
          )
      for {
        aliceA <- r("Alice")()
        nothing <- r()()
        aliceA_bobA <- r("Alice", "Bob")()
        aliceA_bobR <- r("Alice")("Bob")
        aliceR_bobA <- r("Bob")("Alice")
        aliceR_bobR <- r()("Alice", "Bob")
        bobA <- r("Bob")()
        bobR <- r()("Bob")
        bobAR <- r("Bob")("Bob")
      } yield {
        aliceA shouldBe empty
        nothing shouldBe empty
        aliceA_bobA should not be empty
        aliceA_bobR shouldBe empty
        aliceR_bobA should not be empty
        aliceR_bobR shouldBe empty
        bobA should not be empty
        bobR shouldBe empty
        bobAR should not be empty
      }
    }
  }

  "the /login endpoint with an oauth server checking claims" should {
    "not authorize unauthorized parties" taggedAs authorizationSecurity in {
      server.revokeParty(Ref.Party.assertFromString("Eve"))
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Eve")))
      ensureDisallowed(claims)
    }

    "not authorize disallowed admin claims" taggedAs authorizationSecurity in {
      server.revokeAdmin()
      val claims = Request.Claims(admin = true)
      ensureDisallowed(claims)
    }

    def ensureDisallowed(claims: Request.Claims) = {
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
      for {
        resp <- Http().singleRequest(req)
        // Redirect to /authorize on authorization server
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
        // Redirect to /cb on middleware
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
      } yield {
        // Redirect to client callback
        resp.status should ===(StatusCodes.Found)
        resp.header[Location].value.uri.withQuery(Uri.Query()) should ===(
          middlewareClientCallbackUri
        )
        // with error parameter set
        resp.header[Location].value.uri.query().toMap.get("error") should ===(Some("access_denied"))
        // Without token in cookie
        val cookie = resp.header[`Set-Cookie`]
        cookie should ===(None)
      }
    }
  }
}

class TestMiddlewareUserToken extends TestMiddleware {
  override protected[this] def makeJwt(
      claims: Request.Claims,
      expiresIn: Option[Duration],
  ): AuthServiceJWTPayload =
    StandardJWTPayload(
      issuer = None,
      userId = "test-application",
      participantId = None,
      exp = expiresIn.map(in => clock.instant.plus(in)),
      format = StandardJWTTokenFormat.Scope,
      audiences = List.empty,
      scope = Some("daml_ledger_api"),
    )
}

class TestMiddlewareCallbackUriOverride
    extends AsyncWordSpec
    with Matchers
    with OptionValues
    with TestFixture
    with SuiteResourceManagementAroundAll {
  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  override protected val middlewareCallbackUri = Some(Uri("http://localhost/MIDDLEWARE_CALLBACK"))
  "the /login endpoint with an oauth server checking claims" should {
    "redirect to the configured middleware callback URI" taggedAs authenticationSecurity
      .setHappyCase(
        "A valid request to /login redirects to middleware callback"
      ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      val req = HttpRequest(uri = middlewareClientRoutes.loginUri(claims, None))
      for {
        resp <- Http().singleRequest(req)
        // Redirect to /authorize on authorization server
        resp <- {
          resp.status should ===(StatusCodes.Found)
          val req = HttpRequest(uri = resp.header[Location].value.uri)
          Http().singleRequest(req)
        }
      } yield {
        // Redirect to configured callback URI on middleware
        resp.status should ===(StatusCodes.Found)
        resp.header[Location].value.uri.withQuery(Uri.Query()) should ===(
          middlewareCallbackUri.value
        )
      }
    }
  }
}

class TestMiddlewareLimitedCallbackStore
    extends AsyncWordSpec
    with Matchers
    with OptionValues
    with TestFixture
    with SuiteResourceManagementAroundAll {
  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  override protected val maxMiddlewareLogins = 2
  "the /login endpoint with an oauth server checking claims" should {
    "refuse requests when max capacity is reached" taggedAs authenticationSecurity.setAttack(
      Attack(
        "HTTP client",
        "Issues too many requests to the /login endpoint",
        "Refuse request with SERVICE_UNAVAILABLE",
      )
    ) in {
      def login(actAs: Ref.Party) = {
        val claims = Request.Claims(actAs = List(actAs))
        val uri = middlewareClientRoutes.loginUri(claims, None)
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      def followRedirect(resp: HttpResponse) = {
        resp.status should ===(StatusCodes.Found)
        val uri = resp.header[Location].value.uri
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      for {
        // Follow login flows up to redirect to middleware callback.
        redirectAlice <- login(Ref.Party.assertFromString("Alice"))
          .flatMap(followRedirect)
        redirectBob <- login(Ref.Party.assertFromString("Bob"))
          .flatMap(followRedirect)
        // The store should be full
        refusedCarol <- login(Ref.Party.assertFromString("Carol"))
        _ = refusedCarol.status should ===(StatusCodes.ServiceUnavailable)
        // Follow first redirect to middleware callback.
        resultAlice <- followRedirect(redirectAlice)
        _ = resultAlice.status should ===(StatusCodes.Found)
        // The store should have space again
        redirectCarol <- login(Ref.Party.assertFromString("Carol"))
          .flatMap(followRedirect)
        // Follow redirects to middleware callback.
        resultBob <- followRedirect(redirectBob)
        resultCarol <- followRedirect(redirectCarol)
      } yield {
        resultBob.status should ===(StatusCodes.Found)
        resultCarol.status should ===(StatusCodes.Found)
      }
    }
  }
}

class TestMiddlewareClientLimitedCallbackStore
    extends AsyncWordSpec
    with Matchers
    with OptionValues
    with TestFixture
    with SuiteResourceManagementAroundAll {
  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  override protected val maxClientAuthCallbacks = 2
  "the /login client with an oauth server checking claims" should {
    "refuse requests when max capacity is reached" taggedAs authenticationSecurity.setAttack(
      Attack(
        "HTTP client",
        "Issues too many requests to the /login endpoint",
        "Refuse request with SERVICE_UNAVAILABLE",
      )
    ) in {
      def login(actAs: Ref.Party) = {
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
        resp.status should ===(StatusCodes.Found)
        val uri = resp.header[Location].value.uri
        val req = HttpRequest(uri = uri)
        Http().singleRequest(req)
      }

      for {
        // Follow login flows up to last redirect to middleware client.
        redirectAlice <- login(Ref.Party.assertFromString("Alice"))
          .flatMap(followRedirect)
          .flatMap(followRedirect)
          .flatMap(followRedirect)
        redirectBob <- login(Ref.Party.assertFromString("Bob"))
          .flatMap(followRedirect)
          .flatMap(followRedirect)
          .flatMap(followRedirect)
        // The store should be full
        refusedCarol <- login(Ref.Party.assertFromString("Carol"))
        _ = refusedCarol.status should ===(StatusCodes.ServiceUnavailable)
        // Follow first redirect to middleware client.
        resultAlice <- followRedirect(redirectAlice)
        _ = resultAlice.status should ===(StatusCodes.OK)
        // The store should have space again
        redirectCarol <- login(Ref.Party.assertFromString("Carol"))
          .flatMap(followRedirect)
          .flatMap(followRedirect)
          .flatMap(followRedirect)
        resultBob <- followRedirect(redirectBob)
        resultCarol <- followRedirect(redirectCarol)
      } yield {
        resultBob.status should ===(StatusCodes.OK)
        resultCarol.status should ===(StatusCodes.OK)
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
  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  override protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.No
  "the TestMiddlewareClientNoRedirectToLogin client" should {
    "not redirect to /login" taggedAs authenticationSecurity.setHappyCase(
      "An unauthorized request should not redirect to /login using the JSON protocol"
    ) in {
      import com.daml.auth.middleware.api.JsonProtocol.responseAuthenticateChallengeFormat
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
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
        _ = resp.status should ===(StatusCodes.Unauthorized)
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
        headerChallenge.auth should ===(middlewareClient.authUri(claims))
        headerChallenge.login.withQuery(Uri.Query.Empty) should ===(
          middlewareClientRoutes.loginUri(claims).withQuery(Uri.Query.Empty)
        )
        headerChallenge.login.query().get("claims").value should ===(claims.toQueryString())
        bodyChallenge should ===(headerChallenge)
      }
    }
  }
}

class TestMiddlewareClientYesRedirectToLogin
    extends AsyncWordSpec
    with Matchers
    with OptionValues
    with TestFixture
    with SuiteResourceManagementAroundAll {
  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  override protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.Yes
  "the TestMiddlewareClientYesRedirectToLogin client" should {
    "redirect to /login" taggedAs authenticationSecurity.setHappyCase(
      "A valid HTTP request should redirect to /login"
    ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
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
        resp.status should ===(StatusCodes.Found)
        val loginUri = resp.header[headers.Location].value.uri
        loginUri.withQuery(Uri.Query.Empty) should ===(
          middlewareClientRoutes
            .loginUri(claims)
            .withQuery(Uri.Query.Empty)
        )
        loginUri.query().get("claims").value should ===(claims.toQueryString())
      }
    }
  }
}

class TestMiddlewareClientAutoRedirectToLogin
    extends AsyncWordSpec
    with Matchers
    with TestFixture
    with SuiteResourceManagementAroundAll {
  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "OAuth2 Middleware")

  override protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.Auto
  "the TestMiddlewareClientAutoRedirectToLogin client" should {
    "redirect to /login for HTML request" taggedAs authenticationSecurity.setHappyCase(
      "A HTML request is redirected to /login"
    ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
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
        resp.status should ===(StatusCodes.Found)
      }
    }
    "not redirect to /login for JSON request" taggedAs authenticationSecurity.setHappyCase(
      "A JSON request is not redirected to /login"
    ) in {
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
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
        resp.status should ===(StatusCodes.Unauthorized)
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
      authMiddlewareInternalUri = Uri("http://auth.internal"),
      authMiddlewareExternalUri = Uri("http://auth.external"),
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
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      routes.loginUri(claims = claims) shouldBe
        Uri(
          s"http://auth.external/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
        )
    }
  }
  "callback URI from request" should {
    "be used in login URI" in {
      val routes = client.routesFromRequestAuthority(Uri.Path./("cb"))
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      import org.apache.pekko.http.scaladsl.server.directives.RouteDirectives._
      Get("http://client.domain") ~> routes { routes =>
        complete(routes.loginUri(claims).toString)
      } ~> check {
        responseAs[String] shouldEqual
          s"http://auth.external/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
      }
    }
  }
  "automatic callback URI" should {
    "be fixed when absolute" in {
      val routes = client.routesAuto(Uri("http://client.domain/cb"))
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      import org.apache.pekko.http.scaladsl.server.directives.RouteDirectives._
      Get() ~> routes { routes => complete(routes.loginUri(claims).toString) } ~> check {
        responseAs[String] shouldEqual
          s"http://auth.external/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
      }
    }
    "be from request when relative" in {
      val routes = client.routesAuto(Uri().withPath(Uri.Path./("cb")))
      val claims = Request.Claims(actAs = List(Ref.Party.assertFromString("Alice")))
      import org.apache.pekko.http.scaladsl.server.directives.RouteDirectives._
      Get("http://client.domain") ~> routes { routes =>
        complete(routes.loginUri(claims).toString)
      } ~> check {
        responseAs[String] shouldEqual
          s"http://auth.external/login?claims=${claims.toQueryString()}&redirect_uri=http://client.domain/cb"
      }
    }
  }
}
