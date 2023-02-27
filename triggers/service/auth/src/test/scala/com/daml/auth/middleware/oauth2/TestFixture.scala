// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.io.File
import java.time.{Instant, ZoneId}

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import com.auth0.jwt.JWTVerifier.BaseVerification
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.daml.auth.middleware.api.Client
import com.daml.clock.AdjustableClock
import com.daml.jwt.JwtVerifier
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  Resource,
  SuiteResource,
}
import com.daml.ledger.resources.ResourceContext
import com.daml.auth.oauth2.test.server.{Config => OAuthConfig, Server => OAuthServer}
import com.daml.ports.Port
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration

case class TestResources(
    clock: AdjustableClock,
    authServer: OAuthServer,
    authServerBinding: ServerBinding,
    authMiddlewarePortFile: File,
    authMiddlewareBinding: ServerBinding,
    authMiddlewareClient: Client,
    authMiddlewareClientBinding: ServerBinding,
)

trait TestFixture
    extends AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResource[TestResources] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val jwtSecret: String = "secret"
  protected val maxMiddlewareLogins: Int = Config.DefaultMaxLoginRequests
  protected val maxClientAuthCallbacks: Int = 1000
  protected val middlewareCallbackUri: Option[Uri] = None
  protected val middlewareClientCallbackPath: Uri.Path = Uri.Path./("cb")
  protected val redirectToLogin: Client.RedirectToLogin = Client.RedirectToLogin.Yes
  lazy protected val clock: AdjustableClock = suiteResource.value.clock
  lazy protected val server: OAuthServer = suiteResource.value.authServer
  lazy protected val serverBinding: ServerBinding = suiteResource.value.authServerBinding
  lazy protected val middlewarePortFile: File = suiteResource.value.authMiddlewarePortFile
  lazy protected val middlewareBinding: ServerBinding = suiteResource.value.authMiddlewareBinding
  lazy protected val middlewareClient: Client = suiteResource.value.authMiddlewareClient
  lazy protected val middlewareClientBinding: ServerBinding = {
    suiteResource.value.authMiddlewareClientBinding
  }
  lazy protected val middlewareClientCallbackUri: Uri = {
    val host = middlewareClientBinding.localAddress
    Uri()
      .withScheme("http")
      .withAuthority("localhost", host.getPort)
      .withPath(middlewareClientCallbackPath)
  }
  lazy protected val middlewareClientRoutes: Client.Routes =
    middlewareClient.routes(middlewareClientCallbackUri)
  protected def oauthYieldsUserTokens: Boolean = true
  override protected lazy val suiteResource: Resource[TestResources] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, TestResources](
      for {
        clock <- Resources.clock(Instant.now(), ZoneId.systemDefault())
        server = OAuthServer(
          OAuthConfig(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            jwtSecret = jwtSecret,
            clock = Some(clock),
            yieldUserTokens = oauthYieldsUserTokens,
          )
        )
        serverBinding <- Resources.authServerBinding(server)
        serverUri = Uri()
          .withScheme("http")
          .withAuthority(
            serverBinding.localAddress.getHostString,
            serverBinding.localAddress.getPort,
          )
        tempDir <- Resources.temporaryDirectory()
        middlewarePortFile = new File(tempDir, "port")
        middlewareBinding <- Resources.authMiddlewareBinding(
          Config(
            address = "localhost",
            port = 0,
            portFile = Some(middlewarePortFile.toPath),
            callbackUri = middlewareCallbackUri,
            maxLoginRequests = maxMiddlewareLogins,
            loginTimeout = Config.DefaultLoginTimeout,
            cookieSecure = Config.DefaultCookieSecure,
            oauthAuth = serverUri.withPath(Uri.Path./("authorize")),
            oauthToken = serverUri.withPath(Uri.Path./("token")),
            oauthAuthTemplate = None,
            oauthTokenTemplate = None,
            oauthRefreshTemplate = None,
            clientId = "middleware",
            clientSecret = SecretString("middleware-secret"),
            tokenVerifier = new JwtVerifier(
              JWT
                .require(Algorithm.HMAC256(jwtSecret))
                .asInstanceOf[BaseVerification]
                .build(clock)
            ),
            histograms = Seq.empty,
          )
        )
        authUri = Uri()
          .withScheme("http")
          .withAuthority(
            middlewareBinding.localAddress.getHostName,
            middlewareBinding.localAddress.getPort,
          )
        middlewareClientConfig = Client.Config(
          authMiddlewareInternalUri = authUri,
          authMiddlewareExternalUri = authUri,
          redirectToLogin = redirectToLogin,
          maxAuthCallbacks = maxClientAuthCallbacks,
          authCallbackTimeout = FiniteDuration(1, duration.MINUTES),
          maxHttpEntityUploadSize = 4194304,
          httpEntityUploadTimeout = FiniteDuration(1, duration.MINUTES),
        )
        middlewareClient = Client(middlewareClientConfig)
        middlewareClientBinding <- Resources
          .authMiddlewareClientBinding(middlewareClient, middlewareClientCallbackPath)
      } yield TestResources(
        clock = clock,
        authServer = server,
        authServerBinding = serverBinding,
        authMiddlewarePortFile = middlewarePortFile,
        authMiddlewareBinding = middlewareBinding,
        authMiddlewareClient = middlewareClient,
        authMiddlewareClientBinding = middlewareClientBinding,
      )
    )
  }

  override protected def afterEach(): Unit = {
    server.resetAuthorizedParties()
    server.resetAdmin()

    super.afterEach()
  }
}
