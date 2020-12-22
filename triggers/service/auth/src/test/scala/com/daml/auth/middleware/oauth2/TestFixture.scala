// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.net.InetAddress
import java.time.{Instant, ZoneId}
import java.util.Date

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import com.auth0.jwt.JWTVerifier.BaseVerification
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Clock => Auth0Clock}
import com.daml.auth.middleware.api.Client
import com.daml.clock.AdjustableClock
import com.daml.jwt.JwtVerifier
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  Resource,
  SuiteResource
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
    authMiddlewareBinding: ServerBinding,
    authMiddlewareClient: Client,
    authMiddlewareClientBinding: ServerBinding)

trait TestFixture
    extends AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResource[TestResources] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val jwtSecret: String = "secret"
  protected val maxMiddlewareLogins: Long = Config.DefaultMaxLoginRequests
  protected val maxClientAuthCallbacks: Long = 1000
  protected val middlewareCallbackUri: Option[Uri] = None
  lazy protected val clock: AdjustableClock = suiteResource.value.clock
  lazy protected val server: OAuthServer = suiteResource.value.authServer
  lazy protected val serverBinding: ServerBinding = suiteResource.value.authServerBinding
  lazy protected val middlewareBinding: ServerBinding = suiteResource.value.authMiddlewareBinding
  lazy protected val middlewareClient: Client = suiteResource.value.authMiddlewareClient
  lazy protected val middlewareClientBinding: ServerBinding = {
    suiteResource.value.authMiddlewareClientBinding
  }
  lazy protected val middlewareClientCallbackUri: Uri = {
    val host = middlewareClientBinding.localAddress
    Uri()
      .withScheme("http")
      .withAuthority(host.getHostName, host.getPort)
      .withPath(Uri.Path./("cb"))
  }
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
          ))
        serverBinding <- Resources.authServerBinding(server)
        serverUri = Uri()
          .withScheme("http")
          .withAuthority(
            serverBinding.localAddress.getHostString,
            serverBinding.localAddress.getPort)
        middlewareBinding <- Resources.authMiddlewareBinding(
          Config(
            port = Port.Dynamic,
            callbackUri = middlewareCallbackUri,
            maxLoginRequests = maxMiddlewareLogins,
            loginTimeout = Config.DefaultLoginTimeout,
            oauthAuth = serverUri.withPath(Uri.Path./("authorize")),
            oauthToken = serverUri.withPath(Uri.Path./("token")),
            clientId = "middleware",
            clientSecret = "middleware-secret",
            tokenVerifier = new JwtVerifier(
              JWT
                .require(Algorithm.HMAC256(jwtSecret))
                .asInstanceOf[BaseVerification]
                .build(new Auth0Clock {
                  override def getToday: Date = Date.from(clock.instant())
                })
            )
          ))
        middlewareClientPort <- Resources.port()
        middlewareClientConfig = Client.Config(
          authMiddlewareUri = Uri()
            .withScheme("http")
            .withAuthority(
              middlewareBinding.localAddress.getHostName,
              middlewareBinding.localAddress.getPort),
          callbackUri = Uri()
            .withScheme("http")
            .withAuthority(InetAddress.getLoopbackAddress.getHostName, middlewareClientPort.value)
            .withPath(Uri.Path./("cb")),
          maxAuthCallbacks = maxClientAuthCallbacks,
          authCallbackTimeout = FiniteDuration(1, duration.MINUTES),
          maxHttpEntityUploadSize = 4194304,
          httpEntityUploadTimeout = FiniteDuration(1, duration.MINUTES)
        )
        middlewareClient = Client(middlewareClientConfig)
        middlewareClientBinding <- Resources
          .authMiddlewareClientBinding(middlewareClientConfig, middlewareClient)
      } yield
        TestResources(
          clock = clock,
          authServer = server,
          authServerBinding = serverBinding,
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
