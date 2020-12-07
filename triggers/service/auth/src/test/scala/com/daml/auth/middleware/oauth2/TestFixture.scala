// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.time.{Instant, ZoneId}
import java.util.Date

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import com.auth0.jwt.JWTVerifier.BaseVerification
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Clock => Auth0Clock}
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

trait TestFixture
    extends AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResource[(AdjustableClock, OAuthServer, ServerBinding, ServerBinding)] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val jwtSecret: String = "secret"
  lazy protected val clock: AdjustableClock = suiteResource.value._1
  lazy protected val server: OAuthServer = suiteResource.value._2
  lazy protected val serverBinding: ServerBinding = suiteResource.value._3
  lazy protected val middlewareBinding: ServerBinding = suiteResource.value._4
  override protected lazy val suiteResource
    : Resource[(AdjustableClock, OAuthServer, ServerBinding, ServerBinding)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[
      ResourceContext,
      (AdjustableClock, OAuthServer, ServerBinding, ServerBinding)](
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
      } yield { (clock, server, serverBinding, middlewareBinding) }
    )
  }

  override protected def afterEach(): Unit = {
    server.resetAuthorizedParties()
    server.resetAdmin()

    super.afterEach()
  }
}
