// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import java.time.{Clock, Instant, ZoneId}
import java.util.Date

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import com.auth0.jwt.JWTVerifier.BaseVerification
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Clock => Auth0Clock}
import com.daml.jwt.JwtVerifier
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  Resource,
  SuiteResource
}
import com.daml.ledger.resources.ResourceContext
import com.daml.oauth.server.{Config => OAuthConfig}
import com.daml.ports.Port
import org.scalatest.Suite

trait TestFixture
    extends AkkaBeforeAndAfterAll
    with SuiteResource[(Clock, ServerBinding, ServerBinding)] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val applicationId: String = "test-application"
  protected val jwtSecret: String = "secret"
  override protected lazy val suiteResource: Resource[(Clock, ServerBinding, ServerBinding)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Clock, ServerBinding, ServerBinding)](
      for {
        clock <- Resources.clock(Instant.now(), ZoneId.systemDefault())
        server <- Resources.authServer(
          OAuthConfig(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            applicationId = Some(applicationId),
            jwtSecret = jwtSecret,
            parties = Some(ApiTypes.Party.subst(Set("Alice", "Bob"))),
            clock = Some(clock),
          ))
        serverUri = Uri()
          .withScheme("http")
          .withAuthority(server.localAddress.getHostString, server.localAddress.getPort)
        client <- Resources.authMiddleware(
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
      } yield { (clock, server, client) }
    )
  }
}
