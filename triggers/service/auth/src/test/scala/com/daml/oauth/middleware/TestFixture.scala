// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
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

trait TestFixture extends AkkaBeforeAndAfterAll with SuiteResource[(ServerBinding, ServerBinding)] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val applicationId: String = "test-application"
  protected val jwtSecret: String = "secret"
  override protected lazy val suiteResource: Resource[(ServerBinding, ServerBinding)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (ServerBinding, ServerBinding)](
      for {
        server <- Resources.authServer(
          OAuthConfig(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            applicationId = Some(applicationId),
            jwtSecret = jwtSecret))
        serverUri = Uri()
          .withScheme("http")
          .withAuthority(server.localAddress.getHostString, server.localAddress.getPort)
        client <- Resources.authMiddleware(
          Config(
            port = Port.Dynamic,
            oauthAuth = serverUri.withPath(Uri.Path./("authorize")),
            oauthToken = serverUri.withPath(Uri.Path./("token")),
            clientId = "middleware",
            clientSecret = "middleware-secret"
          ))
      } yield { (server, client) }
    )
  }
}
