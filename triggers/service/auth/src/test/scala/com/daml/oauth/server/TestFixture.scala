// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  Resource,
  SuiteResource
}
import com.daml.ports.Port
import org.scalatest.Suite

import scala.concurrent.ExecutionContext

trait TestFixture extends AkkaBeforeAndAfterAll with SuiteResource[(ServerBinding, ServerBinding)] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val applicationId: String = "test-application"
  protected val jwtSecret: String = "secret"
  override protected lazy val suiteResource: Resource[(ServerBinding, ServerBinding)] = {
    implicit val ec: ExecutionContext = system.dispatcher
    new OwnedResource[(ServerBinding, ServerBinding)](
      for {
        server <- Resources.authServer(
          Config(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            applicationId = applicationId,
            jwtSecret = jwtSecret))
        client <- Resources.authClient(
          Client.Config(
            port = Port.Dynamic,
            authServerUrl = Uri()
              .withScheme("http")
              .withAuthority(server.localAddress.getHostString, server.localAddress.getPort),
            clientId = "test-client",
            clientSecret = "test-client-secret"
          ))
      } yield { (server, client) }
    )
  }
}
