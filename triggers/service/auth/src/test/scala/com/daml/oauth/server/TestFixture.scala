// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import java.time.{Instant, ZoneId}

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import com.daml.clock.AdjustableClock
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  Resource,
  SuiteResource
}
import com.daml.ledger.resources.ResourceContext
import com.daml.ports.Port
import com.daml.ledger.api.refinements.ApiTypes.Party
import org.scalatest.Suite

trait TestFixture
    extends AkkaBeforeAndAfterAll
    with SuiteResource[(AdjustableClock, ServerBinding, ServerBinding)] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val applicationId: String = "test-application"
  protected val jwtSecret: String = "secret"
  override protected lazy val suiteResource
    : Resource[(AdjustableClock, ServerBinding, ServerBinding)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (AdjustableClock, ServerBinding, ServerBinding)](
      for {
        clock <- Resources.clock(Instant.now(), ZoneId.systemDefault())
        server <- Resources.authServer(
          Config(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            jwtSecret = jwtSecret,
            parties = Some(Party.subst(Set("Alice", "Bob"))),
            clock = Some(clock)
          ))
        client <- Resources.authClient(
          Client.Config(
            port = Port.Dynamic,
            authServerUrl = Uri()
              .withScheme("http")
              .withAuthority(server.localAddress.getHostString, server.localAddress.getPort),
            clientId = "test-client",
            clientSecret = "test-client-secret"
          ))
      } yield { (clock, server, client) }
    )
  }
}
