// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import java.time.{Instant, ZoneId}

import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.Uri
import com.daml.clock.AdjustableClock
import com.daml.ledger.api.testing.utils.{
  PekkoBeforeAndAfterAll,
  OwnedResource,
  Resource,
  SuiteResource,
}
import com.daml.ledger.resources.ResourceContext
import com.daml.ports.Port
import org.scalatest.{BeforeAndAfterEach, Suite}

trait TestFixture
    extends PekkoBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResource[(AdjustableClock, Server, ServerBinding, ServerBinding)] {
  self: Suite =>
  protected val ledgerId: String = "test-ledger"
  protected val applicationId: String = "test-application"
  protected val jwtSecret: String = "secret"
  lazy protected val clock: AdjustableClock = suiteResource.value._1
  lazy protected val server: Server = suiteResource.value._2
  lazy protected val serverBinding: ServerBinding = suiteResource.value._3
  lazy protected val clientBinding: ServerBinding = suiteResource.value._4
  protected[this] def yieldUserTokens: Boolean
  override protected lazy val suiteResource
      : Resource[(AdjustableClock, Server, ServerBinding, ServerBinding)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (AdjustableClock, Server, ServerBinding, ServerBinding)](
      for {
        clock <- Resources.clock(Instant.now(), ZoneId.systemDefault())
        server = Server(
          Config(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            jwtSecret = jwtSecret,
            clock = Some(clock),
            yieldUserTokens = yieldUserTokens,
          )
        )
        serverBinding <- Resources.authServerBinding(server)
        clientBinding <- Resources.authClientBinding(
          Client.Config(
            port = Port.Dynamic,
            authServerUrl = Uri()
              .withScheme("http")
              .withAuthority(
                serverBinding.localAddress.getHostString,
                serverBinding.localAddress.getPort,
              ),
            clientId = "test-client",
            clientSecret = "test-client-secret",
          )
        )
      } yield { (clock, server, serverBinding, clientBinding) }
    )
  }

  override protected def afterEach(): Unit = {
    server.resetAuthorizedParties()
    server.resetAdmin()

    super.afterEach()
  }
}
