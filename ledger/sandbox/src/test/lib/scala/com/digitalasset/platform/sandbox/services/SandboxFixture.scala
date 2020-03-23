// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.digitalasset.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.digitalasset.platform.sandbox.{AbstractSandboxFixture, SandboxServer}
import com.digitalasset.ports.Port
import com.digitalasset.resources.ResourceOwner
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.ExecutionContext

trait SandboxFixture extends AbstractSandboxFixture with SuiteResource[(SandboxServer, Channel)] {
  self: Suite =>

  protected def server: SandboxServer = suiteResource.value._1

  override protected def serverPort: Port = server.port

  override protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: Resource[(SandboxServer, Channel)] = {
    implicit val ec: ExecutionContext = system.dispatcher
    new OwnedResource[(SandboxServer, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(_.map(info =>
            Some(info.jdbcUrl)))
        server <- SandboxServer.owner(config.copy(jdbcUrl = jdbcUrl))
        channel <- SandboxClientResource.owner(server.port)
      } yield (server, channel)
    )
  }
}
