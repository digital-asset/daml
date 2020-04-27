// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.platform.sandbox.{AbstractSandboxFixture, SandboxServer}
import com.daml.ports.Port
import com.daml.resources.ResourceOwner
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

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
        channel <- GrpcClientResource.owner(server.port)
      } yield (server, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
