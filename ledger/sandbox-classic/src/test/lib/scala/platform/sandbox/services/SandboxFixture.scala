// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import com.daml.ledger.api.testing.utils.{OwnedResource, SuiteResource, Resource => TestResource}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.{AbstractSandboxFixture, SandboxServer}
import com.daml.ports.Port
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt

trait SandboxFixture extends AbstractSandboxFixture with SuiteResource[(SandboxServer, Channel)] {
  self: Suite =>

  override protected def config: SandboxConfig =
    super.config.copy(
      seeding = None,
      ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
    )

  protected def server: SandboxServer = suiteResource.value._1

  override protected def serverPort: Port = server.port

  override protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: TestResource[(SandboxServer, Channel)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (SandboxServer, Channel)](
      Owner,
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }

  private object Owner extends ResourceOwner[(SandboxServer, Channel)] {
    override def acquire()(implicit context: ResourceContext): Resource[(SandboxServer, Channel)] =
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(_.map(info =>
            Some(info.jdbcUrl)))
          .acquire()
        server <- SandboxServer.owner(config.copy(jdbcUrl = jdbcUrl)).acquire()
        _ <- Resource.fromFuture(server.whenReady)
        channel <- GrpcClientResource.owner(server.port).acquire()
      } yield (server, channel)
  }
}
