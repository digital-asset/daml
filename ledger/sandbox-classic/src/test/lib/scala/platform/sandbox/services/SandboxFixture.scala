// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.time.Duration

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.{AbstractSandboxFixture, SandboxServer}
import com.daml.ports.Port
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt

trait SandboxFixture extends AbstractSandboxFixture with SuiteResource[(SandboxServer, Channel)] {
  self: Suite =>

  // TODO append-only: remove after the mutating schema is removed
  protected def enableAppendOnlySchema: Boolean = false

  override protected def config: SandboxConfig =
    super.config.copy(
      delayBeforeSubmittingLedgerConfiguration = Duration.ZERO,
      enableAppendOnlySchema = enableAppendOnlySchema,
    )

  protected def server: SandboxServer = suiteResource.value._1

  override protected def serverPort: Port = server.port

  override protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: Resource[(SandboxServer, Channel)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (SandboxServer, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(
            _.map(info => Some(info.jdbcUrl))
          )
        server <- SandboxServer.owner(config.copy(jdbcUrl = jdbcUrl))
        channel <- GrpcClientResource.owner(server.port)
      } yield (server, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
