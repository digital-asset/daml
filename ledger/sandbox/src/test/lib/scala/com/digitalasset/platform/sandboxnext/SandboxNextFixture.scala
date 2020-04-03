// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import com.daml.ledger.participant.state.v1.SeedService
import com.digitalasset.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.digitalasset.platform.sandbox.AbstractSandboxFixture
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.GrpcClientResource
import com.digitalasset.ports.Port
import com.digitalasset.resources.ResourceOwner
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

trait SandboxNextFixture extends AbstractSandboxFixture with SuiteResource[(Port, Channel)] {
  self: Suite =>

  override protected def config: SandboxConfig =
    super.config.copy(
      seeding = Some(SeedService.Seeding.Weak),
    )

  override protected def serverPort: Port = suiteResource.value._1

  override protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: Resource[(Port, Channel)] = {
    implicit val ec: ExecutionContext = system.dispatcher
    new OwnedResource[(Port, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(_.map(info =>
            Some(info.jdbcUrl)))
        port <- new Runner(config.copy(jdbcUrl = jdbcUrl))
        channel <- GrpcClientResource.owner(port)
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
