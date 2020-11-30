// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.resources.ResourceContext
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.{AbstractSandboxFixture, SandboxBackend}
import com.daml.ports.Port
import io.grpc.Channel
import io.netty.handler.ssl.SslContext
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt

trait SandboxNextFixture extends AbstractSandboxFixture with SuiteResource[(Port, Channel)] {
  self: Suite =>

  override protected def serverPort: Port = suiteResource.value._1

  override protected def channel: Channel = suiteResource.value._2

  protected def clientSslContext: Option[SslContext] = None

  override protected lazy val suiteResource: Resource[(Port, Channel)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Channel)](
      for {
        // We must provide a random database if none is provided.
        // The default is to always use the same index database URL, which means that tests can
        // share an index. As you can imagine, this causes all manner of issues, the most important
        // of which is that the ledger and index databases will be out of sync.
        jdbcUrl <- database
          .getOrElse(SandboxBackend.H2Database.owner)
          .map(info => Some(info.jdbcUrl))
        port <- new Runner(config.copy(jdbcUrl = jdbcUrl))
        channel <- GrpcClientResource.owner(port, clientSslContext)
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
