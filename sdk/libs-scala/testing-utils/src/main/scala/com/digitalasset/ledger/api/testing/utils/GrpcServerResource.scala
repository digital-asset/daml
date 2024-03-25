// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

import io.grpc._

final class GrpcServerResource(
    services: () => Iterable[BindableService with AutoCloseable],
    port: Option[SocketAddress],
) extends ManagedResource[ServerWithChannelProvider] {

  @volatile private var boundServices: Iterable[BindableService with AutoCloseable] = Nil

  override protected def construct(): ServerWithChannelProvider = {
    boundServices = services()
    ServerWithChannelProvider.fromServices(boundServices, port, "server")
  }

  override protected def destruct(resource: ServerWithChannelProvider): Unit = {
    resource.server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
    boundServices.foreach(_.close())
    boundServices = Nil
    ()
  }
}
