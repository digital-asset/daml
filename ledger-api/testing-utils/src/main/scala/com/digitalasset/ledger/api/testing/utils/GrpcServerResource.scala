// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import io.grpc._
import java.net.SocketAddress
import java.util.concurrent.TimeUnit

class GrpcServerResource(
    services: () => Iterable[BindableService with AutoCloseable],
    port: Option[SocketAddress])
    extends ManagedResource[ServerWithChannelProvider] {

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
