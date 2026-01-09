// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

import java.net.SocketAddress

final case class ServerWithChannelProvider(server: Server, channel: () => ManagedChannel) {

  def getClient[Stub](createStub: Channel => Stub): Stub = createStub(channel())
}

object ServerWithChannelProvider {
  def fromServices(
      services: Iterable[BindableService],
      address: Option[SocketAddress],
      serverName: String,
  ): ServerWithChannelProvider = {
    val serverBuilder = address.fold[ServerBuilder[? <: ServerBuilder[?]]](
      services.foldLeft(InProcessServerBuilder.forName(serverName))(_ addService _)
    )(a => services.foldLeft(NettyServerBuilder.forAddress(a))(_ addService _))
    val server = serverBuilder
      .build()

    server.start()

    ServerWithChannelProvider(
      server,
      () => getChannel(address.map(_ => server.getPort()), serverName),
    )
  }

  private def getChannel(port: Option[Int], serverName: String) =
    port
      .fold[ManagedChannelBuilder[?]](
        InProcessChannelBuilder
          .forName(serverName)
          .usePlaintext()
      )(p =>
        ManagedChannelBuilder
          .forAddress("127.0.0.1", p)
          .usePlaintext()
      )
      .build()
}
