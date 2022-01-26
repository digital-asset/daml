// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.daml.ledger.client.configuration.LedgerClientChannelConfiguration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ports.Port
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{Channel, ManagedChannel}

import scala.concurrent.Future

object GrpcChannel {

  final class Owner(builder: NettyChannelBuilder) extends ResourceOwner[ManagedChannel] {
    def this(port: Port, configuration: LedgerClientChannelConfiguration) =
      this(
        configuration.builderFor(InetAddress.getLoopbackAddress.getHostAddress, port.value)
      )

    override def acquire()(implicit context: ResourceContext): Resource[ManagedChannel] =
      Resource(Future(builder.build()))(channel =>
        Future {
          channel.shutdownNow()
          ()
        }
      )
  }

  def withShutdownHook(
      builder: NettyChannelBuilder
  ): ManagedChannel = {
    val channel = builder.build()
    sys.addShutdownHook {
      channel.shutdownNow()
      ()
    }
    channel
  }

  def close(channel: Channel): Unit =
    channel match {
      case channel: ManagedChannel =>
        // This includes closing active connections.
        channel.shutdownNow()
        channel.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
        ()
      case _ => // do nothing
    }

}
