// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{Channel, ManagedChannel}

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

object GrpcChannel {

  final class Owner(builder: NettyChannelBuilder) extends ResourceOwner[ManagedChannel] {
    def this(port: Int, configuration: LedgerClientChannelConfiguration) =
      this(
        configuration.builderFor(InetAddress.getLoopbackAddress.getHostAddress, port)
      )

    override def acquire()(implicit context: ResourceContext): Resource[ManagedChannel] =
      Resource(Future(builder.build()))(channel =>
        Future {
          discard(channel.shutdownNow())
        }
      )
  }

  def withShutdownHook(
      builder: NettyChannelBuilder
  ): ManagedChannel = {
    val channel = builder.build()
    discard(sys.addShutdownHook {
      discard(channel.shutdownNow())
    })
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
