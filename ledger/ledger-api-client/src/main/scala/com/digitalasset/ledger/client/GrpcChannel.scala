// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import java.net.{InetAddress, InetSocketAddress}
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ports.Port
import io.grpc.{Channel, ManagedChannel}
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

import java.util.concurrent.TimeUnit
import scala.concurrent.Future

object GrpcChannel {
  def close(channel: Channel): Unit =
    channel match {
      case channel: ManagedChannel =>
        // This includes closing active connections.
        channel.shutdownNow()
        channel.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
        ()
      case _ => // do nothing
    }

  def apply(
      builder: NettyChannelBuilder,
      configuration: LedgerClientConfiguration,
  ): ManagedChannel = {
    configuration.sslContext
      .fold(builder.usePlaintext())(builder.sslContext(_).negotiationType(NegotiationType.TLS))
    builder.maxInboundMetadataSize(configuration.maxInboundMetadataSize)
    builder.maxInboundMessageSize(configuration.maxInboundMessageSize)
    builder.build()
  }

  final class Owner(builder: NettyChannelBuilder, configuration: LedgerClientConfiguration)
      extends ResourceOwner[ManagedChannel] {
    def this(port: Port, configuration: LedgerClientConfiguration) =
      this(
        NettyChannelBuilder
          .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port.value)),
        configuration,
      )

    override def acquire()(implicit context: ResourceContext): Resource[ManagedChannel] =
      Resource(Future(GrpcChannel(builder, configuration)))(channel =>
        Future {
          channel.shutdownNow()
          ()
        }
      )
  }

  def withShutdownHook(
      builder: NettyChannelBuilder,
      configuration: LedgerClientConfiguration,
  ): ManagedChannel = {
    val channel = GrpcChannel(builder, configuration)
    sys.addShutdownHook {
      channel.shutdownNow()
      ()
    }
    channel
  }
}
