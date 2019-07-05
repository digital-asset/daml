// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.ledger.api.tls.TlsConfiguration
import io.grpc.ManagedChannel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory

object RemoteServerResource {
  def apply(host: String, port: Int, tlsConfig: Option[TlsConfiguration]) =
    new RemoteServerResource(host, port, tlsConfig)
}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class RemoteServerResource(host: String, port: Int, tlsConfig: Option[TlsConfiguration])
    extends Resource[PlatformChannels] {

  @volatile
  private var eventLoopGroup: EventLoopGroup = _
  @volatile
  private var channel: ManagedChannel = _

  override def value: PlatformChannels = PlatformChannels(channel)

  override def setup(): Unit = {
    eventLoopGroup = createEventLoopGroup("remote-server-client")

    val channelBuilder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(host, port)
      .eventLoopGroup(eventLoopGroup)
      .directExecutor()

    tlsConfig
      .flatMap(_.client)
      .fold {
        channelBuilder.usePlaintext()
      } { sslContext =>
        channelBuilder.sslContext(sslContext).negotiationType(NegotiationType.TLS)
      }

    channel = channelBuilder.build()

  }

  private def createEventLoopGroup(threadPoolName: String): NioEventLoopGroup = {
    val threadFactory = new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  override def close(): Unit = {
    channel.shutdownNow()
    if (!channel.awaitTermination(5L, TimeUnit.SECONDS)) {
      sys.error(
        "Unable to shutdown channel to a remote API under tests. Unable to recover. Terminating.")
    }
    if (!eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).await(10L, TimeUnit.SECONDS)) {
      sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
    }
    channel = null
    eventLoopGroup = null
  }
}
