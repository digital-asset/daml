// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.testing.utils.Resource
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{Channel, ManagedChannel}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory

class SandboxClientResource(port: Int) extends Resource[Channel] {
  @volatile
  private var eventLoopGroup: EventLoopGroup = _
  @volatile
  private var channel: ManagedChannel = _

  override def value: Channel = channel

  override def setup(): Unit = {
    eventLoopGroup = createEventLoopGroup("api-client")
    val channelBuilder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port))
    channelBuilder.eventLoopGroup(eventLoopGroup)
    channelBuilder.usePlaintext()
    channelBuilder.directExecutor()
    channel = channelBuilder.build()
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

  private def createEventLoopGroup(threadPoolName: String): NioEventLoopGroup = {
    val threadFactory = new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }
}
