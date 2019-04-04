// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.testing.utils.Resource
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory

object RemoteServerResource {
  def apply(host: String, port: Int) = new RemoteServerResource(host, port)
}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class RemoteServerResource(host: String, port: Int) extends Resource[PlatformChannels] {
  @volatile
  private var eventLoopGroup: EventLoopGroup = _
  @volatile
  private var channel: ManagedChannel = _

  override def value: PlatformChannels = PlatformChannels(channel)

  override def setup(): Unit = {
    eventLoopGroup = createEventLoopGroup("remote-server-client")

    channel = {
      val channelBuilder: NettyChannelBuilder = NettyChannelBuilder
        .forAddress(host, port)
      channelBuilder.eventLoopGroup(eventLoopGroup)
      channelBuilder.usePlaintext()
      channelBuilder.directExecutor()
      channelBuilder.build()
    }
  }

  def createEventLoopGroup(threadPoolName: String): NioEventLoopGroup = {
    val threadFactory = new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  override def close(): Unit = {
    channel.shutdownNow()
    channel.awaitTermination(1L, TimeUnit.SECONDS)
    eventLoopGroup.shutdownGracefully().await(1L, TimeUnit.SECONDS)
    channel = null
    eventLoopGroup = null
  }
}
