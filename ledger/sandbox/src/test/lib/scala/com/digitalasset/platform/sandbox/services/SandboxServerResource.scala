// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{Channel, ManagedChannel}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory

class SandboxServerResource(config: => SandboxConfig) extends Resource[Channel] {
  @volatile
  private var eventLoopGroup: EventLoopGroup = _
  @volatile
  private var channel: ManagedChannel = _
  @volatile
  private var sandboxServer: SandboxServer = _

  override def value: Channel = channel

  override def setup(): Unit = {
    sandboxServer = SandboxServer(config)
    eventLoopGroup = createEventLoopGroup("api-client")

    channel = {
      val channelBuilder: NettyChannelBuilder = NettyChannelBuilder
        .forAddress("127.0.0.1", sandboxServer.port)
      channelBuilder.eventLoopGroup(eventLoopGroup)
      channelBuilder.usePlaintext()
      channelBuilder.directExecutor()
      channelBuilder.build()
    }
  }

  private def createEventLoopGroup(threadPoolName: String): NioEventLoopGroup = {
    val threadFactory = new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  override def close(): Unit = {
    channel.shutdownNow()
    eventLoopGroup.shutdownGracefully().await(10L, TimeUnit.SECONDS)
    sandboxServer.close()
    channel = null
    eventLoopGroup = null
    sandboxServer = null
  }

  def getPort: Int = sandboxServer.port
}
