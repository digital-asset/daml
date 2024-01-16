// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.ledger.resources.ResourceOwner
import com.daml.ports.Port
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.netty.channel.EventLoopGroup
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory

import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID
import scala.concurrent.duration.DurationInt

object GrpcClientResource {
  def owner(port: Port, sslContext: Option[SslContext] = None): ResourceOwner[Channel] = {
    val threadFactoryName = s"api-client-grpc-event-loop-${UUID.randomUUID()}"
    val threadFactory = new DefaultThreadFactory(threadFactoryName, true)
    val threadCount = sys.runtime.availableProcessors()
    for {
      eventLoopGroup <- ResourceOwner.forEventLoopGroup(threadCount, threadFactory)
      channelBuilder = makeChannelBuilder(port, eventLoopGroup, sslContext)
      channel <- ResourceOwner.forChannel(channelBuilder, shutdownTimeout = 5.seconds)
    } yield channel
  }

  private def makeChannelBuilder(
      port: Port,
      eventLoopGroup: EventLoopGroup,
      sslContext: Option[SslContext],
  ): NettyChannelBuilder = {
    val builder =
      NettyChannelBuilder
        .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port.value))
        .channelType(ResourceOwner.EventLoopGroupChannelType)
        .eventLoopGroup(eventLoopGroup)
        .directExecutor()

    sslContext
      .fold(builder.usePlaintext())(
        builder.sslContext(_).negotiationType(NegotiationType.TLS)
      )
  }
}
