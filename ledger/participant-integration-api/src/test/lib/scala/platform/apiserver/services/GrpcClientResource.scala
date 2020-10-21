// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.platform.apiserver.EventLoopGroupOwner
import com.daml.ports.Port
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.EventLoopGroup

import scala.concurrent.Future

object GrpcClientResource {
  def owner(port: Port): ResourceOwner[Channel] =
    for {
      eventLoopGroup <- new EventLoopGroupOwner("api-client", sys.runtime.availableProcessors())
      channel <- channelOwner(port, EventLoopGroupOwner.clientChannelType, eventLoopGroup)
    } yield channel

  private def channelOwner(
      port: Port,
      channelType: Class[_ <: io.netty.channel.Channel],
      eventLoopGroup: EventLoopGroup,
  ): ResourceOwner[Channel] =
    new ResourceOwner[Channel] {
      override def acquire()(implicit context: ResourceContext): Resource[Channel] = {
        Resource(Future {
          NettyChannelBuilder
            .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port.value))
            .channelType(channelType)
            .eventLoopGroup(eventLoopGroup)
            .usePlaintext()
            .directExecutor()
            .build()
        })(channel =>
          Future {
            channel.shutdownNow()
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
              sys.error(
                "Unable to shutdown channel to a remote API under tests. Unable to recover. Terminating.")
            }
        })
      }
    }
}
