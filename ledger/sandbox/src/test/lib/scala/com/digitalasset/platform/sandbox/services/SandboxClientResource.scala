// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.digitalasset.platform.apiserver.EventLoopGroupOwner
import com.digitalasset.resources.{Resource, ResourceOwner}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.EventLoopGroup

import scala.concurrent.{ExecutionContext, Future}

object SandboxClientResource {
  def owner(port: Int): ResourceOwner[Channel] =
    for {
      eventLoopGroup <- new EventLoopGroupOwner("api-client", sys.runtime.availableProcessors())
      channel <- channelOwner(port, eventLoopGroup)
    } yield channel

  def channelOwner(port: Int, eventLoopGroup: EventLoopGroup): ResourceOwner[Channel] =
    new ResourceOwner[Channel] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[Channel] = {
        Resource(Future {
          NettyChannelBuilder
            .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port))
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
