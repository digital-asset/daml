// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.testing.utils.{OwnedResource, Resource}
import com.digitalasset.platform.apiserver.EventLoopGroupOwner
import com.digitalasset.resources
import com.digitalasset.resources.ResourceOwner
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.EventLoopGroup

import scala.concurrent.{ExecutionContext, Future}

object SandboxClientResource {
  def apply(port: Int): Resource[Channel] =
    new OwnedResource[Channel](owner(port))

  def owner(port: Int): ResourceOwner[Channel] =
    for {
      eventLoopGroup <- new EventLoopGroupOwner("api-client", sys.runtime.availableProcessors())
      channel <- channelOwner(port, eventLoopGroup)
    } yield channel

  def channelOwner(port: Int, eventLoopGroup: EventLoopGroup): ResourceOwner[Channel] =
    new ResourceOwner[Channel] {
      override def acquire()(
          implicit executionContext: ExecutionContext
      ): resources.Resource[Channel] = {
        resources.Resource(Future {
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
