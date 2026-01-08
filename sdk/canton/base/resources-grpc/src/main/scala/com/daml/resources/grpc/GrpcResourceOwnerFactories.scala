// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ResourceOwnerFactories}
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel
import io.grpc.{Channel, ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}

import java.util.concurrent.ThreadFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait GrpcResourceOwnerFactories[Context] {
  protected implicit val hasExecutionContext: HasExecutionContext[Context]

  val EventLoopGroupChannelType: Class[? <: io.grpc.netty.shaded.io.netty.channel.Channel] =
    classOf[NioSocketChannel]

  def forEventLoopGroup(
      threadCount: Int,
      threadFactory: ThreadFactory,
  ): AbstractResourceOwner[Context, EventLoopGroup] =
    forNioEventLoopGroup(threadCount, threadFactory)

  def forServer(
      builder: ServerBuilder[?],
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, Server] =
    new ServerResourceOwner[Context](builder, shutdownTimeout)

  def forChannel(
      builder: ManagedChannelBuilder[?],
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, Channel] =
    forManagedChannel(builder, shutdownTimeout)

  private[grpc] def forNioEventLoopGroup(
      threadCount: Int,
      threadFactory: ThreadFactory,
  ): AbstractResourceOwner[Context, NioEventLoopGroup] =
    new NioEventLoopGroupResourceOwner[Context](threadCount, threadFactory)

  private[grpc] def forManagedChannel(
      builder: ManagedChannelBuilder[?],
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, ManagedChannel] =
    new ManagedChannelResourceOwner(builder, shutdownTimeout)

}

object GrpcResourceOwnerFactories
    extends ResourceOwnerFactories[ExecutionContext]
    with GrpcResourceOwnerFactories[ExecutionContext] {

  override protected implicit val hasExecutionContext: HasExecutionContext[ExecutionContext] =
    HasExecutionContext.`ExecutionContext has itself`

}
