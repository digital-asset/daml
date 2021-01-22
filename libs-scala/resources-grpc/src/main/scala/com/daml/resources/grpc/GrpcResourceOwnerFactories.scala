// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import java.util.concurrent.ThreadFactory

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ResourceOwnerFactories}
import io.grpc.{Channel, ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait GrpcResourceOwnerFactories[Context] {
  protected implicit val hasExecutionContext: HasExecutionContext[Context]

  def forEventLoopGroup(
      threadCount: Int,
      threadFactory: ThreadFactory,
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, EventLoopGroup] =
    forNioEventLoopGroup(threadCount, threadFactory, shutdownTimeout)

  def forServer(
      builder: ServerBuilder[_],
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, Server] =
    new ServerResourceOwner[Context](builder, shutdownTimeout)

  def forChannel(
      builder: ManagedChannelBuilder[_],
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, Channel] =
    forManagedChannel(builder, shutdownTimeout)

  private[grpc] def forNioEventLoopGroup(
      threadCount: Int,
      threadFactory: ThreadFactory,
      shutdownTimeout: FiniteDuration,
  ): AbstractResourceOwner[Context, NioEventLoopGroup] =
    new NioEventLoopGroupResourceOwner[Context](threadCount, threadFactory, shutdownTimeout)

  private[grpc] def forManagedChannel(
      builder: ManagedChannelBuilder[_],
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
