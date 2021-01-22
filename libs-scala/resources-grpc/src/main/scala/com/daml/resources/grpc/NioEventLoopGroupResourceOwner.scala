// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import java.util.concurrent.ThreadFactory

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, Resource}
import io.netty.channel.nio.NioEventLoopGroup

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[grpc] final class NioEventLoopGroupResourceOwner[Context: HasExecutionContext](
    threadCount: Int,
    threadFactory: ThreadFactory,
    shutdownTimeout: FiniteDuration,
) extends AbstractResourceOwner[Context, NioEventLoopGroup] {
  override def acquire()(implicit context: Context): Resource[Context, NioEventLoopGroup] =
    Resource[Context].apply(Future(new NioEventLoopGroup(threadCount, threadFactory))) {
      eventLoopGroup =>
        Future {
          val shutdownFuture = eventLoopGroup.shutdownGracefully()
          if (!shutdownFuture.await(shutdownTimeout.length, shutdownTimeout.unit))
            throw shutdownFuture.cause()
        }
    }
}
