// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}
import io.netty.channel.nio.NioEventLoopGroup

import scala.concurrent.{Future, Promise}
import scala.util.Try

private[grpc] final class NioEventLoopGroupResourceOwner[Context: HasExecutionContext](
    threadCount: Int,
    threadFactory: ThreadFactory,
) extends AbstractResourceOwner[Context, NioEventLoopGroup] {
  override def acquire()(implicit context: Context): Resource[Context, NioEventLoopGroup] =
    ReleasableResource(Future(new NioEventLoopGroup(threadCount, threadFactory))) {
      eventLoopGroup =>
        val promise = Promise[Unit]()
        val future = eventLoopGroup.shutdownGracefully(0, 0, MILLISECONDS)
        future.addListener((f: io.netty.util.concurrent.Future[_]) =>
          promise.complete(Try(f.get).map(_ => ()))
        )
        promise.future
    }
}
