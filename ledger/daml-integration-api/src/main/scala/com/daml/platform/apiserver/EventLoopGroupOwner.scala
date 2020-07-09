// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.util.UUID
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.daml.resources.{Resource, ResourceOwner}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.channel.{Channel, EventLoopGroup, ServerChannel}
import io.netty.util.concurrent.DefaultThreadFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

final class EventLoopGroupOwner(threadPoolName: String, parallelism: Int)
    extends ResourceOwner[EventLoopGroup] {

  override def acquire()(implicit executionContext: ExecutionContext): Resource[EventLoopGroup] =
    Resource(
      Future(new NioEventLoopGroup(
        parallelism,
        new DefaultThreadFactory(s"$threadPoolName-grpc-event-loop-${UUID.randomUUID()}", true))))(
      group => {
        val promise = Promise[Unit]()
        val future = group.shutdownGracefully(0, 0, MILLISECONDS)
        future.addListener((f: io.netty.util.concurrent.Future[_]) =>
          promise.complete(Try(f.get).map(_ => ())))
        promise.future
      }
    )
}

object EventLoopGroupOwner {

  val clientChannelType: Class[_ <: Channel] = classOf[NioSocketChannel]

  val serverChannelType: Class[_ <: ServerChannel] = classOf[NioServerSocketChannel]

}
