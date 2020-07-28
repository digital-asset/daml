// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.resources.{Resource, ResourceOwner}
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.{EventLoopGroup, ServerChannel}

import scala.concurrent.ExecutionContext

case class ServerEventLoopGroups(
    worker: EventLoopGroup,
    boss: EventLoopGroup,
    channelType: Class[_ <: ServerChannel],
) {

  def populate(builder: NettyServerBuilder): NettyServerBuilder =
    builder
      .channelType(channelType)
      .bossEventLoopGroup(boss)
      .workerEventLoopGroup(worker)

}

object ServerEventLoopGroups {

  final class Owner(name: String, workerParallelism: Int, bossParallelism: Int)
      extends ResourceOwner[ServerEventLoopGroups] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[ServerEventLoopGroups] =
      Resource
        .sequence(
          Seq(
            new EventLoopGroupOwner(s"$name-worker", parallelism = workerParallelism).acquire(),
            new EventLoopGroupOwner(s"$name-boss", parallelism = bossParallelism).acquire(),
          ))
        .map {
          case Seq(worker, boss) =>
            ServerEventLoopGroups(
              worker = worker,
              boss = boss,
              channelType = EventLoopGroupOwner.serverChannelType,
            )
        }
  }

}
