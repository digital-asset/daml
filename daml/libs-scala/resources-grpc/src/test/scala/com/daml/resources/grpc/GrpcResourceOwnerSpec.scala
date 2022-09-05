// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import com.daml.resources.grpc.{GrpcResourceOwnerFactories => Resources}
import io.grpc.health.v1.{HealthCheckRequest, HealthGrpc}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import io.grpc.protobuf.services.HealthStatusManager
import io.netty.util.concurrent.DefaultThreadFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

final class GrpcResourceOwnerSpec extends AsyncFlatSpec with Matchers {

  private val DefaultTimeout = 5.seconds

  behavior of "GrpcResourceOwnerFactories"

  it should "correctly manage the lifecycle of underlying resources" in {

    val serverName = InProcessServerBuilder.generateName()
    val serverBuilder = InProcessServerBuilder
      .forName(serverName)
      .addService(new HealthStatusManager().getHealthService)
    val channelBuilder = InProcessChannelBuilder.forName(serverName)

    val resource = for {
      server <- Resources.forServer(serverBuilder, DefaultTimeout).acquire()
      channel <- Resources.forManagedChannel(channelBuilder, DefaultTimeout).acquire()
    } yield {
      server.isShutdown shouldBe false
      channel.isShutdown shouldBe false
      (server, channel)
    }

    for {
      (server, channel) <- resource.asFuture
      _ = HealthGrpc.newBlockingStub(channel).check(HealthCheckRequest.newBuilder().build())
      _ <- resource.release()
    } yield {
      server.isShutdown shouldBe true
      channel.isShutdown shouldBe true
    }
  }

  it should "correctly manage Netty-based resources" in {

    val threadFactory =
      new DefaultThreadFactory("test-pool", false)

    val serverBuilder =
      NettyServerBuilder.forPort(0).addService(new HealthStatusManager().getHealthService)

    val resource = for {
      server <- Resources.forServer(serverBuilder, DefaultTimeout).acquire()
      eventLoopGroup <- Resources.forNioEventLoopGroup(1, threadFactory).acquire()
      channelBuilder = NettyChannelBuilder.forAddress(server.getListenSockets.get(0)).usePlaintext()
      channel <- Resources.forManagedChannel(channelBuilder, DefaultTimeout).acquire()
    } yield {
      eventLoopGroup.isShutdown shouldBe false
      server.isShutdown shouldBe false
      channel.isShutdown shouldBe false
      (eventLoopGroup, server, channel)
    }

    for {
      (eventLoopGroup, server, channel) <- resource.asFuture
      _ = HealthGrpc.newBlockingStub(channel).check(HealthCheckRequest.newBuilder().build())
      _ <- resource.release()
    } yield {
      eventLoopGroup.isShutdown shouldBe true
      server.isShutdown shouldBe true
      channel.isShutdown shouldBe true
    }

  }

}
