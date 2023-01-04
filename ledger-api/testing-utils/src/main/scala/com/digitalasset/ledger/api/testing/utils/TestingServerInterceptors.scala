// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import java.net.{InetAddress, InetSocketAddress}

import com.daml.ledger.resources.{ResourceContext, ResourceOwner, Resource => LedgerResource}
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import io.grpc.{BindableService, Channel, Server, ServerInterceptor}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object TestingServerInterceptors {

  def channelOwner(
      interceptor: ServerInterceptor,
      service: BindableService,
  ): ResourceOwner[Channel] = {
    for {
      server <- serverOwner(interceptor, service)
      channel <- ResourceOwner.forChannel(
        NettyChannelBuilder
          .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, server.getPort))
          .usePlaintext(),
        shutdownTimeout = 5.seconds,
      )
    } yield channel
  }

  def serverOwner(
      interceptor: ServerInterceptor,
      service: BindableService,
  ): ResourceOwner[Server] =
    new ResourceOwner[Server] {
      def acquire()(implicit context: ResourceContext): LedgerResource[Server] =
        LedgerResource(Future {
          val server =
            NettyServerBuilder
              .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
              .directExecutor()
              .intercept(interceptor)
              .addService(service)
              .build()
          server.start()
          server
        })(server => Future(server.shutdown().awaitTermination()))
    }
}
