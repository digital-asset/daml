// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.testing

import java.net.{InetAddress, InetSocketAddress, SocketAddress}

import com.daml.ports.FreePort
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.{ManagedChannelBuilder, ServerBuilder}

import scala.concurrent.ExecutionContext

object DummyTestSetup {

  final class Builders(
      val participantServer: ServerBuilder[_ <: ServerBuilder[_]],
      val participantChannel: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]],
      val proxyServer: ServerBuilder[_ <: ServerBuilder[_]],
      val proxyChannel: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]],
  )

  object Builders {

    private def withRequiredServices[Builder <: ServerBuilder[Builder]](
        builder: Builder,
        executionContext: ExecutionContext,
    ): Builder =
      builder
        .addService(DummyLedgerIdentityService.bind(executionContext))
        .addService(DummyCommandSubmissionService.bind(executionContext))
        .addService(DummyCommandService.bind(executionContext))
        .addService(ProtoReflectionService.newInstance())

    def apply(useNetworkStack: Boolean, serviceExecutionContext: ExecutionContext): Builders = {
      if (useNetworkStack) {
        val participantPort: Int = FreePort.find().value
        val participantAddress: SocketAddress =
          new InetSocketAddress(InetAddress.getLoopbackAddress, participantPort)
        val proxyPort: Int = FreePort.find().value
        val proxyAddress: SocketAddress =
          new InetSocketAddress(InetAddress.getLoopbackAddress, proxyPort)
        new Builders(
          withRequiredServices(
            NettyServerBuilder.forPort(participantPort),
            serviceExecutionContext,
          ),
          NettyChannelBuilder.forAddress(participantAddress).usePlaintext(),
          NettyServerBuilder.forPort(proxyPort),
          NettyChannelBuilder.forAddress(proxyAddress).usePlaintext(),
        )
      } else {
        val participantName: String = InProcessServerBuilder.generateName()
        val proxyName: String = InProcessServerBuilder.generateName()
        new Builders(
          withRequiredServices(
            InProcessServerBuilder.forName(participantName),
            serviceExecutionContext,
          ),
          InProcessChannelBuilder.forName(participantName),
          InProcessServerBuilder.forName(proxyName),
          InProcessChannelBuilder.forName(proxyName),
        )
      }
    }

  }

}
