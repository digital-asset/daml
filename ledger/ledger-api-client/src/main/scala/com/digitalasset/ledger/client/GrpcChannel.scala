// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import java.net.{InetAddress, InetSocketAddress}

import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.ports.Port
import com.daml.resources.{Resource, ResourceOwner}
import io.grpc.ManagedChannel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

import scala.concurrent.{ExecutionContext, Future}

object GrpcChannel {

  final class Owner(builder: NettyChannelBuilder, configuration: LedgerClientConfiguration)
      extends ResourceOwner[ManagedChannel] {
    def this(port: Port, configuration: LedgerClientConfiguration) =
      this(
        NettyChannelBuilder
          .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port.value)),
        configuration,
      )

    override def acquire()(implicit executionContext: ExecutionContext): Resource[ManagedChannel] =
      Resource(
        Future {
          configuration.sslContext
            .fold(builder.usePlaintext())(
              builder.sslContext(_).negotiationType(NegotiationType.TLS))
          builder.maxInboundMetadataSize(configuration.maxInboundMessageSize)
          builder.build()
        }
      )(channel =>
        Future {
          channel.shutdownNow()
          ()
      })
  }

}
