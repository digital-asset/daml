// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.configuration

import io.grpc.internal.GrpcUtil
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.netty.handler.ssl.SslContext

/** @param sslContext             If defined, the context will be passed on to the underlying gRPC code to ensure the communication channel is secured by TLS
  * @param maxInboundMetadataSize The maximum size of the response headers.
  * @param maxInboundMessageSize  The maximum (uncompressed) size of the response body.
  */
final case class LedgerClientChannelConfiguration(
    sslContext: Option[SslContext],
    maxInboundMetadataSize: Int = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE,
    maxInboundMessageSize: Int = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE,
) {

  def builderFor(host: String, port: Int): NettyChannelBuilder = {
    val builder = NettyChannelBuilder.forAddress(host, port)
    sslContext
      .fold(builder.usePlaintext())(builder.sslContext(_).negotiationType(NegotiationType.TLS))
      .maxInboundMetadataSize(maxInboundMetadataSize)
      .maxInboundMessageSize(maxInboundMessageSize)
  }

}

object LedgerClientChannelConfiguration {

  val InsecureDefaults: LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration(sslContext = None)

}
