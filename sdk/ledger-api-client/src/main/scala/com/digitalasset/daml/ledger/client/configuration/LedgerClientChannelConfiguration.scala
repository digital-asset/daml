// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client.configuration

import io.grpc.netty.shaded.io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

/** @param sslContext
  *   If defined, the context will be passed on to the underlying gRPC code to ensure the
  *   communication channel is secured by TLS.
  * @param maxInboundMetadataSize
  *   The maximum size of the response headers.
  * @param maxInboundMessageSize
  *   The maximum (uncompressed) size of the response body.
  */
final case class LedgerClientChannelConfiguration(
    sslContext: Option[SslContext],
    maxInboundMetadataSize: Int = LedgerClientChannelConfiguration.DefaultMaxInboundMetadataSize,
    maxInboundMessageSize: Int = LedgerClientChannelConfiguration.DefaultMaxInboundMessageSize,
    flowControlWindow: Int = LedgerClientChannelConfiguration.DefaultFlowControlWindow,
) {

  def builderFor(host: String, port: Int): NettyChannelBuilder = {
    val builder = NettyChannelBuilder.forAddress(host, port)
    sslContext
      .fold(builder.usePlaintext())(builder.sslContext(_).negotiationType(NegotiationType.TLS))
      .maxInboundMetadataSize(maxInboundMetadataSize)
      .maxInboundMessageSize(maxInboundMessageSize)
      .flowControlWindow(flowControlWindow)
  }

}

object LedgerClientChannelConfiguration {

  val DefaultMaxInboundMetadataSize: Int = 8192
  val DefaultMaxInboundMessageSize: Int = 10 * 1024 * 1024
  val DefaultFlowControlWindow: Int = 1024 * 1024 // 1mb instead of 64kb

  val InsecureDefaults: LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration(sslContext = None)

}
