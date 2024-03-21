// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.SslContext

import javax.net.ssl.SSLEngine

final case class TlsInfo(
    enabledCipherSuites: Seq[String],
    enabledProtocols: Seq[String],
    supportedCipherSuites: Seq[String],
    supportedProtocols: Seq[String],
)

object TlsInfo {
  def fromSslContext(sslContext: SslContext): TlsInfo = {
    val engine: SSLEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT)
    TlsInfo(
      enabledCipherSuites = engine.getEnabledCipherSuites.toIndexedSeq,
      enabledProtocols = engine.getEnabledProtocols.toIndexedSeq,
      supportedCipherSuites = engine.getSupportedCipherSuites.toIndexedSeq,
      supportedProtocols = engine.getSupportedProtocols.toIndexedSeq,
    )
  }
}
