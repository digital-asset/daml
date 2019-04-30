// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.tls

import java.io.File

import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.{ClientAuth, SslContext}

final case class TlsConfiguration(
    enabled: Boolean,
    keyCertChainFile: Option[File], // mutual auth is disabled if null
    keyFile: Option[File],
    trustCertCollectionFile: Option[File]) { // System default if null

  def keyFileOrFail: File =
    keyFile.getOrElse(throw new IllegalStateException(
      s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyFile."))

  def keyCertChainFileOrFail: File =
    keyCertChainFile.getOrElse(throw new IllegalStateException(
      s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyCertChainFile."))

  /** If enabled and all required fields are present, it returns an SslContext suitable for client usage */
  def client: Option[SslContext] = {
    if (enabled)
      Some(
        GrpcSslContexts
          .forClient()
          .keyManager(keyCertChainFile.orNull, keyFileOrFail)
          .trustManager(trustCertCollectionFile.orNull)
          .build()
      )
    else None
  }

  /** If enabled and all required fields are present, it returns an SslContext suitable for server usage */
  def server: Option[SslContext] =
    if (enabled)
      Some(
        GrpcSslContexts
          .forServer(
            keyCertChainFileOrFail,
            keyFileOrFail
          )
          .trustManager(trustCertCollectionFile.orNull)
          .clientAuth(ClientAuth.REQUIRE)
          .build
      )
    else None
}
