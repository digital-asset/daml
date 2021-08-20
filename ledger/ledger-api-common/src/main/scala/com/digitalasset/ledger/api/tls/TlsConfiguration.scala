// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.{ClientAuth, SslContext}

import java.io.{ByteArrayInputStream, File, FileInputStream, InputStream}
import java.net.URL
import java.nio.file.Files
import scala.jdk.CollectionConverters._

final case class TlsConfiguration(
    enabled: Boolean,
    keyCertChainFile: Option[File], // mutual auth is disabled if null
    keyFile: Option[File],
    trustCertCollectionFile: Option[File], // System default if null
    secretsUrl: Option[URL] = None,
    clientAuth: ClientAuth =
      ClientAuth.REQUIRE, // Client auth setting used by the server. This is not used in the client configuration.
    enableCertRevocationChecking: Boolean = false,
    protocols: Seq[String] = Seq.empty,
) {

  /** If enabled and all required fields are present, it returns an SslContext suitable for client usage */
  def client: Option[SslContext] = {
    if (enabled)
      Some(
        GrpcSslContexts
          .forClient()
          .keyManager(
            keyCertChainFile.orNull,
            keyFile.orNull,
          )
          .trustManager(trustCertCollectionFile.orNull)
          .protocols(if (protocols.nonEmpty) protocols.asJava else null)
          .build()
      )
    else None
  }

  /** If enabled and all required fields are present, it returns an SslContext suitable for server usage */
  def server: Option[SslContext] =
    if (enabled) {
      scala.util.Using.resources(
        keyCertChainInputStreamOrFail,
        keyInputStreamOrFail,
      ) { (keyCertChain: InputStream, key: InputStream) =>
        Some(
          GrpcSslContexts
            .forServer(
              keyCertChain,
              key,
            )
            .trustManager(trustCertCollectionFile.orNull)
            .clientAuth(clientAuth)
            .protocols(if (protocols.nonEmpty) protocols.asJava else null)
            .build
        )

      }
    } else None

  /** This is a side-effecting method. It modifies JVM TLS properties according to the TLS configuration. */
  def setJvmTlsProperties(): Unit =
    if (enabled && enableCertRevocationChecking) OcspProperties.enableOcsp()

  private[tls] def keyInputStreamOrFail: InputStream = {
    val keyFileOrFail = keyFile.getOrElse(
      throw new IllegalArgumentException(
        s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyFile."
      )
    )
    prepareKeyInputStream(keyFileOrFail)
  }

  private[tls] def prepareKeyInputStream(keyFile: File): InputStream = {
    val bytes = if (keyFile.getName.endsWith(".enc")) {
      // TODO PBATKO: How to handle problems: url connection failure, json parsing failure, etc?
      val params = DecryptionParameters.fromSecretsServer(secretsUrlOrFail)
      params.decrypt(encrypted = keyFile)
    } else {
      Files.readAllBytes(keyFile.toPath)
    }
    new ByteArrayInputStream(bytes)
  }

  def secretsUrlOrFail: URL = secretsUrl.getOrElse(
    throw new IllegalStateException(
      s"Unable to convert ${this.toString} to SSL Context: cannot decrypt keyFile without secretsUrl."
    )
  )

  private def keyCertChainInputStreamOrFail: InputStream =
    keyCertChainFile
      .map(new FileInputStream(_))
      .getOrElse(
        throw new IllegalStateException(
          s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyCertChainFile."
        )
      )

}

object TlsConfiguration {
  val Empty: TlsConfiguration = TlsConfiguration(
    enabled = true,
    keyCertChainFile = None,
    keyFile = None,
    trustCertCollectionFile = None,
  )
}
