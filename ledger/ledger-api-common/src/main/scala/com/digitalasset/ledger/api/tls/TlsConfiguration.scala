// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.{ClientAuth, SslContext}

import java.io.{ByteArrayInputStream, File, FileInputStream, InputStream}
import java.net.URL
import java.nio.file.Files
import scala.jdk.CollectionConverters._

// TODO PBATKO: How to discern TlsConfig for client vs. for server? Is it obvious from the context where it was instantiated, or by some field? Or is the same one instance used (?sometimes) for both??
final case class TlsConfiguration(
    enabled: Boolean,
    // TODO PBATKO assign default None values to the params below?
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
          ) // TODO PBATKO handle encrypted keu files for clients: either fail with a meaningful message or (?unlikely) decrypt it the same way as for the server
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

  //}.getOrElse(
  //throw new IllegalStateException(
  //s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyFile."
  //)
  // TODO PBATKO: Array vs. ArrayBuffer vs. sth else?
  private def keyInputStreamOrFail: InputStream =
    keyFile
      .collect {
        case file if file.getName.endsWith(".enc") =>
          decryptKeyFile(keyFile = file, secretsUrl = secretsUrlOrFail)
        case file => Files.readAllBytes(file.toPath)
        // TODO PBATKO convert to a safe form (storing plain text private key in a file system is a big no no)

      }
      .map(bytes => new ByteArrayInputStream(bytes))
      .getOrElse(
        // TODO PBATKO accurate error messages: ? separate message for plaintext keyFile and cipherText keyFile
        //      current message is imprecise: the keyFile could exists and the part that failed is e.g. secrets server connectivity
        throw new IllegalArgumentException(
          s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyFile."
        )
      )

  def secretsUrlOrFail: URL = secretsUrl.getOrElse(
    throw new IllegalStateException(
      s"Unable to convert ${this.toString} to SSL Context: cannot decrypt keyFile withtou secretsUrl."
    )
  )

  private def decryptKeyFile(keyFile: File, secretsUrl: URL): Array[Byte] = {
    val params = DecryptionParameters.fromSecretsServer(secretsUrl)
    params.decrypt(encrypted = keyFile)
  }

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
