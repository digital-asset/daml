// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.tls

import com.digitalasset.canton.ledger.api.tls.TlsVersion.{TlsVersion, V1, V1_1, V1_2, V1_3}
import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.{ClientAuth, SslContext}
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, File, FileInputStream, InputStream}
import java.lang
import java.nio.file.Files
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

// Interacting with java libraries makes null a necessity
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.AsInstanceOf"))
final case class TlsConfiguration(
    enabled: Boolean,
    certChainFile: Option[File] = None, // mutual auth is disabled if null
    privateKeyFile: Option[File] = None,
    trustCollectionFile: Option[File] = None, // System default if null
    secretsUrl: Option[SecretsUrl] = None,
    clientAuth: ClientAuth =
      ClientAuth.REQUIRE, // Client auth setting used by the server. This is not used in the client configuration.
    enableCertRevocationChecking: Boolean = false,
    minimumServerProtocolVersion: Option[TlsVersion] = None,
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** If enabled and all required fields are present, it returns an SslContext suitable for client usage */
  def client(enabledProtocols: Seq[TlsVersion] = Seq.empty): Option[SslContext] = {
    if (enabled) {
      val enabledProtocolsNames =
        if (enabledProtocols.isEmpty)
          null
        else
          enabledProtocols.map(_.version).asJava
      val sslContext = GrpcSslContexts
        .forClient()
        .keyManager(
          certChainFile.orNull,
          privateKeyFile.orNull,
        )
        .trustManager(trustCollectionFile.orNull)
        .protocols(enabledProtocolsNames)
        .sslProvider(SslContext.defaultClientProvider())
        .build()
      logTlsProtocolsAndCipherSuites(sslContext, isServer = false)
      Some(sslContext)
    } else None
  }

  /** If enabled and all required fields are present, it returns an SslContext suitable for server usage
    *
    *  Details:
    *  We create two instances of sslContext:
    *  1) The first one with default protocols: in order to query it for a set of supported protocols.
    *  2) The second one with a custom set of protocols to enable.
    *     We have used previously obtained set of supported protocols to make sure every protocol we want to enable is supported.
    *     @see [[javax.net.ssl.SSLEngine#setEnabledProtocols]]
    */
  def server: Option[SslContext] =
    if (enabled) {
      val tlsInfo = scala.util.Using.resources(
        keyCertChainInputStreamOrFail,
        keyInputStreamOrFail,
      ) { (keyCertChain: InputStream, key: InputStream) =>
        val defaultSslContext = buildServersSslContext(
          keyCertChain = keyCertChain,
          key = key,
          protocols = null.asInstanceOf[lang.Iterable[String]],
        )
        TlsInfo.fromSslContext(defaultSslContext)
      }

      scala.util.Using.resources(
        keyCertChainInputStreamOrFail,
        keyInputStreamOrFail,
      ) { (keyCertChain: InputStream, key: InputStream) =>
        val sslContext = buildServersSslContext(
          keyCertChain = keyCertChain,
          key = key,
          protocols = filterSupportedProtocols(tlsInfo),
        )
        logTlsProtocolsAndCipherSuites(sslContext, isServer = true)
        Some(sslContext)
      }

    } else {
      logger.info(s"Server's TLS: Disabled.")
      None
    }

  private def buildServersSslContext(
      keyCertChain: InputStream,
      key: InputStream,
      protocols: lang.Iterable[String],
  ) = {
    GrpcSslContexts
      .forServer(
        keyCertChain,
        key,
      )
      .trustManager(trustCollectionFile.orNull)
      .clientAuth(clientAuth)
      .protocols(protocols)
      .sslProvider(SslContext.defaultServerProvider())
      .build()
  }

  private[tls] def logTlsProtocolsAndCipherSuites(
      sslContext: SslContext,
      isServer: Boolean,
  ): Unit = {
    val (who, provider) =
      if (isServer)
        ("Server", SslContext.defaultServerProvider())
      else
        ("Client", SslContext.defaultClientProvider())
    val tlsInfo = TlsInfo.fromSslContext(sslContext)
    logger.info(s"$who TLS - enabled via $provider")
    logger.debug(
      s"$who TLS - supported protocols: ${filterSSLv2Hello(tlsInfo.supportedProtocols).mkString(", ")}."
    )
    logger.info(
      s"$who TLS - enabled protocols: ${filterSSLv2Hello(tlsInfo.enabledProtocols).mkString(", ")}."
    )
    logger.debug(
      s"$who TLS $who - supported cipher suites: ${tlsInfo.supportedCipherSuites.mkString(", ")}."
    )
    logger.info(s"$who TLS - enabled cipher suites: ${tlsInfo.enabledCipherSuites.mkString(", ")}.")
  }

  /** This is a side-effecting method. It modifies JVM TLS properties according to the TLS configuration. */
  def setJvmTlsProperties(): Unit =
    if (enabled) {
      if (enableCertRevocationChecking) OcspProperties.enableOcsp()
      ProtocolDisabler.disableSSLv2Hello()
    }

  /** Netty incorrectly hardcodes the report that the SSLv2Hello protocol is enabled. There is no way
    * to stop it from doing it, so we just filter the netty's erroneous claim. We also make sure that
    * the SSLv2Hello protocol is knocked out completely at the JSSE level through the ProtocolDisabler
    */
  private def filterSSLv2Hello(protocols: Seq[String]): Seq[String] =
    protocols.filter(_ != ProtocolDisabler.sslV2Protocol)

  private[tls] def filterSupportedProtocols(tlsInfo: TlsInfo): java.lang.Iterable[String] = {
    minimumServerProtocolVersion match {
      case None => null
      case Some(tlsVersion) =>
        val versions = tlsVersion match {
          case V1 | V1_1 =>
            throw new IllegalArgumentException(s"Unsupported TLS version: ${tlsVersion}")
          case V1_2 => Seq[TlsVersion](V1_2, V1_3)
          case V1_3 => Seq(V1_3)
          case _ =>
            throw new IllegalStateException(s"Could not recognize TLS version: |${tlsVersion}|!")
        }
        versions
          .map(_.version)
          .filter(tlsInfo.supportedProtocols.contains(_))
          .asJava
    }
  }

  private[tls] def keyInputStreamOrFail: InputStream = {
    val keyFileOrFail = privateKeyFile.getOrElse(
      throw new IllegalArgumentException(
        s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyFile."
      )
    )
    prepareKeyInputStream(keyFileOrFail)
  }

  private[tls] def prepareKeyInputStream(keyFile: File): InputStream = {
    val bytes = if (keyFile.getName.endsWith(".enc")) {
      try {
        val params = DecryptionParameters.fromSecretsServer(secretsUrlOrFail)
        params.decrypt(encrypted = keyFile)
      } catch {
        case NonFatal(e) => throw new PrivateKeyDecryptionException(e)
      }
    } else {
      Files.readAllBytes(keyFile.toPath)
    }
    new ByteArrayInputStream(bytes)
  }

  private def secretsUrlOrFail: SecretsUrl = secretsUrl.getOrElse(
    throw new IllegalStateException(
      s"Unable to convert ${this.toString} to SSL Context: cannot decrypt keyFile without secretsUrl."
    )
  )

  private def keyCertChainInputStreamOrFail: InputStream = {
    val msg =
      s"Unable to convert ${this.toString} to SSL Context: cannot create SSL context without keyCertChainFile."
    val keyFile = certChainFile.getOrElse(throw new IllegalStateException(msg))
    new FileInputStream(keyFile)
  }

}

object TlsConfiguration {
  val Empty: TlsConfiguration = TlsConfiguration(
    enabled = true,
    certChainFile = None,
    privateKeyFile = None,
    trustCollectionFile = None,
  )
}
