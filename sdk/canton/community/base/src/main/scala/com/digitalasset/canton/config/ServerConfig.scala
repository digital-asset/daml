// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.*
import cats.syntax.traverse.*
import com.daml.metrics.api.MetricHandle.MetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.daml.nonempty.NonEmpty
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.AdminServerConfig.defaultAddress
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port}
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.ledger.api.tls.TlsVersion
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{
  CantonCommunityServerInterceptors,
  CantonServerBuilder,
  CantonServerInterceptors,
}
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.{ProtoDeserializationError, SequencerAlias}
import io.netty.handler.ssl.{ClientAuth, SslContext}
import org.slf4j.LoggerFactory

import java.io.File
import scala.annotation.nowarn
import scala.math.Ordering.Implicits.infixOrderingOps

/** Configuration for hosting a server api */
trait ServerConfig extends Product with Serializable {

  /** The address of the interface to be listening on */
  val address: String

  /** Port to be listening on (must be greater than 0). If the port is None, a default port will be assigned on startup.
    *
    * NOTE: If you rename this field, adapt the corresponding product hint for config reading. In the configuration the
    * field is still called `port` for usability reasons.
    */
  protected val internalPort: Option[Port]

  /** Returns the configured or the default port that must be assigned after config loading and before config usage.
    *
    * We split between `port` and `internalPort` to offer a clean API to users of the config in the form of `port`,
    * which must always return a configured or default port, and the internal representation that may be None before
    * being assigned a default port.
    */
  def port: Port =
    internalPort.getOrElse(
      throw new IllegalStateException("Accessing server port before default was set")
    )

  /** If defined, dictates to use TLS when connecting to this node through the given `address` and `port`.
    * Server authentication is always enabled.
    * Subclasses may decide whether to support client authentication.
    */
  def sslContext: Option[SslContext]

  /** If any defined, enforces token based authorization when accessing this node through the given `address` and `port`.
    */
  def authServices: Seq[AuthServiceConfig]

  /** If defined, the admin-token based authoriztion will be supported when accessing this node through the given `address` and `port`.
    */
  def adminToken: Option[String]

  /** server cert chain file if TLS is defined
    *
    * Used for domain internal GRPC sequencer connections
    */
  def serverCertChainFile: Option[ExistingFile]

  /** server keep alive settings */
  def keepAliveServer: Option[KeepAliveServerConfig]

  /** maximum inbound message size in bytes on the ledger api and the admin api */
  def maxInboundMessageSize: NonNegativeInt

  /** Use the configuration to instantiate the interceptors for this server */
  def instantiateServerInterceptors(
      tracingConfig: TracingConfig,
      apiLoggingConfig: ApiLoggingConfig,
      metricsPrefix: MetricName,
      @nowarn("cat=deprecation") metrics: MetricsFactory,
      loggerFactory: NamedLoggerFactory,
      grpcMetrics: GrpcServerMetrics,
      authServices: Seq[AuthServiceConfig],
      adminToken: Option[CantonAdminToken],
      telemetry: Telemetry,
  ): CantonServerInterceptors

}

trait CommunityServerConfig extends ServerConfig {
  override def instantiateServerInterceptors(
      tracingConfig: TracingConfig,
      apiLoggingConfig: ApiLoggingConfig,
      metricsPrefix: MetricName,
      @nowarn("cat=deprecation") metrics: MetricsFactory,
      loggerFactory: NamedLoggerFactory,
      grpcMetrics: GrpcServerMetrics,
      authServices: Seq[AuthServiceConfig],
      adminToken: Option[CantonAdminToken],
      telemetry: Telemetry,
  ) = new CantonCommunityServerInterceptors(
    tracingConfig,
    apiLoggingConfig,
    loggerFactory,
    grpcMetrics,
    authServices,
    adminToken,
    telemetry,
  )
}

object ServerConfig {
  val defaultMaxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(10 * 1024 * 1024)
}

/** A variant of [[ServerConfig]] that by default listens to connections only on the loopback interface.
  */
trait AdminServerConfig extends ServerConfig {

  override val address: String = defaultAddress

  def tls: Option[TlsServerConfig]

  def clientConfig: ClientConfig =
    ClientConfig(
      address,
      port,
      tls = tls.map(_.clientConfig),
      keepAliveClient = keepAliveServer.map(_.clientConfigFor),
    )

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.sslContext)

  override def serverCertChainFile: Option[ExistingFile] = tls.map(_.certChainFile)
}
object AdminServerConfig {
  val defaultAddress: String = "127.0.0.1"
}

final case class CommunityAdminServerConfig(
    override val address: String = defaultAddress,
    internalPort: Option[Port] = None,
    tls: Option[TlsServerConfig] = None,
    keepAliveServer: Option[BasicKeepAliveServerConfig] = Some(BasicKeepAliveServerConfig()),
    maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
    authServices: Seq[AuthServiceConfig] = Seq.empty,
    adminToken: Option[String] = None,
) extends AdminServerConfig
    with CommunityServerConfig

/** GRPC keep alive server configuration. */
trait KeepAliveServerConfig {

  /** time sets the time without read activity before sending a keepalive ping. Do not set to small numbers (default is 40s)
    * Corresponds to [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def time: NonNegativeFiniteDuration

  /** timeout sets the time waiting for read activity after sending a keepalive ping (default is 20s)
    * Corresponds to [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#keepAliveTimeout-long-java.util.concurrent.TimeUnit-]]
    */
  def timeout: NonNegativeFiniteDuration

  /** permitKeepAliveTime sets the most aggressive keep-alive time that clients are permitted to configure (default is 20s)
    * Corresponds to [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#permitKeepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def permitKeepAliveTime: NonNegativeFiniteDuration

  /** permitKeepAliveWithoutCalls allows the clients to send keep alive signals outside any ongoing grpc subscription (default false)
    * Corresponds to [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#permitKeepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def permitKeepAliveWithoutCalls: Boolean

  /** A sensible default choice of client config for the given server config */
  def clientConfigFor: KeepAliveClientConfig = {
    val clientKeepAliveTime = permitKeepAliveTime.max(time)
    KeepAliveClientConfig(clientKeepAliveTime, timeout)
  }
}

final case class BasicKeepAliveServerConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(40),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
    permitKeepAliveTime: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
    permitKeepAliveWithoutCalls: Boolean = false,
) extends KeepAliveServerConfig

final case class LedgerApiKeepAliveServerConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(10),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
    permitKeepAliveTime: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
    permitKeepAliveWithoutCalls: Boolean = true,
) extends KeepAliveServerConfig

/** GRPC keep alive client configuration
  *
  * Settings according to [[https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-]]
  *
  * @param time Sets the time without read activity before sending a keepalive ping. Do not set to small numbers (default is 40s)
  * @param timeout Sets the time waiting for read activity after sending a keepalive ping (default is 20s)
  */
final case class KeepAliveClientConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(40),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
)

sealed trait ApiType extends PrettyPrinting {
  def toProtoEnum: v0.SequencerApiType
}

object ApiType {
  case object Grpc extends ApiType {
    def toProtoEnum: v0.SequencerApiType = v0.SequencerApiType.Grpc
    override def pretty: Pretty[Grpc.type] = prettyOfObject[Grpc.type]
  }

  def fromProtoEnum(
      field: String,
      apiTypeP: v0.SequencerApiType,
  ): ParsingResult[ApiType] =
    apiTypeP match {
      case v0.SequencerApiType.Grpc => Right(Grpc)
      case v0.SequencerApiType.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
    }
}

trait GrpcClientConfig {
  def address: String
  def port: Port
  def tlsConfig: Option[TlsClientConfig]
  def toGrpcSequencerConnection: Either[String, GrpcSequencerConnection] =
    for {
      pem <- tlsConfig
        .flatMap(_.trustCollectionFile)
        .traverse(file => X509CertificatePem.fromFile(file.unwrap.toScala))
    } yield GrpcSequencerConnection(
      endpoints = NonEmpty(Seq, Endpoint(address, port)),
      transportSecurity = tlsConfig.exists(_.enabled),
      customTrustCertificates = pem.map(_.unwrap),
      sequencerAlias = SequencerAlias.Default,
    )
}

/** A client configuration to a corresponding server configuration */
final case class ClientConfig(
    override val address: String = "127.0.0.1",
    override val port: Port,
    tls: Option[TlsClientConfig] = None,
    keepAliveClient: Option[KeepAliveClientConfig] = Some(KeepAliveClientConfig()),
) extends GrpcClientConfig {
  override def tlsConfig: Option[TlsClientConfig] = tls
}

/** A sequencer API client configuration, which unlike a generic gRPC client config
  * does not support TLS client certificates because the sequencer API uses auth tokens.
  */
final case class SequencerApiClientConfig(
    override val address: String,
    override val port: Port,
    tls: Option[TlsClientConfigOnlyTrustFile] = None,
) extends GrpcClientConfig {
  override def tlsConfig: Option[TlsClientConfig] = tls.map(_.toTlsClientConfig)
}

object SequencerApiClientConfig {
  def fromClientConfig(clientConfig: ClientConfig): SequencerApiClientConfig =
    SequencerApiClientConfig(
      clientConfig.address,
      clientConfig.port,
      clientConfig.tlsConfig.map(_.withoutClientCert),
    )
}

sealed trait BaseTlsArguments {
  def certChainFile: ExistingFile
  def privateKeyFile: ExistingFile
  def minimumServerProtocolVersion: Option[String]
  def ciphers: Option[Seq[String]]

  def protocols: Option[Seq[String]] =
    minimumServerProtocolVersion.map { minVersion =>
      val knownTlsVersions =
        Seq(
          TlsVersion.V1.version,
          TlsVersion.V1_1.version,
          TlsVersion.V1_2.version,
          TlsVersion.V1_3.version,
        )
      knownTlsVersions
        .find(_ == minVersion)
        .fold[Seq[String]](
          throw new IllegalArgumentException(s"Unknown TLS protocol version $minVersion")
        )(versionFound => knownTlsVersions.filter(_ >= versionFound))
    }
}

/** A wrapper for TLS related server parameters supporting mutual authentication.
  *
  * Certificates and keys must be provided in the PEM format.
  * It is recommended to create them with OpenSSL.
  * Other formats (such as GPG) may also work, but have not been tested.
  *
  * @param certChainFile a file containing a certificate chain,
  *                            containing the certificate chain from the server to the root CA.
  *                            The certificate chain is used to authenticate the server.
  *                            The order of certificates in the chain matters, i.e., it must start with the
  *                            server certificate and end with the root certificate.
  * @param privateKeyFile a file containing the server's private key.
  *                             The key must not use a password.
  * @param trustCollectionFile a file containing certificates of all nodes the server trusts.
  *                            Used for client authentication.
  *                            It depends on the enclosing configuration whether client authentication is mandatory,
  *                            optional or unsupported.
  *                            If client authentication is enabled and this parameter is absent,
  *                            the certificates in the JVM trust store will be used instead.
  * @param secretsUrl URL of a secrets service that provide parameters needed to decrypt the private key.
  *                   Required when private key is encrypted (indicated by '.enc' filename suffix).
  * @param clientAuth indicates whether server requires, requests, does does not request auth from clients.
  *                   Normally the ledger api server requires client auth under TLS, but using this setting this
  *                   requirement can be loosened.
  *                   See https://github.com/digital-asset/daml/commit/edd73384c427d9afe63bae9d03baa2a26f7b7f54
  * @param minimumServerProtocolVersion minimum supported TLS protocol. Set None (or null in config file) to default to JVM settings.
  * @param ciphers   supported ciphers. Set to None (or null in config file) to default to JVM settings.
  * @param enableCertRevocationChecking whether to enable certificate revocation checking per
  *                                     https://tersesystems.com/blog/2014/03/22/fixing-certificate-revocation/
  *                                     TODO(#4881): implement cert-revocation at the participant and domain admin endpoints
  *                                     Ledger api server reference PR: https://github.com/digital-asset/daml/pull/7965
  */
// Information in this ScalaDoc comment has been taken from https://grpc.io/docs/guides/auth/.
final case class TlsServerConfig(
    certChainFile: ExistingFile,
    privateKeyFile: ExistingFile,
    trustCollectionFile: Option[ExistingFile] = None,
    secretsUrl: Option[String] = None,
    clientAuth: ServerAuthRequirementConfig = ServerAuthRequirementConfig.Optional,
    minimumServerProtocolVersion: Option[String] = Some(
      TlsServerConfig.defaultMinimumServerProtocol
    ),
    ciphers: Option[Seq[String]] = TlsServerConfig.defaultCiphers,
    enableCertRevocationChecking: Boolean = false,
) extends BaseTlsArguments {
  lazy val clientConfig: TlsClientConfig = {
    val clientCert = clientAuth match {
      case ServerAuthRequirementConfig.Require(cert) => Some(cert)
      case _ => None
    }
    TlsClientConfig(trustCollectionFile = Some(certChainFile), clientCert = clientCert)
  }
}

object TlsServerConfig {
  // default OWASP strong cipher set with broad compatibility (B list)
  // https://cheatsheetseries.owasp.org/cheatsheets/TLS_Cipher_String_Cheat_Sheet.html
  lazy val defaultCiphers = {
    val candidates = Seq(
      "TLS_AES_256_GCM_SHA384",
      "TLS_CHACHA20_POLY1305_SHA256",
      "TLS_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
      "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    )
    val logger = LoggerFactory.getLogger(TlsServerConfig.getClass)
    val filtered = candidates.filter { x =>
      io.netty.handler.ssl.OpenSsl.availableOpenSslCipherSuites().contains(x) ||
      io.netty.handler.ssl.OpenSsl.availableJavaCipherSuites().contains(x)
    }
    if (filtered.isEmpty) {
      val len = io.netty.handler.ssl.OpenSsl
        .availableOpenSslCipherSuites()
        .size() + io.netty.handler.ssl.OpenSsl
        .availableJavaCipherSuites()
        .size()
      logger.warn(
        s"All of Canton's default TLS ciphers are unsupported by your JVM (netty reports $len ciphers). Defaulting to JVM settings."
      )
      if (!io.netty.handler.ssl.OpenSsl.isAvailable) {
        logger.info(
          "Netty OpenSSL is not available because of an issue",
          io.netty.handler.ssl.OpenSsl.unavailabilityCause(),
        )
      }
      None
    } else {
      logger.debug(
        s"Using ${filtered.length} out of ${candidates.length} Canton's default TLS ciphers"
      )
      Some(filtered)
    }
  }

  val defaultMinimumServerProtocol = "TLSv1.2"

}

/** A wrapper for TLS server parameters supporting only server side authentication
  *
  * Same parameters as the more complete `TlsServerConfig`
  */
final case class TlsBaseServerConfig(
    certChainFile: ExistingFile,
    privateKeyFile: ExistingFile,
    minimumServerProtocolVersion: Option[String] = Some(
      TlsServerConfig.defaultMinimumServerProtocol
    ),
    ciphers: Option[Seq[String]] = TlsServerConfig.defaultCiphers,
) extends BaseTlsArguments

/** A wrapper for TLS related client configurations
  *
  * @param trustCollectionFile a file containing certificates of all nodes the client trusts. If none is specified, defaults to the JVM trust store
  * @param clientCert the client certificate
  * @param enabled allows enabling TLS without `trustCollectionFile` or `clientCert`
  */
final case class TlsClientConfig(
    trustCollectionFile: Option[ExistingFile],
    clientCert: Option[TlsClientCertificate],
    enabled: Boolean = true,
) {
  def withoutClientCert: TlsClientConfigOnlyTrustFile = TlsClientConfigOnlyTrustFile(
    trustCollectionFile = trustCollectionFile,
    enabled = enabled,
  )
}

/** A wrapper for TLS related client configurations without client auth support (currently public sequencer api)
  *
  * @param trustCollectionFile a file containing certificates of all nodes the client trusts. If none is specified, defaults to the JVM trust store
  * @param enabled allows enabling TLS without `trustCollecionFile` or `clientCert`
  */
final case class TlsClientConfigOnlyTrustFile(
    trustCollectionFile: Option[ExistingFile],
    enabled: Boolean = true,
) {
  def toTlsClientConfig: TlsClientConfig = TlsClientConfig(
    trustCollectionFile = trustCollectionFile,
    clientCert = None,
    enabled = enabled,
  )
}

/**
  */
final case class TlsClientCertificate(certChainFile: File, privateKeyFile: File)

/** Configuration on whether server requires auth, requests auth, or no auth */
sealed trait ServerAuthRequirementConfig {
  def clientAuth: ClientAuth
}
object ServerAuthRequirementConfig {

  /** A variant of [[ServerAuthRequirementConfig]] by which the server requires auth from clients */
  final case class Require(adminClient: TlsClientCertificate) extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.REQUIRE
  }

  /** A variant of [[ServerAuthRequirementConfig]] by which the server merely requests auth from clients */
  case object Optional extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.OPTIONAL
  }

  /** A variant of [[ServerAuthRequirementConfig]] by which the server does not even request auth from clients */
  case object None extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.NONE
  }
}
