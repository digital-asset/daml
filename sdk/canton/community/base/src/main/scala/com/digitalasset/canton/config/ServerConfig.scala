// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.jwt.JwtTimestampLeeway
import com.daml.nonempty.NonEmpty
import com.daml.tls.{OcspProperties, ProtocolDisabler, TlsInfo, TlsVersion}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.config.AdminServerConfig.defaultAddress
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.ActiveRequestsMetrics.GrpcServerMetricsX
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{
  CantonCommunityServerInterceptors,
  CantonServerBuilder,
  CantonServerInterceptors,
}
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TracingConfig
import io.grpc.ServerInterceptor
import io.grpc.netty.shaded.io.netty.handler.ssl.{ClientAuth, SslContext}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.math.Ordering.Implicits.infixOrderingOps

/** Configuration to limit the number of open requests per service
  *
  * @param active
  *   map of service name to maximum number of parallel active requests or streams
  * @param warnOnUndefinedLimits
  *   emit warning if a limit is not configured for a stream
  * @param throttleLoggingRatePerSecond
  *   maximum rate for logging rejections for requests exceeding the limit (prevents DOS on logs)
  */
final case class ActiveRequestLimitsConfig(
    active: Map[String, NonNegativeInt] = Map.empty,
    warnOnUndefinedLimits: Boolean = false,
    throttleLoggingRatePerSecond: NonNegativeInt = NonNegativeInt.tryCreate(10),
)

/** Configuration for hosting a server api */
trait ServerConfig extends Product with Serializable {

  /** The name of the api */
  def name: String

  /** The address of the interface to be listening on */
  val address: String

  /** Port to be listening on (must be greater than 0). If the port is None, a default port will be
    * assigned on startup.
    *
    * NOTE: If you rename this field, adapt the corresponding product hint for config reading. In
    * the configuration the field is still called `port` for usability reasons.
    */
  protected val internalPort: Option[Port]

  /** Returns the configured or the default port that must be assigned after config loading and
    * before config usage.
    *
    * We split between `port` and `internalPort` to offer a clean API to users of the config in the
    * form of `port`, which must always return a configured or default port, and the internal
    * representation that may be None before being assigned a default port.
    */
  def port: Port =
    internalPort.getOrElse(
      throw new IllegalStateException("Accessing server port before default was set")
    )

  /** If defined, dictates to use TLS when connecting to this node through the given `address` and
    * `port`. Server authentication is always enabled. Subclasses may decide whether to support
    * client authentication.
    */
  def sslContext: Option[SslContext]

  /** If any defined, enforces token based authorization when accessing this node through the given
    * `address` and `port`.
    */
  def authServices: Seq[AuthServiceConfig]

  /** Leeway parameters for the jwt processing algorithms used in the authorization services
    */
  def jwtTimestampLeeway: Option[JwtTimestampLeeway]

  /** The configuration of the admin-token based authorization that will be supported when accessing
    * this node through the given `address` and `port`.
    */
  def adminTokenConfig: AdminTokenConfig

  /** server cert chain file if TLS is defined
    *
    * Used for synchronizer internal GRPC sequencer connections
    */
  def serverCertChainFile: Option[PemFileOrString]

  /** server keep alive settings */
  def keepAliveServer: Option[KeepAliveServerConfig]

  /** maximum inbound message size in bytes on the ledger api and the admin api */
  def maxInboundMessageSize: NonNegativeInt

  /** maximum expiration time accepted for tokens */
  def maxTokenLifetime: NonNegativeDuration

  /** settings for the jwks cache */
  def jwksCacheConfig: JwksCacheConfig

  /** configure limits for open streams per service */
  def limits: Option[ActiveRequestLimitsConfig]

  /** Use the configuration to instantiate the interceptors for this server */
  def instantiateServerInterceptors(
      api: String,
      tracingConfig: TracingConfig,
      apiLoggingConfig: ApiLoggingConfig,
      loggerFactory: NamedLoggerFactory,
      grpcMetrics: GrpcServerMetricsX,
      authServices: Seq[AuthServiceConfig],
      adminTokenDispenser: Option[CantonAdminTokenDispenser],
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      adminTokenConfig: AdminTokenConfig,
      jwksCacheConfig: JwksCacheConfig,
      telemetry: Telemetry,
      additionalInterceptors: Seq[ServerInterceptor] = Seq.empty,
      requestLimits: Option[ActiveRequestLimitsConfig],
  ): CantonServerInterceptors = new CantonCommunityServerInterceptors(
    api,
    tracingConfig,
    apiLoggingConfig,
    loggerFactory,
    grpcMetrics,
    authServices,
    adminTokenDispenser,
    jwtTimestampLeeway,
    adminTokenConfig,
    jwksCacheConfig,
    telemetry,
    additionalInterceptors,
    requestLimits,
  )

}

object ServerConfig {
  val defaultMaxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(10 * 1024 * 1024)
  val defaultMaxInboundMetadataSize: NonNegativeInt = NonNegativeInt.tryCreate(8 * 1024)
}

/** A variant of [[ServerConfig]] that by default listens to connections only on the loopback
  * interface.
  */
final case class AdminServerConfig(
    override val address: String = defaultAddress,
    override val internalPort: Option[Port] = None,
    tls: Option[TlsServerConfig] = None,
    override val jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = Some(
      BasicKeepAliveServerConfig()
    ),
    override val maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
    override val authServices: Seq[AuthServiceConfig] = Seq.empty,
    override val adminTokenConfig: AdminTokenConfig = AdminTokenConfig(),
    override val maxTokenLifetime: NonNegativeDuration = NonNegativeDuration(5.minutes),
    override val jwksCacheConfig: JwksCacheConfig = JwksCacheConfig(),
    override val limits: Option[ActiveRequestLimitsConfig] = None,
) extends ServerConfig {
  override val name: String = "admin"
  def clientConfig: FullClientConfig =
    FullClientConfig(
      address,
      port,
      tls = tls.map(_.clientConfig),
      keepAliveClient = keepAliveServer.map(_.clientConfigFor),
    )

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.sslContext(_))

  override def serverCertChainFile: Option[PemFileOrString] = tls.map(_.certChainFile)
}
object AdminServerConfig {
  val defaultAddress: String = "127.0.0.1"
}

/** GRPC keep alive server configuration. */
trait KeepAliveServerConfig {

  /** time sets the time without read activity before sending a keepalive ping. Do not set to small
    * numbers (default is 40s) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def time: NonNegativeFiniteDuration

  /** timeout sets the time waiting for read activity after sending a keepalive ping (default is
    * 20s) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#keepAliveTimeout-long-java.util.concurrent.TimeUnit-]]
    */
  def timeout: NonNegativeFiniteDuration

  /** permitKeepAliveTime sets the most aggressive keep-alive time that clients are permitted to
    * configure (default is 20s) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#permitKeepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def permitKeepAliveTime: NonNegativeFiniteDuration

  /** permitKeepAliveWithoutCalls allows the clients to send keep alive signals outside any ongoing
    * grpc subscription (default false) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#permitKeepAliveTime-long-java.util.concurrent.TimeUnit-]]
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
    permitKeepAliveWithoutCalls: Boolean = false,
) extends KeepAliveServerConfig

/** gRPC keep alive client configuration
  *
  * Settings according to
  * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-]]
  *
  * @param time
  *   Sets the time without read activity before sending a keepalive ping. Do not set to small
  *   numbers (default is 40s)
  * @param timeout
  *   Sets the time waiting for read activity after sending a keepalive ping (default is 15s).
  *
  * The default timeout was previously 20s and was lowered to 15s, because besides configuring the
  * gRPC KeepAliveManager, it also sets up the socket's TCP_USER_TIMEOUT (see
  * [[https://man7.org/linux/man-pages/man7/tcp.7.html]]). 15s gives a larger margin to detect a
  * faulty connection earlier and retry a submission on another sequencer via amplification, thereby
  * avoiding a request failure.
  */
final case class KeepAliveClientConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(40),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(15),
)

/** Base trait for Grpc transport client configuration classes, abstracts access to the configs */
trait ClientConfig {
  def address: String
  def port: Port
  def tlsConfig: Option[TlsClientConfig]
  def keepAliveClient: Option[KeepAliveClientConfig]
  def endpointAsString: String = address + ":" + port.unwrap
}

/** This trait is only there to force presence of `tls` field in the config classes */
trait TlsField[A] {
  def tls: Option[A]
}

/** A full feature complete client configuration to a corresponding server configuration, the class
  * is aimed to be used in configs
  */
final case class FullClientConfig(
    override val address: String = "127.0.0.1",
    override val port: Port,
    override val tls: Option[TlsClientConfig] = None,
    override val keepAliveClient: Option[KeepAliveClientConfig] = Some(KeepAliveClientConfig()),
) extends ClientConfig
    with TlsField[TlsClientConfig] {
  override def tlsConfig: Option[TlsClientConfig] = tls
}

/** A client configuration for a public server configuration, the class is aimed to be used in
  * configs
  */
final case class SequencerApiClientConfig(
    override val address: String,
    override val port: Port,
    override val tls: Option[TlsClientConfigOnlyTrustFile] = None,
) extends ClientConfig
    with TlsField[TlsClientConfigOnlyTrustFile] {

  override def keepAliveClient: Option[KeepAliveClientConfig] = None

  override def tlsConfig: Option[TlsClientConfig] = tls.map(_.toTlsClientConfig)

  def asSequencerConnection(
      sequencerAlias: SequencerAlias,
      sequencerId: Option[SequencerId],
  ): GrpcSequencerConnection = {
    val endpoint = Endpoint(address, port)
    GrpcSequencerConnection(
      NonEmpty(Seq, endpoint),
      tls.exists(_.enabled),
      tls.flatMap(_.trustCollectionFile).map(_.pemBytes),
      sequencerAlias,
      sequencerId = sequencerId,
    )
  }
}

sealed trait BaseTlsArguments {
  def certChainFile: PemFileOrString
  def privateKeyFile: PemFile
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
  * Certificates and keys must be provided in the PEM format. It is recommended to create them with
  * OpenSSL. Other formats (such as GPG) may also work, but have not been tested.
  *
  * @param certChainFile
  *   a file containing a certificate chain, containing the certificate chain from the server to the
  *   root CA. The certificate chain is used to authenticate the server. The order of certificates
  *   in the chain matters, i.e., it must start with the server certificate and end with the root
  *   certificate.
  * @param privateKeyFile
  *   a file containing the server's private key. The key must not use a password.
  * @param trustCollectionFile
  *   a file containing certificates of all nodes the server trusts. Used for client authentication.
  *   It depends on the enclosing configuration whether client authentication is mandatory, optional
  *   or unsupported. If client authentication is enabled and this parameter is absent, the
  *   certificates in the JVM trust store will be used instead.
  * @param clientAuth
  *   indicates whether server requires, requests, or does not request auth from clients. Normally
  *   the ledger api server requires client auth under TLS, but using this setting this requirement
  *   can be loosened. See
  *   https://github.com/digital-asset/daml/commit/edd73384c427d9afe63bae9d03baa2a26f7b7f54
  * @param minimumServerProtocolVersion
  *   minimum supported TLS protocol. Set None (or null in config file) to default to JVM settings.
  * @param ciphers
  *   supported ciphers. Set to None (or null in config file) to default to JVM settings.
  * @param enableCertRevocationChecking
  *   whether to enable certificate revocation checking per
  *   https://tersesystems.com/blog/2014/03/22/fixing-certificate-revocation/
  */
// Information in this ScalaDoc comment has been taken from https://grpc.io/docs/guides/auth/.
final case class TlsServerConfig(
    certChainFile: PemFileOrString,
    privateKeyFile: PemFile,
    trustCollectionFile: Option[PemFileOrString] = None,
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

  /** This is a side-effecting method. It modifies JVM TLS properties according to the TLS
    * configuration.
    */
  def setJvmTlsProperties(): Unit = {
    if (enableCertRevocationChecking) OcspProperties.enableOcsp()
    ProtocolDisabler.disableSSLv2Hello()
  }

  override def protocols: Option[Seq[String]] = {
    val disallowedTlsVersions =
      Seq(
        TlsVersion.V1.version,
        TlsVersion.V1_1.version,
      )
    minimumServerProtocolVersion match {
      case Some(minVersion) if disallowedTlsVersions.contains(minVersion) =>
        throw new IllegalArgumentException(s"Unsupported TLS version: $minVersion")
      case _ =>
        super.protocols
    }
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
      io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl
        .availableOpenSslCipherSuites()
        .contains(x) ||
      io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.availableJavaCipherSuites().contains(x)
    }
    if (filtered.isEmpty) {
      val len = io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl
        .availableOpenSslCipherSuites()
        .size() + io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl
        .availableJavaCipherSuites()
        .size()
      logger.warn(
        s"All of Canton's default TLS ciphers are unsupported by your JVM (netty reports $len ciphers). Defaulting to JVM settings."
      )
      if (!io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.isAvailable) {
        logger.info(
          "Netty OpenSSL is not available because of an issue",
          io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.unavailabilityCause(),
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

  /** Netty incorrectly hardcodes the report that the SSLv2Hello protocol is enabled. There is no
    * way to stop it from doing it, so we just filter the netty's erroneous claim. We also make sure
    * that the SSLv2Hello protocol is knocked out completely at the JSSE level through the
    * ProtocolDisabler
    */
  private def filterSSLv2Hello(protocols: Seq[String]): Seq[String] =
    protocols.filter(_ != ProtocolDisabler.sslV2Protocol)

  def logTlsProtocolsAndCipherSuites(
      sslContext: SslContext,
      isServer: Boolean,
  ): Unit = {
    val (who, provider, logger) =
      if (isServer)
        (
          "Server",
          SslContext.defaultServerProvider(),
          LoggerFactory.getLogger(TlsServerConfig.getClass),
        )
      else
        (
          "Client",
          SslContext.defaultClientProvider(),
          LoggerFactory.getLogger(TlsClientConfig.getClass),
        )

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

}

/** A wrapper for TLS server parameters supporting only server side authentication
  *
  * Same parameters as the more complete `TlsServerConfig`
  */
final case class TlsBaseServerConfig(
    certChainFile: PemFileOrString,
    privateKeyFile: PemFile,
    minimumServerProtocolVersion: Option[String] = Some(
      TlsServerConfig.defaultMinimumServerProtocol
    ),
    ciphers: Option[Seq[String]] = TlsServerConfig.defaultCiphers,
) extends BaseTlsArguments

/** A wrapper for TLS related client configurations
  *
  * @param trustCollectionFile
  *   a file containing certificates of all nodes the client trusts. If none is specified, defaults
  *   to the JVM trust store
  * @param clientCert
  *   the client certificate
  * @param enabled
  *   allows enabling TLS without `trustCollectionFile` or `clientCert`
  */
final case class TlsClientConfig(
    trustCollectionFile: Option[PemFileOrString],
    clientCert: Option[TlsClientCertificate],
    enabled: Boolean = true,
) {
  def withoutClientCert: TlsClientConfigOnlyTrustFile =
    TlsClientConfigOnlyTrustFile(
      trustCollectionFile = trustCollectionFile,
      enabled = enabled,
    )
}

/** A wrapper for TLS related client configurations without client auth support (currently public
  * sequencer api)
  *
  * @param trustCollectionFile
  *   a file containing certificates of all nodes the client trusts. If none is specified, defaults
  *   to the JVM trust store
  * @param enabled
  *   allows enabling TLS without `trustCollectionFile`
  */
final case class TlsClientConfigOnlyTrustFile(
    trustCollectionFile: Option[PemFileOrString],
    enabled: Boolean = true,
) {
  def toTlsClientConfig: TlsClientConfig = TlsClientConfig(
    trustCollectionFile = trustCollectionFile,
    clientCert = None,
    enabled = enabled,
  )
}

final case class TlsClientCertificate(certChainFile: PemFileOrString, privateKeyFile: PemFile)

/** Configuration on whether server requires auth, requests auth, or no auth */
sealed trait ServerAuthRequirementConfig {
  def clientAuth: ClientAuth
}
object ServerAuthRequirementConfig {

  /** A variant of [[ServerAuthRequirementConfig]] by which the server requires auth from clients */
  final case class Require(adminClient: TlsClientCertificate) extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.REQUIRE
  }

  /** A variant of [[ServerAuthRequirementConfig]] by which the server merely requests auth from
    * clients
    */
  case object Optional extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.OPTIONAL
  }

  /** A variant of [[ServerAuthRequirementConfig]] by which the server does not even request auth
    * from clients
    */
  case object None extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.NONE
  }
}

/** Configuration for jwks cache underpinning JWT token validation.
  */
final case class JwksCacheConfig(
    cacheMaxSize: Long = JwksCacheConfig.DefaultCacheMaxSize,
    cacheExpiration: NonNegativeFiniteDuration = JwksCacheConfig.DefaultCacheExpiration,
    connectionTimeout: NonNegativeFiniteDuration = JwksCacheConfig.DefaultConnectionTimeout,
    readTimeout: NonNegativeFiniteDuration = JwksCacheConfig.DefaultReadTimeout,
)

object JwksCacheConfig {
  private val DefaultCacheMaxSize: Long = 1000
  private val DefaultCacheExpiration: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(5)
  private val DefaultConnectionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)
  private val DefaultReadTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)
}

/** Configuration for admin-token based authorization.
  *
  * The given token will be valid forever but it will not be used internally. Other admin-tokens
  * will be generated and rotated periodically. The fixed token is only used for testing purposes
  * and should not be used in production.
  *
  * The admin-token based authorization will create tokens and rotate them periodically. Each
  * admin-token is valid for the defined token duration. The half of this value is used as the
  * rotation interval, after which a new admin-token is generated (if needed).
  */
final case class AdminTokenConfig(
    fixedAdminToken: Option[String] = None,
    adminTokenDuration: PositiveFiniteDuration = AdminTokenConfig.DefaultAdminTokenDuration,
    actAsAnyPartyClaim: Boolean = false,
    adminClaim: Boolean = false,
) {

  def merge(other: AdminTokenConfig): AdminTokenConfig =
    AdminTokenConfig(
      fixedAdminToken = fixedAdminToken.orElse(other.fixedAdminToken),
      adminTokenDuration = adminTokenDuration.min(other.adminTokenDuration),
      actAsAnyPartyClaim = actAsAnyPartyClaim || other.actAsAnyPartyClaim,
      adminClaim = adminClaim || other.adminClaim,
    )
}

object AdminTokenConfig {

  val DefaultAdminTokenDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(5)
}
