// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.nonempty.NonEmpty
import com.daml.tls.TlsVersion.TlsVersion
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.TlsServerConfig.logTlsProtocolsAndCipherSuites
import com.digitalasset.canton.config.{ClientConfig, KeepAliveClientConfig, TlsClientConfig}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.tracing.TraceContextGrpc
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.google.protobuf.ByteString
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.netty.handler.ssl.{SslContext, SslContextBuilder}

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executor, TimeUnit}
import scala.jdk.CollectionConverters.*

/** Construct a GRPC channel to be used by a client within canton. */
trait ClientChannelBuilder {
  def create(
      endpoints: NonEmpty[Seq[Endpoint]],
      useTls: Boolean,
      executor: Executor,
      trustCertificate: Option[ByteString] = None,
      traceContextPropagation: Propagation = Propagation.Disabled,
      maxInboundMessageSize: Option[NonNegativeInt] = None,
      keepAliveClient: Option[KeepAliveClientConfig] = None,
  ): NettyChannelBuilder = {
    // the bulk of this channel builder is the same between community and enterprise
    // we only extract the bits that are different into calls to the protected implementation specific methods

    // the builder calls mutate this instance so is fine to assign to a val
    val builder = createNettyChannelBuilder(endpoints)
    additionalChannelBuilderSettings(builder, endpoints)

    builder.executor(executor)
    maxInboundMessageSize.foreach(s => builder.maxInboundMessageSize(s.unwrap))
    ClientChannelBuilder.configureKeepAlive(keepAliveClient, builder).discard
    if (traceContextPropagation == Propagation.Enabled)
      builder.intercept(TraceContextGrpc.clientInterceptor).discard

    if (useTls) {
      builder
        .useTransportSecurity() // this is strictly unnecessary as is the default for the channel builder, but can't hurt either

      // add certificates if provided
      trustCertificate.foreach { certChain =>
        val sslContext = withResource(certChain.newInput()) { inputStream =>
          GrpcSslContexts.forClient().trustManager(inputStream).build()
        }
        builder.sslContext(sslContext)
      }
    } else
      builder.usePlaintext().discard

    builder
  }

  /** Create the initial netty channel builder before customizing settings */
  protected def createNettyChannelBuilder(endpoints: NonEmpty[Seq[Endpoint]]): NettyChannelBuilder

  /** Set implementation specific channel settings */
  protected def additionalChannelBuilderSettings(
      builder: NettyChannelBuilder,
      endpoints: NonEmpty[Seq[Endpoint]],
  ): Unit = ()
}

trait ClientChannelBuilderFactory extends (NamedLoggerFactory => ClientChannelBuilder)

/** Supports creating GRPC channels but only supports a single host. If multiple endpoints are
  * provided a warning will be logged and the first supplied will be used.
  */
class CommunityClientChannelBuilder(protected val loggerFactory: NamedLoggerFactory)
    extends ClientChannelBuilder
    with NamedLogging {

  /** Create the initial netty channel builder before customizing settings */
  override protected def createNettyChannelBuilder(
      endpoints: NonEmpty[Seq[Endpoint]]
  ): NettyChannelBuilder = {
    val singleHost = endpoints.head1

    // warn that community does not support more than one synchronizer connection if we've been passed multiple
    if (endpoints.sizeIs > 1) {
      noTracingLogger.warn(
        s"Canton Community does not support using many connections for a synchronizer. Defaulting to first: $singleHost"
      )
    }

    NettyChannelBuilder.forAddress(singleHost.host, singleHost.port.unwrap)
  }
}

object CommunityClientChannelBuilder extends ClientChannelBuilderFactory {
  override def apply(loggerFactory: NamedLoggerFactory): ClientChannelBuilder =
    new CommunityClientChannelBuilder(loggerFactory)
}

object ClientChannelBuilder {
  // basic service locator to prevent having to pass these instances around everywhere
  private lazy val factoryRef =
    new AtomicReference[ClientChannelBuilderFactory](CommunityClientChannelBuilder)
  def apply(loggerFactory: NamedLoggerFactory): ClientChannelBuilder =
    factoryRef.get()(loggerFactory)
  private[canton] def setFactory(factory: ClientChannelBuilderFactory): Unit =
    factoryRef.set(factory)

  private def sslContextBuilder(tls: TlsClientConfig): SslContextBuilder = {
    val builder = GrpcSslContexts
      .forClient()
    val trustBuilder = tls.trustCollectionFile.fold(builder)(trustCollection =>
      builder.trustManager(trustCollection.pemStream)
    )
    tls.clientCert
      .fold(trustBuilder)(cc =>
        trustBuilder.keyManager(cc.certChainFile.pemStream, cc.privateKeyFile.pemStream)
      )
  }

  def sslContext(
      tls: TlsClientConfig,
      logTlsProtocolAndCipherSuites: Boolean = false,
  ): SslContext = {
    val sslContext = sslContextBuilder(tls).build()
    if (logTlsProtocolAndCipherSuites)
      logTlsProtocolsAndCipherSuites(sslContext, isServer = false)
    sslContext
  }

  def sslContext(
      tls: TlsClientConfig,
      enabledProtocols: Seq[TlsVersion],
  ): SslContext =
    sslContextBuilder(tls)
      .protocols(enabledProtocols.map(_.version).asJava)
      .build()

  def configureKeepAlive(
      keepAlive: Option[KeepAliveClientConfig],
      builder: NettyChannelBuilder,
  ): NettyChannelBuilder =
    keepAlive.fold(builder) { opt =>
      val time = opt.time.unwrap
      val timeout = opt.timeout.unwrap
      builder
        .keepAliveTime(time.toMillis, TimeUnit.MILLISECONDS)
        .keepAliveTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
    }

  /** Simple channel construction for test and console clients. `maxInboundMessageSize` is 2GB; so
    * don't use this to connect to an untrusted server.
    */
  def createChannelBuilderToTrustedServer(
      clientConfig: ClientConfig
  )(implicit executor: Executor): ManagedChannelBuilderProxy =
    createChannelBuilder(clientConfig, maxInboundMessageSize = Some(Int.MaxValue))

  def createChannelBuilder(
      clientConfig: ClientConfig,
      maxInboundMessageSize: Option[Int] = None,
  )(implicit executor: Executor): ManagedChannelBuilderProxy = {
    val nettyChannelBuilder =
      NettyChannelBuilder
        .forAddress(clientConfig.address, clientConfig.port.unwrap)
        .executor(executor)

    val baseBuilder =
      maxInboundMessageSize
        .map(nettyChannelBuilder.maxInboundMessageSize)
        .getOrElse(nettyChannelBuilder)

    // apply keep alive settings
    val builder =
      clientConfig.tlsConfig
        // if tls isn't configured assume that it's a plaintext channel
        .fold(baseBuilder.usePlaintext()) { tls =>
          if (tls.enabled)
            baseBuilder
              .useTransportSecurity()
              .sslContext(sslContext(tls))
          else
            baseBuilder.usePlaintext()
        }

    ManagedChannelBuilderProxy(configureKeepAlive(clientConfig.keepAliveClient, builder))
  }
}
