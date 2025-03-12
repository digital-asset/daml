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

import java.util.concurrent.{Executor, TimeUnit}
import scala.jdk.CollectionConverters.*

/** Construct a GRPC channel to be used by a client within canton. */
class ClientChannelBuilder private (protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  /** Create the initial netty channel builder before customizing settings */
  private def createNettyChannelBuilder(
      endpoints: NonEmpty[Seq[Endpoint]]
  ): NettyChannelBuilder =
    // only use our multi-host name resolver if using multiple hosts
    endpoints.forgetNE match {
      case Seq(singleHost) =>
        NettyChannelBuilder.forAddress(singleHost.host, singleHost.port.unwrap)
      case _multipleHosts =>
        // setup a multi-host config
        val uri = MultiHostNameResolverProvider.setupEndpointConfig(endpoints)
        NettyChannelBuilder.forTarget(uri)
    }

  /** Set implementation specific channel settings */
  private def additionalChannelBuilderSettings(
      builder: NettyChannelBuilder
  ): Unit = {
    import scala.jdk.CollectionConverters.*
    builder.defaultLoadBalancingPolicy("round_robin")
    // enable health checking as a basis for round robin failover
    builder.defaultServiceConfig(
      Map(
        "healthCheckConfig" -> Map(
          "serviceName" -> CantonGrpcUtil.sequencerHealthCheckServiceName
        ).asJava
      ).asJava
    )
    ()
  }

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
    additionalChannelBuilderSettings(builder)

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

}

object ClientChannelBuilder {

  // setup enterprise GRPC client channels that supports load-balancing
  MultiHostNameResolverProvider.register()

  def apply(loggerFactory: NamedLoggerFactory): ClientChannelBuilder =
    // Create through the companion object to ensure we register the multi-host name resolver
    new ClientChannelBuilder(loggerFactory)

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
