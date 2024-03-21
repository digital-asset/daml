// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{ClientConfig, KeepAliveClientConfig, TlsClientConfig}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.tracing.{NoTracing, TraceContextGrpc}
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.netty.handler.ssl.SslContext

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executor, TimeUnit}

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
      trustCertificate
        .fold(builder) { certChain =>
          val sslContext = withResource(certChain.newInput()) { inputStream =>
            GrpcSslContexts.forClient().trustManager(inputStream).build()
          }
          builder.sslContext(sslContext)
        }
        .discard
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

/** Supports creating GRPC channels but only supports a single host.
  * If multiple endpoints are provided a warning will be logged and the first supplied will be used.
  */
class CommunityClientChannelBuilder(protected val loggerFactory: NamedLoggerFactory)
    extends ClientChannelBuilder
    with NamedLogging
    with NoTracing {

  /** Create the initial netty channel builder before customizing settings */
  override protected def createNettyChannelBuilder(
      endpoints: NonEmpty[Seq[Endpoint]]
  ): NettyChannelBuilder = {
    val singleHost = endpoints.head1

    // warn that community does not support more than one domain connection if we've been passed multiple
    if (endpoints.size > 1) {
      logger.warn(
        s"Canton Community does not support using many connections for a domain. Defaulting to first: $singleHost"
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

  def sslContext(tls: TlsClientConfig): SslContext = {
    val builder = GrpcSslContexts
      .forClient()
    val trustBuilder = tls.trustCollectionFile.fold(builder)(trustCollection =>
      builder.trustManager(trustCollection.unwrap)
    )
    tls.clientCert
      .fold(trustBuilder)(cc => trustBuilder.keyManager(cc.certChainFile, cc.privateKeyFile))
      .build()
  }

  def configureKeepAlive(
      keepAlive: Option[KeepAliveClientConfig],
      builder: NettyChannelBuilder,
  ): NettyChannelBuilder = {
    keepAlive.fold(builder) { opt =>
      val time = opt.time.unwrap
      val timeout = opt.timeout.unwrap
      builder
        .keepAliveTime(time.toMillis, TimeUnit.MILLISECONDS)
        .keepAliveTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
  }

  /** Simple channel construction for test and console clients.
    * `maxInboundMessageSize` is 2GB; so don't use this to connect to an untrusted server.
    */
  def createChannelToTrustedServer(
      clientConfig: ClientConfig
  )(implicit executor: Executor): ManagedChannel = {
    val baseBuilder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(clientConfig.address, clientConfig.port.unwrap)
      .executor(executor)
      .maxInboundMessageSize(Int.MaxValue)

    // apply keep alive settings
    configureKeepAlive(
      clientConfig.keepAliveClient,
      // if tls isn't configured assume that it's a plaintext channel
      clientConfig.tls
        .fold(baseBuilder.usePlaintext()) { tls =>
          baseBuilder
            .useTransportSecurity()
            .sslContext(sslContext(tls))
        },
    ).build()
  }
}
