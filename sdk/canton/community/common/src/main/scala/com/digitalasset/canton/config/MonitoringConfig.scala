// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.metrics.api.MetricHandle.MetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.{
  CantonCommunityServerInterceptors,
  CantonServerInterceptors,
}
import com.digitalasset.canton.tracing.TracingConfig
import io.netty.handler.ssl.SslContext

import scala.annotation.nowarn

/** Configuration of the gRPC health server for a canton node.
  * @param parallelism number of threads to be used in the gRPC server
  */
final case class GrpcHealthServerConfig(
    override val address: String = "0.0.0.0",
    override val internalPort: Option[Port] = None,
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = Some(
      BasicKeepAliveServerConfig()
    ),
    parallelism: Int = 4,
) extends ServerConfig {
  override def authServices: Seq[AuthServiceConfig] = Seq.empty
  override def adminToken: Option[String] = None
  override val sslContext: Option[SslContext] = None
  override val serverCertChainFile: Option[RequireTypes.ExistingFile] = None
  override def maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize
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
  ): CantonServerInterceptors =
    new CantonCommunityServerInterceptors(
      tracingConfig,
      apiLoggingConfig,
      loggerFactory,
      grpcMetrics,
      authServices,
      adminToken,
      telemetry,
    )

  def toRemoteConfig: ClientConfig =
    ClientConfig(address, port, keepAliveClient = keepAliveServer.map(_.clientConfigFor))
}

/** Monitoring configuration for a canton node.
  * @param grpcHealthServer Optional gRPC Health server configuration
  */
final case class NodeMonitoringConfig(grpcHealthServer: Option[GrpcHealthServerConfig] = None)
