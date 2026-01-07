// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

import scala.concurrent.duration.Duration

/** Configuration of the gRPC health server for a canton node.
  * @param parallelism
  *   number of threads to be used in the gRPC server
  */
final case class GrpcHealthServerConfig(
    override val address: String = "0.0.0.0",
    override val internalPort: Option[Port] = None,
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = Some(
      BasicKeepAliveServerConfig()
    ),
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    parallelism: Int = 4,
) extends ServerConfig {
  override val name: String = "grpc-health"
  override def authServices: Seq[AuthServiceConfig] = Seq.empty
  override def adminTokenConfig: AdminTokenConfig = AdminTokenConfig()
  override val sslContext: Option[SslContext] = None
  override val serverCertChainFile: Option[PemFileOrString] = None
  override def maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize
  override val maxTokenLifetime: NonNegativeDuration = NonNegativeDuration(Duration.Inf)
  override val jwksCacheConfig: JwksCacheConfig = JwksCacheConfig()
  override def limits: Option[ActiveRequestLimitsConfig] = None
  def toRemoteConfig: FullClientConfig =
    FullClientConfig(address, port, keepAliveClient = keepAliveServer.map(_.clientConfigFor))
}

/** Configuration of health server backend. */
final case class HttpHealthServerConfig(address: String = "0.0.0.0", port: Port)

/** Monitoring configuration for a canton node.
  * @param grpcHealthServer
  *   Optional gRPC Health server configuration
  * @param httpHealthServer
  *   Optional HTTP Health server configuration
  */
final case class NodeMonitoringConfig(
    grpcHealthServer: Option[GrpcHealthServerConfig] = None,
    httpHealthServer: Option[HttpHealthServerConfig] = None,
)
