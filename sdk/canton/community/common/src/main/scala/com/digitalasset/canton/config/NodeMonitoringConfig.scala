// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import io.netty.handler.ssl.SslContext

/** Configuration of the gRPC health server for a canton node.
  * @param parallelism number of threads to be used in the gRPC server
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
  override def authServices: Seq[AuthServiceConfig] = Seq.empty
  override def adminToken: Option[String] = None
  override val sslContext: Option[SslContext] = None
  override val serverCertChainFile: Option[RequireTypes.ExistingFile] = None
  override def maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize

  def toRemoteConfig: ClientConfig =
    ClientConfig(address, port, keepAliveClient = keepAliveServer.map(_.clientConfigFor))
}

/** Configuration of health server backend. */
final case class HttpHealthServerConfig(address: String = "0.0.0.0", port: Port)

/** Monitoring configuration for a canton node.
  * @param grpcHealthServer Optional gRPC Health server configuration
  * @param httpHealthServer Optional HTTP Health server configuration
  */
final case class NodeMonitoringConfig(
    grpcHealthServer: Option[GrpcHealthServerConfig] = None,
    httpHealthServer: Option[HttpHealthServerConfig] = None,
)
