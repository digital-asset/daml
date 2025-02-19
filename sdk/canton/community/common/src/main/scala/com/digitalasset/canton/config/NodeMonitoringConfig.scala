// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
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
) extends ServerConfig
    with UniformCantonConfigValidation {
  override def authServices: Seq[AuthServiceConfig] = Seq.empty
  override def adminToken: Option[String] = None
  override val sslContext: Option[SslContext] = None
  override val serverCertChainFile: Option[PemFileOrString] = None
  override def maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize

  def toRemoteConfig: FullClientConfig =
    FullClientConfig(address, port, keepAliveClient = keepAliveServer.map(_.clientConfigFor))
}
object GrpcHealthServerConfig {
  implicit val grpcHealthServerConfigCanontConfigValidator
      : CantonConfigValidator[GrpcHealthServerConfig] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[GrpcHealthServerConfig]
  }
}

/** Configuration of health server backend. */
final case class HttpHealthServerConfig(address: String = "0.0.0.0", port: Port)
    extends UniformCantonConfigValidation

object HttpHealthServerConfig {
  implicit val httpHealthServerConfigCanontConfigValidator
      : CantonConfigValidator[HttpHealthServerConfig] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[HttpHealthServerConfig]
  }
}

/** Monitoring configuration for a canton node.
  * @param grpcHealthServer Optional gRPC Health server configuration
  * @param httpHealthServer Optional HTTP Health server configuration
  */
final case class NodeMonitoringConfig(
    grpcHealthServer: Option[GrpcHealthServerConfig] = None,
    httpHealthServer: Option[HttpHealthServerConfig] = None,
) extends UniformCantonConfigValidation

object NodeMonitoringConfig {
  implicit val nodeMonitoringConfigCanontConfigValidator
      : CantonConfigValidator[NodeMonitoringConfig] =
    CantonConfigValidatorDerivation[NodeMonitoringConfig]
}
