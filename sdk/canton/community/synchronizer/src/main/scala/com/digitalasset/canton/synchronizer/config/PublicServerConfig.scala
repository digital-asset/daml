// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.config

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import io.netty.handler.ssl.SslContext

/** The public server configuration ServerConfig used by the domain.
  *
  * @param nonceExpirationInterval Expiration interval for a nonce that is generated for an
  *        authentication challenge. As an authentication request is
  *        expected to be followed up with almost immediately to generate
  *        an authentication token the nonce expiry should be short. The
  *        nonce is automatically invalided on use.
  * @param maxTokenExpirationInterval
  *        Expiration time interval for authentication tokens. Tokens are used to authenticate participants.
  *        Choose a shorter interval for better security and a longer interval for better performance.
  * @param useExponentialRandomTokenExpiration
  *        If enabled, the token expiration interval will be exponentially distributed with the following parameters:
  *        - `scale` of `0.75 * maxTokenExpirationInterval`;
  *        - value is re-sampled to fit into the interval [maxTokenExpirationInterval / 2, maxTokenExpirationInterval].
  *        This is useful to avoid the thundering herd problem when many tokens expire at the same time and should
  *        result in nearly uniform distribution of token expiration intervals.
  *        If disabled, the token expiration interval will be constant.
  * @param overrideMaxRequestSize
  *        overrides the default maximum request size in bytes on the sequencer node
  */
// TODO(i4056): Client authentication over TLS is currently unsupported,
//  because there is a token based protocol to authenticate clients. This may change in the future.
final case class PublicServerConfig(
    override val address: String = "127.0.0.1",
    override val internalPort: Option[Port] = None,
    tls: Option[TlsBaseServerConfig] = None,
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = Some(
      BasicKeepAliveServerConfig()
    ),
    nonceExpirationInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
    maxTokenExpirationInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(1),
    useExponentialRandomTokenExpiration: Boolean = false,
    overrideMaxRequestSize: Option[NonNegativeInt] = None,
) extends ServerConfig {

  override def authServices: Seq[AuthServiceConfig] = Seq.empty

  override def jwtTimestampLeeway: Option[JwtTimestampLeeway] = None

  override def adminToken: Option[String] = None

  lazy val clientConfig: ClientConfig =
    ClientConfig(address, port, tls.map(c => TlsClientConfig(Some(c.certChainFile), None)))

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.baseSslContext)

  override def serverCertChainFile: Option[ExistingFile] = tls.map(_.certChainFile)

  /** This setting has no effect. Therfore hardcoding it to 0.
    */
  override final def maxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(0)
  def connection: String = {
    val scheme = tls.fold("http")(_ => "https")
    s"$scheme://$address:$port"
  }
}
