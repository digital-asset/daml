// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import io.netty.handler.ssl.SslContext

/** The public server configuration ServerConfig used by the domain.
  *
  * TODO(i4056): Client authentication over TLS is currently unsupported,
  *  because there is a token based protocol to authenticate clients. This may change in the future.
  */
trait PublicServerConfig extends ServerConfig {

  def tls: Option[TlsBaseServerConfig]

  /** Expiration time for a nonce that is generated for an
    * authentication challenge. as an authentication request is
    * expected to be followed up with almost immediately to generate
    * an authentication token the nonce expiry should be short. the
    * nonce is automatically invalided on use.
    */
  def nonceExpirationTime: NonNegativeFiniteDuration

  /** Expiration time for authentication tokens. Tokens are used to authenticate participants.
    * Choose a shorter time for better security and a longer time for better performance.
    */
  def tokenExpirationTime: NonNegativeFiniteDuration

  lazy val clientConfig: ClientConfig =
    ClientConfig(address, port, tls.map(c => TlsClientConfig(Some(c.certChainFile), None)))

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.baseSslContext)

  override def serverCertChainFile: Option[ExistingFile] = tls.map(_.certChainFile)

  /** overrides the default maximum request size in bytes on the sequencer node */
  def overrideMaxRequestSize: Option[NonNegativeInt]

  /** This setting has no effect. Therfore hardcoding it to 0.
    */
  override final def maxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(0)
  def connection: String = {
    val scheme = tls.fold("http")(_ => "https")
    s"$scheme://$address:$port"
  }
}

final case class CommunityPublicServerConfig(
    override val address: String = "127.0.0.1",
    override val internalPort: Option[Port] = None,
    override val tls: Option[TlsBaseServerConfig] = None,
    override val keepAliveServer: Option[KeepAliveServerConfig] = Some(KeepAliveServerConfig()),
    override val nonceExpirationTime: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMinutes(1),
    override val tokenExpirationTime: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofHours(1),
    override val overrideMaxRequestSize: Option[NonNegativeInt] = None,
) extends PublicServerConfig
    with CommunityServerConfig
