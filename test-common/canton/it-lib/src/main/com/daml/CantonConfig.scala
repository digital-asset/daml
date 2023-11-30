// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package integrationtest

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.{LedgerClient, GrpcChannel}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => LedgerClientWithoutId}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder

import scala.concurrent.{ExecutionContext, Future}
import java.nio.file.{Path, Paths}

object CantonConfig {

  case class Tls(
      serverCrt: Path,
      serverPem: Path,
      caCrt: Path,
      clientCrt: Path,
      clientPem: Path,
  ) {
    def clientConfig =
      TlsConfiguration(
        enabled = true,
        certChainFile = Some(clientCrt.toFile),
        privateKeyFile = Some(clientPem.toFile),
        trustCollectionFile = Some(caCrt.toFile),
      )
  }

  def noTlsConfig = TlsConfiguration(false)

}

final case class CantonConfig(
    jarPath: Path = CantonRunner.cantonPath,
    authSecret: Option[String] = None,
    devMode: Boolean = false,
    nParticipants: Int = 1,
    timeProviderType: TimeProviderType = TimeProviderType.WallClock,
    tlsEnable: Boolean = false,
    debug: Boolean = false,
    bootstrapScript: Option[String] = None,
    enableUpgrade: Boolean = false,
    targetScope: Option[String] = None,
) {

  lazy val tlsConfig =
    if (tlsEnable)
      Some(
        CantonConfig.Tls(
          Paths.get(rlocation("test-common/test-certificates/server.crt")),
          Paths.get(rlocation("test-common/test-certificates/server.pem")),
          Paths.get(rlocation("test-common/test-certificates/ca.crt")),
          Paths.get(rlocation("test-common/test-certificates/client.crt")),
          Paths.get(rlocation("test-common/test-certificates/client.pem")),
        )
      )
    else
      None

  lazy val participantIds =
    Iterator
      .range(0, nParticipants)
      .map(i => Ref.ParticipantId.assertFromString("participant" + i.toString))
      .toVector

  lazy val ledgerIds = participantIds.asInstanceOf[Vector[String]]

  def getToken(userId: Ref.IdString.UserId): Option[String] =
    CantonRunner.getToken(userId, authSecret, targetScope)

  lazy val adminToken: Option[String] = getToken(CantonRunner.adminUserId)

  def tlsClientConfig: TlsConfiguration = tlsConfig.fold(CantonConfig.noTlsConfig)(_.clientConfig)

  def channelBuilder(
      port: Port,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  ): NettyChannelBuilder = {
    import com.daml.ledger.client.configuration._
    LedgerClientChannelConfiguration(
      sslContext = tlsClientConfig.client(),
      maxInboundMessageSize = maxInboundMessageSize,
    ).builderFor("localhost", port.value)
  }

  def channel(
      port: Port,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  ): ManagedChannel =
    GrpcChannel.withShutdownHook(
      channelBuilder(port, maxInboundMessageSize)
    )

  def channelResource(
      port: Port,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  ): ResourceOwner[ManagedChannel] = {
    new GrpcChannel.Owner(
      channelBuilder(port, maxInboundMessageSize)
    )
  }

  // LedgerIds are deprecated, prefer ledgerClientWithoutId (#16831)
  def ledgerClient(
      port: Port,
      token: Option[String],
      applicationId: Option[Ref.ApplicationId],
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    import com.daml.ledger.client.configuration._
    LedgerClient(
      channel = channel(port, maxInboundMessageSize),
      config = LedgerClientConfiguration(
        applicationId = token.orElse(applicationId).getOrElse(""),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        token = token,
      ),
    )
  }

  // Prefer this whenever possible - ledgerIds should be avoided
  def ledgerClientWithoutId(
      port: Port,
      token: Option[String],
      applicationId: Option[Ref.ApplicationId],
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClientWithoutId = {
    import com.daml.ledger.client.configuration._
    LedgerClientWithoutId(
      channel = channel(port, maxInboundMessageSize),
      config = LedgerClientConfiguration(
        applicationId = token.orElse(applicationId).getOrElse(""),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        token = token,
      ),
    )
  }
}
