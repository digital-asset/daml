// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package integrationtest

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.integrationtest.CantonConfig.noTlsConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import scalaz.syntax.tag._

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
    applicationId: ApplicationId,
    darFiles: List[Path] = List.empty,
    authSecret: Option[String] = None,
    devMode: Boolean = false,
    nParticipants: Int = 1,
    timeProviderType: TimeProviderType = TimeProviderType.WallClock,
    tlsEnable: Boolean = false,
    debug: Boolean = false,
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
    CantonRunner.getToken(userId, authSecret)

  lazy val adminToken: Option[String] = getToken(CantonRunner.adminUserId)

  def tlsClientConfig: TlsConfiguration = tlsConfig.fold(noTlsConfig)(_.clientConfig)

  def ledgerClient(
      port: Port,
      token: Option[String],
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    import com.daml.ledger.client.configuration._
    LedgerClient.singleHost(
      hostIp = "localhost",
      port = port.value,
      configuration = LedgerClientConfiguration(
        applicationId = token.fold(applicationId.unwrap)(_ => ""),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        token = token,
      ),
      channelConfig = LedgerClientChannelConfiguration(
        sslContext = tlsClientConfig.client(),
        maxInboundMessageSize = maxInboundMessageSize,
      ),
    )
  }
}
