// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.fixture

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.withoutledgerid.LedgerClient
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ConfigAdaptor, ParticipantId}
import com.daml.ledger.sandbox.{BridgeConfigAdaptor, SandboxOnXForTest, SandboxOnXRunner}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.{AbstractSandboxFixture, SandboxRequiringAuthorizationFuns}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.ports.Port
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.duration._

trait SandboxFixture
    extends AbstractSandboxFixture
    with SuiteResource[(Port, Channel)]
    with SandboxRequiringAuthorizationFuns {
  self: Suite =>

  override protected def serverPort: Port = suiteResource.value._1

  override protected def channel: Channel = suiteResource.value._2

  private def adminLedgerClient(port: Port, config: Config): LedgerClient = {
    val sslContext = config.participants.head._2.apiServer.tls.flatMap(_.client())
    val clientConfig = LedgerClientConfiguration(
      applicationId = "admin-client",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = Some(toHeader(adminTokenStandardJWT)),
    )
    LedgerClient.singleHost(
      hostIp = "localhost",
      port = port.value,
      configuration = clientConfig,
      channelConfig = LedgerClientChannelConfiguration(sslContext),
    )(
      system.dispatcher,
      executionSequencerFactory,
    )
  }

  override protected lazy val suiteResource: Resource[(Port, Channel)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(
            _.map(info => Some(info.jdbcUrl))
          )

        participantDataSource = jdbcUrl match {
          case Some(url) => Map(ParticipantId -> ParticipantDataSourceConfig(url))
          case None =>
            Map(
              ParticipantId -> ParticipantDataSourceConfig(
                SandboxOnXForTest.defaultH2SandboxJdbcUrl()
              )
            )
        }

        cfg = config.copy(
          dataSource = participantDataSource
        )
        configAdaptor: BridgeConfigAdaptor = new ConfigAdaptor(
          authService
        )
        port <- SandboxOnXRunner.owner(configAdaptor, cfg, bridgeConfig)
        channel <- GrpcClientResource.owner(port)
        client = adminLedgerClient(port, cfg)
        _ <- ResourceOwner.forFuture(() => uploadDarFiles(client, packageFiles)(system.dispatcher))
        _ = println(s"Voila ${packageFiles.mkString}")
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
