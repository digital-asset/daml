// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.HttpServiceTestFixture.{UseTls, clientTlsConfig, serverTlsConfig}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ports.Port
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.runner.common.Config.{
  SandboxEngineConfig,
  SandboxParticipantConfig,
  SandboxParticipantId,
}
import com.daml.ledger.sandbox.{BridgeConfig, NewSandboxServer}
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite
import scalaz.@@

import scala.concurrent.{ExecutionContext, Future}

trait SandboxTestLedger extends SandboxFixture {
  self: Suite =>

  protected def testId: String

  def useTls: UseTls

  def ledgerId: String @@ domain.LedgerIdTag = LedgerId(testId)

  override protected def config: NewSandboxServer.CustomConfig = NewSandboxServer.CustomConfig(
    genericConfig = com.daml.ledger.runner.common.Config.SandboxDefault.copy(
      ledgerId = testId,
      engine = SandboxEngineConfig.copy(
        allowedLanguageVersions = LanguageVersion.DevVersions
      ),
      participants = Map(
        SandboxParticipantId -> SandboxParticipantConfig.copy(apiServer =
          SandboxParticipantConfig.apiServer.copy(
            seeding = Seeding.Weak,
            timeProviderType = TimeProviderType.WallClock,
            tls = if (useTls) Some(serverTlsConfig) else None,
          )
        )
      ),
    ),
    bridgeConfig = BridgeConfig(),
    damlPackages = packageFiles,
    authServiceFromConfig = authService,
  )

  def clientCfg(token: Option[String], testName: String): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = testName,
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = token,
    )

  private val clientChannelCfg: LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration(
      sslContext = if (useTls) clientTlsConfig.client() else None
    )

  def usingLedger[A](testName: String, token: Option[String] = None)(
      testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]
  )(implicit
      esf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[A] = {

    val clientF: Future[DamlLedgerClient] = for {
      ledgerPort <- Future(serverPort)
    } yield DamlLedgerClient.singleHost(
      "localhost",
      ledgerPort.value,
      clientCfg(token, testName),
      clientChannelCfg,
    )(ec, esf)

    val fa: Future[A] = for {
      ledgerPort <- Future(serverPort)
      client <- clientF
      a <- testFn(ledgerPort, client, ledgerId)
    } yield a

    fa
  }
}
