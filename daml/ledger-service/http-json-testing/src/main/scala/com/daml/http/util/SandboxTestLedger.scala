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
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.sandbox.SandboxOnXForTest.{
  ApiServerConfig,
  Default,
  DevEngineConfig,
  singleParticipant,
}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.SandboxRequiringAuthorizationFuns
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import org.scalatest.Suite
import scalaz.@@

import scala.concurrent.{ExecutionContext, Future}

trait SandboxTestLedger extends SandboxFixture with SandboxRequiringAuthorizationFuns {
  self: Suite =>

  protected def testId: String

  def useTls: UseTls

  def ledgerId: String @@ domain.LedgerIdTag = LedgerId(testId)

  override protected def config = Default.copy(
    ledgerId = testId,
    engine = DevEngineConfig,
    participants = singleParticipant(
      ApiServerConfig.copy(
        seeding = Seeding.Weak,
        timeProviderType = TimeProviderType.WallClock,
        tls = if (useTls) Some(serverTlsConfig) else None,
      )
    ),
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

    val client: DamlLedgerClient = DamlLedgerClient.singleHost(
      "localhost",
      serverPort.value,
      clientCfg(token, testName),
      clientChannelCfg,
    )(ec, esf)

    val fa: Future[A] = for {
      ledgerPort <- Future(serverPort)
      a <- testFn(ledgerPort, client, ledgerId)
    } yield a

    fa
  }
}
