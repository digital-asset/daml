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
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite
import scalaz.@@

import scala.concurrent.{ExecutionContext, Future}

trait SandboxTestLedger extends SandboxNextFixture {
  self: Suite =>

  protected def testId: String

  def useTls: UseTls

  def ledgerId: String @@ domain.LedgerIdTag = LedgerId(testId)

  override protected def config: SandboxConfig = SandboxConfig.defaultConfig.copy(
    port = Port.Dynamic,
    damlPackages = packageFiles,
    timeProviderType = Some(TimeProviderType.WallClock),
    tlsConfig = if (useTls) Some(serverTlsConfig) else None,
    ledgerIdMode = LedgerIdMode.Static(ledgerId),
    authService = authService,
    scenario = scenario,
    engineMode = SandboxConfig.EngineMode.Dev,
    seeding = Seeding.Weak,
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
