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
import com.daml.ledger.sandbox.SandboxOnXForTest.{
  SandboxDefault,
  SandboxEngineConfig,
  SandboxParticipantConfig,
  SandboxParticipantId,
}
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.SandboxRequiringAuthorizationFuns
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import com.google.protobuf.ByteString
import org.scalatest.Suite
import scalaz.@@

import java.io.FileInputStream
import scala.concurrent.{ExecutionContext, Future}

trait SandboxTestLedger extends SandboxFixture with SandboxRequiringAuthorizationFuns {
  self: Suite =>

  protected def testId: String

  def useTls: UseTls

  def ledgerId: String @@ domain.LedgerIdTag = LedgerId(testId)

  override protected def config = SandboxDefault.copy(
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

    val adminClient: DamlLedgerClient = DamlLedgerClient.singleHost(
      "localhost",
      serverPort.value,
      clientCfg(Some(toHeader(adminTokenStandardJWT)), testName),
      clientChannelCfg,
    )(ec, esf)

    val fa: Future[A] = for {
      ledgerPort <- Future(serverPort)
      _ <- Future.sequence(packageFiles.map { dar =>
        adminClient.packageManagementClient.uploadDarFile(
          ByteString.readFrom(new FileInputStream(dar))
        )
      })
      a <- testFn(ledgerPort, client, ledgerId)
    } yield a

    fa
  }
}
