// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{TlsConfiguration, TlsVersion}
import com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest.wordspec.AsyncWordSpec
import java.io.File

import scala.concurrent.Future

class TlsIT extends AsyncWordSpec with SandboxFixture with SuiteResourceManagementAroundAll {

  private def getFilePath(fileName: String) = new File(
    rlocation("ledger/test-common/test-certificates/" + fileName)
  )
  lazy private val certChainFilePath = getFilePath("server.crt")
  lazy private val privateKeyFilePath = getFilePath("server.pem")
  lazy private val trustCertCollectionFilePath = getFilePath("ca.crt")
  lazy private val clientCertChainFilePath = getFilePath("client.crt")
  lazy private val clientPrivateKeyFilePath = getFilePath("client.pem")

  private lazy val baseConfig: LedgerClientConfiguration =
    LedgerClientConfiguration(
      "appId",
      LedgerIdRequirement.none,
      CommandClientConfiguration.default,
    )

  private def tlsEnabledConfig(
      minimumProtocolVersion: TlsVersion
  ): LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration(
      TlsConfiguration(
        enabled = true,
        Some(clientCertChainFilePath),
        Some(clientPrivateKeyFilePath),
        Some(trustCertCollectionFilePath),
        minimumServerProtocolVersion = Some(minimumProtocolVersion),
      ).client()
    )

  override def config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        tls = Some(
          TlsConfiguration(
            enabled = true,
            Some(certChainFilePath),
            Some(privateKeyFilePath),
            Some(trustCertCollectionFilePath),
            minimumServerProtocolVersion = None,
          )
        )
      )
    )
  )

  private def clientF(protocol: TlsVersion) =
    LedgerClient.singleHost(serverHost, serverPort.value, baseConfig, tlsEnabledConfig(protocol))

  "A TLS-enabled server" should {
    "reject ledger queries when the client connects without tls" in {
      recoverToSucceededIf[io.grpc.StatusRuntimeException] {
        LedgerClient
          .insecureSingleHost(serverHost, serverPort.value, baseConfig)
          .flatMap(_.transactionClient.getLedgerEnd())
      }
    }

    "serve ledger queries when the client presents a valid certificate" in {
      def testWith(protocol: TlsVersion): Future[GetLedgerEndResponse] =
        withClue(s"Testing with $protocol") {
          clientF(protocol).flatMap(_.transactionClient.getLedgerEnd())
        }

      for {
        _ <- testWith(TlsVersion.V1_1)
        _ <- testWith(TlsVersion.V1_3)
      } yield succeed
    }
  }
}
