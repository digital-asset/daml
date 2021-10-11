// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.io.File
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.{TlsConfiguration, TlsVersion}
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.SandboxFixture
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TlsIT extends AsyncWordSpec with SandboxFixture with SuiteResourceManagementAroundAll {

  private val List(
    certChainFilePath,
    privateKeyFilePath,
    trustCertCollectionFilePath,
    clientCertChainFilePath,
    clientPrivateKeyFilePath,
  ) = {
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      new File(rlocation("ledger/test-common/test-certificates/" + src))
    }
  }

  private lazy val baseConfig: LedgerClientConfiguration =
    LedgerClientConfiguration(
      "appId",
      LedgerIdRequirement.none,
      CommandClientConfiguration.default,
      None,
    )

  private def tlsEnabledConfig(minimumProtocolVersion: TlsVersion): LedgerClientConfiguration =
    baseConfig.copy(sslContext =
      TlsConfiguration(
        enabled = true,
        Some(clientCertChainFilePath),
        Some(clientPrivateKeyFilePath),
        Some(trustCertCollectionFilePath),
        minimumServerProtocolVersion = Some(minimumProtocolVersion),
      ).client()
    )

  override protected lazy val config: SandboxConfig =
    super.config.copy(
      tlsConfig = Some(
        TlsConfiguration(
          enabled = true,
          Some(certChainFilePath),
          Some(privateKeyFilePath),
          Some(trustCertCollectionFilePath),
          minimumServerProtocolVersion = None,
        )
      )
    )

  private def clientF(protocol: TlsVersion) =
    LedgerClient.singleHost(serverHost, serverPort.value, tlsEnabledConfig(protocol))

  "A TLS-enabled server" should {
    "reject ledger queries when the client connects without tls" in {
      recoverToSucceededIf[io.grpc.StatusRuntimeException] {
        LedgerClient
          .singleHost(serverHost, serverPort.value, baseConfig)
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
