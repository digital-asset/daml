// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.SandboxFixture
import org.scalatest.AsyncWordSpec

import scala.language.implicitConversions

class TlsIT extends AsyncWordSpec with SandboxFixture with SuiteResourceManagementAroundAll {

  private val List(
    certChainFilePath,
    privateKeyFilePath,
    trustCertCollectionFilePath,
    clientCertChainFilePath,
    clientPrivateKeyFilePath) = {
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      new File(rlocation("ledger/test-common/test-certificates/" + src))
    }
  }

  private implicit def str2File(str: String): File = new File(str)

  private lazy val tlsEnabledConfig = LedgerClientConfiguration(
    "appId",
    LedgerIdRequirement("", enabled = false),
    CommandClientConfiguration.default,
    TlsConfiguration(
      enabled = true,
      Some(clientCertChainFilePath),
      Some(clientPrivateKeyFilePath),
      Some(trustCertCollectionFilePath)).client
  )

  override protected lazy val config: SandboxConfig =
    super.config.copy(
      tlsConfig = Some(
        TlsConfiguration(
          enabled = true,
          Some(certChainFilePath),
          Some(privateKeyFilePath),
          Some(trustCertCollectionFilePath))))

  private lazy val clientF = LedgerClient.singleHost(serverHost, serverPort.value, tlsEnabledConfig)

  "A TLS-enabled server" should {
    "reject ledger queries when the client connects without tls" in {
      recoverToSucceededIf[io.grpc.StatusRuntimeException] {
        LedgerClient
          .singleHost(serverHost, serverPort.value, tlsEnabledConfig.copy(sslContext = None))
          .flatMap(_.transactionClient.getLedgerEnd())
      }
    }

    "serve ledger queries when the client presents a valid certificate" in {
      clientF.flatMap(_.transactionClient.getLedgerEnd()).map(_ => succeed)
    }
  }
}
