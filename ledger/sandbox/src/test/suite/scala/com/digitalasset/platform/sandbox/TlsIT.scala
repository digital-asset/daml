// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.nio.file.{Files, Path}

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  Resource,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.{SandboxFixture, SandboxServerResource}
import io.grpc.Channel
import org.scalatest.AsyncWordSpec
import org.apache.commons.io.FileUtils

import scala.language.implicitConversions

class TlsIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with TestExecutionSequencerFactory
    with SandboxFixture
    with SuiteResourceManagementAroundAll {

  private def extractCerts: Path = {
    val dir = Files.createTempDirectory("TlsIT").toFile
    dir.deleteOnExit()
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").foreach { src =>
      val target = new File(dir, src)
      target.deleteOnExit()
      val stream = getClass.getClassLoader.getResourceAsStream("certificates/" + src)
      FileUtils.copyInputStreamToFile(stream, target)
    }
    dir.toPath
  }

  private lazy val certificatesPath = extractCerts
  private lazy val certificatesDirPrefix: String = certificatesPath.toString + File.separator

  private lazy val certChainFilePath = certificatesDirPrefix + "server.crt"
  private lazy val privateKeyFilePath = certificatesDirPrefix + "server.pem"
  private lazy val trustCertCollectionFilePath = certificatesDirPrefix + "ca.crt"
  private lazy val clientCertChainFilePath = certificatesDirPrefix + "client.crt"
  private lazy val clientPrivateKeyFilePath = certificatesDirPrefix + "client.pem"

  private implicit def str2File(str: String) = new File(str)

  private lazy val tlsEnabledConfig = LedgerClientConfiguration(
    "appId",
    LedgerIdRequirement("", false),
    CommandClientConfiguration.default,
    TlsConfiguration(
      true,
      Some(clientCertChainFilePath),
      Some(clientPrivateKeyFilePath),
      Some(trustCertCollectionFilePath)).client
  )

  override protected lazy val config: SandboxConfig =
    super.config.copy(
      tlsConfig = Some(
        TlsConfiguration(
          true,
          Some(certChainFilePath),
          Some(privateKeyFilePath),
          Some(trustCertCollectionFilePath))))

  private lazy val sandboxServer = SandboxServer(config)

  private lazy val clientF = LedgerClient.singleHost(
    "localhost",
    sandboxServer.port,
    tlsEnabledConfig
  )

  override protected lazy val suiteResource: Resource[Channel] =
    new SandboxServerResource(config)

  "A TLS-enabled server" should {

    "reject ledger queries when the client connects without tls" in {
      recoverToSucceededIf[io.grpc.StatusRuntimeException] {
        LedgerClient
          .singleHost(
            "localhost",
            sandboxServer.port,
            tlsEnabledConfig.copy(sslContext = None)
          )
          .flatMap { c =>
            c.transactionClient.getLedgerEnd
          }
      }
    }
    "serve ledger queries when the client presents a valid certificate" in {
      clientF
        .flatMap { c =>
          c.transactionClient.getLedgerEnd
        }
        .map(_ => succeed)
    }
  }
}
