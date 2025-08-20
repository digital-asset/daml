// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{
  CantonConfig,
  DbConfig,
  PemFile,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.util.JarResourceUtils
import monocle.macros.syntax.lens.*
import org.scalatest.RecoverMethods.recoverToSucceededIf

class TlsIT extends CantonFixture {

  registerPlugin(TLSPlugin(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private def getPemFile(fileName: String): PemFile =
    PemFile(ExistingFile.tryCreate(JarResourceUtils.resourceFile("test-certificates/" + fileName)))

  lazy private val certChainFile = getPemFile("server.crt")
  lazy private val privateKeyFile = getPemFile("server.pem")
  lazy private val trustCertCollectionFile = getPemFile("ca.crt")
  lazy private val clientCertChainFile = getPemFile("client.crt")
  lazy private val clientPrivateKeyFile = getPemFile("client.pem")

  private lazy val baseConfig: LedgerClientConfiguration =
    LedgerClientConfiguration(
      "userId",
      CommandClientConfiguration.default,
    )

  private val tlsEnabledConfig: LedgerClientChannelConfiguration = {
    val tlsConfiguration = TlsClientConfig(
      trustCollectionFile = Some(trustCertCollectionFile),
      clientCert = Some(
        TlsClientCertificate(
          certChainFile = clientCertChainFile,
          privateKeyFile = clientPrivateKeyFile,
        )
      ),
    )
    val sslContext =
      ClientChannelBuilder.sslContext(tlsConfiguration)
    LedgerClientChannelConfiguration(Some(sslContext))
  }

  private def clientF(serverPort: Port)(implicit
      env: TestConsoleEnvironment
  ) = {
    import env.*
    LedgerClient.singleHost(
      serverHost,
      serverPort.unwrap,
      baseConfig,
      tlsEnabledConfig,
      loggerFactory,
    )
  }

  "A TLS-enabled server" should {
    "reject ledger queries when the client connects without tls" in { env =>
      import env.*
      val serverPort = participant1.config.ledgerApi.clientConfig.port
      recoverToSucceededIf[io.grpc.StatusRuntimeException](
        LedgerClient
          .insecureSingleHost(serverHost, serverPort.unwrap, baseConfig, loggerFactory)
          .flatMap(_.stateService.getLedgerEnd())
      ).futureValue
    }

    "serve ledger queries when the client presents a valid certificate" in { implicit env =>
      import env.*
      val serverPort = participant1.config.ledgerApi.clientConfig.port

      (for {
        _ <- clientF(serverPort).flatMap(_.stateService.getLedgerEnd())
      } yield succeed).futureValue
    }
  }

  case class TLSPlugin(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {

    private val tls = TlsServerConfig(
      certChainFile = certChainFile,
      privateKeyFile = privateKeyFile,
      trustCollectionFile = Some(trustCertCollectionFile),
      minimumServerProtocolVersion = None,
    )

    override def beforeEnvironmentCreated(
        config: CantonConfig
    ): CantonConfig =
      ConfigTransforms
        .updateParticipantConfig("participant1")(
          _.focus(_.ledgerApi.tls)
            .replace(Some(tls))
            .focus(_.ledgerApi.authServices)
            .replace(Seq(Wildcard))
        )(config)
  }

}
