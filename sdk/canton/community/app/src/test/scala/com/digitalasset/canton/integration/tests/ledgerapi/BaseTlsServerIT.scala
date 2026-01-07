// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.ledger.api.v2.state_service.GetLedgerEndResponse
import com.daml.tls.TlsVersion
import com.daml.tls.TlsVersion.TlsVersion
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{
  CantonConfig,
  PemFile,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
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
import io.grpc.StatusRuntimeException
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.scalatest.RecoverMethods.recoverToSucceededIf
import org.scalatest.exceptions.ModifiableMessage

import scala.concurrent.Future

abstract class BaseTlsServerIT(minimumServerProtocolVersion: Option[TlsVersion])
    extends CantonFixture {

  registerPlugin(TLSPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  minimumServerProtocolVersion match {
    case Some(TlsVersion.V1_3) =>
      "A server with TLSv1.3 or higher enabled" should {
        "accept client connections secured equal of higher than TLSv1.3" in { implicit env =>
          import env.*
          (for {
            _ <- assertSuccessfulClient(enabledProtocols = Seq(TlsVersion.V1_3))
          } yield succeed).futureValue
        }
        "reject client connections secured lower than TLSv1.3" in { implicit env =>
          import env.*
          (for {
            _ <- assertFailedClient(enabledProtocols = Seq.empty)
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1))
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1_1))
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1_2))
          } yield succeed).futureValue
        }
      }
    case Some(TlsVersion.V1_2) =>
      "A server with TLSv1.2 or higher enabled" should {
        "accept client connections secured equal of higher than TLSv1.2" in { implicit env =>
          import env.*
          (for {
            _ <- assertSuccessfulClient(enabledProtocols = Seq(TlsVersion.V1_3))
            _ <- assertSuccessfulClient(enabledProtocols = Seq(TlsVersion.V1_2))
          } yield succeed).futureValue
        }
        "reject client connections secured lower than TLSv1.2" in { implicit env =>
          import env.*
          (for {
            _ <- assertFailedClient(enabledProtocols = Seq.empty)
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1))
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1_1))
          } yield succeed).futureValue
        }
      }
    case other =>
      throw new IllegalArgumentException(s"No test cases found for TLS version: |$other|!")
  }

  private def getPemFile(fileName: String): PemFile =
    PemFile(ExistingFile.tryCreate(JarResourceUtils.resourceFile("test-certificates/" + fileName)))

  lazy private val certChainFile = getPemFile("server.crt")
  lazy private val privateKeyFile = getPemFile("server.pem")
  lazy private val trustCertCollectionFile = getPemFile("ca.crt")
  lazy private val clientCertChainFile = getPemFile("client.crt")
  lazy private val clientPrivateKeyFile = getPemFile("client.pem")

  private val clientConfig: LedgerClientConfiguration =
    LedgerClientConfiguration(
      "userId",
      CommandClientConfiguration.default,
    )

  protected def assertFailedClient(
      enabledProtocols: Seq[TlsVersion]
  )(implicit env: TestConsoleEnvironment): Future[Assertion] = {
    import env.*

    // given
    val clientChannelConfig = if (enabledProtocols.nonEmpty) {
      getClientChannelConfigWithTls(enabledProtocols)
    } else {
      LedgerClientChannelConfiguration.InsecureDefaults
    }
    val clueMsg = s"Client enabled following protocols: $enabledProtocols. "
    val prependClueMsg: Throwable => Throwable = {
      case e: ModifiableMessage[?] =>
        e.modifyMessage(_.map(clueMsg + _))
      case t => t
    }

    // when
    recoverToSucceededIf[StatusRuntimeException] {
      createLedgerClient(clientChannelConfig).flatMap(_.stateService.getLedgerEnd())
    }.transform(
      identity,
      prependClueMsg,
    )
  }

  protected def assertSuccessfulClient(
      enabledProtocols: Seq[TlsVersion]
  )(implicit env: TestConsoleEnvironment): Future[Assertion] = {
    import env.*

    // given
    val clientConfig = if (enabledProtocols.nonEmpty) {
      getClientChannelConfigWithTls(enabledProtocols)
    } else {
      LedgerClientChannelConfiguration.InsecureDefaults
    }
    val clueMsg = s"Client enabled protocols: $enabledProtocols. "
    val addClueThrowable: Throwable => Throwable = { t =>
      new Throwable(clueMsg + "Test failed with an exception. See the cause.", t)
    }

    // when
    val response: Future[GetLedgerEndResponse] =
      createLedgerClient(clientConfig).flatMap(_.stateService.getLedgerEnd())

    // then
    response.value
    response
      .map { response =>
        assert(response ne null)
        assert(response.isInstanceOf[GetLedgerEndResponse])
      }
      .transform(identity, addClueThrowable)
  }

  private def getClientChannelConfigWithTls(
      enabledProtocols: Seq[TlsVersion]
  ): LedgerClientChannelConfiguration = {
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
      ClientChannelBuilder.sslContext(tlsConfiguration, enabledProtocols = enabledProtocols)
    LedgerClientChannelConfiguration(Some(sslContext))
  }

  private def createLedgerClient(
      channelConfig: LedgerClientChannelConfiguration
  )(implicit env: TestConsoleEnvironment): Future[LedgerClient] = {
    import env.*
    val serverPort = participant1.config.ledgerApi.clientConfig.port

    LedgerClient.singleHost(
      hostIp = serverHost,
      port = serverPort.unwrap,
      configuration = clientConfig,
      channelConfig = channelConfig,
      loggerFactory,
    )
  }

  case class TLSPlugin(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {

    private val tls = TlsServerConfig(
      certChainFile = certChainFile,
      privateKeyFile = privateKeyFile,
      trustCollectionFile = Some(trustCertCollectionFile),
      minimumServerProtocolVersion = minimumServerProtocolVersion.map(_.version),
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
