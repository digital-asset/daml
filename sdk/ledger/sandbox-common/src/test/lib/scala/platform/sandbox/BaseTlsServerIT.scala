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
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import io.grpc.StatusRuntimeException
import io.netty.handler.ssl.ClientAuth
import org.scalatest.Assertion
import org.scalatest.exceptions.ModifiableMessage
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import scala.concurrent.Future

abstract class BaseTlsServerIT(minimumServerProtocolVersion: Option[TlsVersion])
    extends AsyncWordSpec
    with AbstractSandboxFixture
    with SuiteResourceManagementAroundAll {

  minimumServerProtocolVersion match {
    case Some(TlsVersion.V1_3) =>
      "A server with TLSv1.3 or higher enabled" should {
        "accept client connections secured equal of higher than TLSv1.3" in {
          for {
            _ <- assertSuccessfulClient(enabledProtocols = Seq(TlsVersion.V1_3))
          } yield succeed
        }
        "reject client connections secured lower than TLSv1.3" in {
          for {
            _ <- assertFailedClient(enabledProtocols = Seq.empty)
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1))
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1_1))
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1_2))
          } yield succeed
        }
      }
    case Some(TlsVersion.V1_2) =>
      "A server with TLSv1.2 or higher enabled" should {
        "accept client connections secured equal of higher than TLSv1.2" in {
          for {
            _ <- assertSuccessfulClient(enabledProtocols = Seq(TlsVersion.V1_3))
            _ <- assertSuccessfulClient(enabledProtocols = Seq(TlsVersion.V1_2))
          } yield succeed
        }
        "reject client connections secured lower than TLSv1.2" in {
          for {
            _ <- assertFailedClient(enabledProtocols = Seq.empty)
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1))
            _ <- assertFailedClient(enabledProtocols = Seq(TlsVersion.V1_1))
          } yield succeed
        }
      }
    case other =>
      throw new IllegalArgumentException(s"Not test cases found for TLS version: |${other}|!")
  }

  protected val List(
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

  override protected def config: Config =
    super.config.copy(
      participants = singleParticipant(
        ApiServerConfig.copy(
          tls = Some(
            TlsConfiguration(
              enabled = true,
              Some(certChainFilePath),
              Some(privateKeyFilePath),
              Some(trustCertCollectionFilePath),
              minimumServerProtocolVersion = minimumServerProtocolVersion,
              clientAuth = ClientAuth.NONE,
            )
          )
        )
      )
    )

  private val clientConfig: LedgerClientConfiguration =
    LedgerClientConfiguration(
      "appId",
      LedgerIdRequirement.none,
      CommandClientConfiguration.default,
    )

  protected def assertFailedClient(enabledProtocols: Seq[TlsVersion]): Future[Assertion] = {
    // given
    val clientChannelConfig = if (enabledProtocols.nonEmpty) {
      getClientChannelConfigWithTls(enabledProtocols)
    } else {
      LedgerClientChannelConfiguration.InsecureDefaults
    }
    val clueMsg = s"Client enabled following protocols: ${enabledProtocols}. "
    val prependClueMsg: Throwable => Throwable = {
      case e: ModifiableMessage[_] =>
        e.modifyMessage(_.map(clueMsg + _))
      case t => t
    }

    // when
    recoverToSucceededIf[StatusRuntimeException] {
      createLedgerClient(clientChannelConfig).flatMap(_.transactionClient.getLedgerEnd())
    }.transform(
      identity,
      prependClueMsg,
    )
  }

  protected def assertSuccessfulClient(enabledProtocols: Seq[TlsVersion]): Future[Assertion] = {
    // given
    val clientConfig = if (enabledProtocols.nonEmpty) {
      getClientChannelConfigWithTls(enabledProtocols)
    } else {
      LedgerClientChannelConfiguration.InsecureDefaults
    }
    val clueMsg = s"Client enabled protocols: ${enabledProtocols}. "
    val addClueThrowable: Throwable => Throwable = { t =>
      new Throwable(clueMsg + "Test failed with an exception. See the cause.", t)
    }

    // when
    val response: Future[GetLedgerEndResponse] =
      createLedgerClient(clientConfig).flatMap(_.transactionClient.getLedgerEnd())

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
    val tlsConfiguration = TlsConfiguration(
      enabled = true,
      Some(clientCertChainFilePath),
      Some(clientPrivateKeyFilePath),
      Some(trustCertCollectionFilePath),
    )
    val sslContext = tlsConfiguration.client(enabledProtocols = enabledProtocols)
    LedgerClientChannelConfiguration(sslContext)
  }

  private def createLedgerClient(
      channelConfig: LedgerClientChannelConfiguration
  ): Future[LedgerClient] =
    LedgerClient.singleHost(
      hostIp = serverHost,
      port = serverPort.value,
      configuration = clientConfig,
      channelConfig = channelConfig,
    )

}
