// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.ServerAuthRequirementConfig.Require
import com.digitalasset.canton.config.{
  PemFile,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.http.HttpService
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.{
  UseTls,
  clientTlsConfig,
  serverTlsConfig,
}
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.ParticipantSelector
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import org.scalatest.prop.TableDrivenPropertyChecks

import javax.net.ssl.SSLHandshakeException
import scala.concurrent.Future

class JsonTlsConfigTests
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken
    with TableDrivenPropertyChecks {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def useTls: UseTls = UseTls.Tls

  private lazy val testCases = Table(
    ("participantName", "testCase", "tlsConfig", "expectedStatus"),
    (
      "participant1",
      "regular TLS with required client cert should work",
      Some(
        serverTlsConfig.copy(clientAuth = Require(clientTlsConfig.clientCert.value))
      ),
      true,
    ),
    (
      "participant2",
      "regular TLS with required with wrong client cert should fail",
      Some(
        generateTLSServerConfig("ledger-api")
      ),
      false,
    ),
    (
      "participant3",
      "server with minimum TLSv1.3 should fail with TLSv1.2 client",
      Some(
        serverTlsConfig.copy(
          clientAuth = Require(clientTlsConfig.clientCert.value),
          minimumServerProtocolVersion = Some("TLSv1.3"),
        )
      ),
      false,
    ),
  )

  "http service" should {
    forAll(testCases) { case (participantName, testName, _, success) =>
      s"$testName on  $participantName " in { implicit env =>
        testService(env => env.lp(participantName), success)
      }
    }
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2, participant3).foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
        }
        createChannel(participant1)
      }
      .prependConfigTransforms(
        testCases.flatMap { case (participant, _, tls, _) =>
          Seq(
            ConfigTransforms.enableHttpLedgerApi(participant),
            // The JSON API is reusing the TLS config from the ledger API
            ConfigTransforms
              .updateParticipantConfig(participant)(
                _.focus(_.ledgerApi.tls)
                  .replace(tls)
              ),
          )
        }*
      )

  def testService(participantSelector: ParticipantSelector, expectedSuccess: Boolean)(implicit
      env: TestConsoleEnvironment
  ) = {
    import env.*

    (for {
      http <- adHocHttp(participantSelector)
      (status, _) <- http.getRequestString(Uri.Path(s"/v2/idps"), List.empty)
    } yield {
      if (expectedSuccess) {
        status should be(StatusCodes.OK)
      } else {
        // actually that is never an active path - as in case of failure the exception is thrown
        // no status is reported
        status should not be StatusCodes.OK
      }
    }).recoverWith { case error: Throwable =>
      if (expectedSuccess) {
        fail("Expected success, got error", error)
      } else {
        Future(error shouldBe a[SSLHandshakeException])
      }
    }.futureValue
  }

  private def generateTLSServerConfig(name: String): TlsServerConfig =
    TlsServerConfig(
      certChainFile =
        PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/$name.crt")),
      privateKeyFile =
        PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/$name.pem")),
      trustCollectionFile = Some(
        PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/root-ca.crt"))
      ),
      clientAuth = Require(
        TlsClientCertificate(
          certChainFile =
            PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/$name.crt")),
          privateKeyFile =
            PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/$name.pem")),
        )
      ),
    )

  // all the clients for this tests use TLSv1.2
  override protected def clientConnectionContext(config: TlsClientConfig): HttpsConnectionContext =
    ConnectionContext.httpsClient { (host, port) =>
      val sslContext = HttpService.buildSSLContext(config)
      val engine = sslContext.createSSLEngine(host, port)
      engine.setEnabledProtocols(Seq("TLSv1.2").toArray)
      engine.setUseClientMode(true)
      engine
    }
}
