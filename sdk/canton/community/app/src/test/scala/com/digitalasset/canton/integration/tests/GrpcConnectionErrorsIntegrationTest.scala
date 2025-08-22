// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{IdentityConfig, PemFile, StorageConfig, TlsBaseServerConfig}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInconsistentConnectivity
import monocle.macros.syntax.lens.*

/** Test different error cases when connecting a participant to a synchronizer with and without tls.
  * Unfortunately, tls errors lead to rather poor error messages, mostly because the Grpc
  * StatusRuntimeException does not necessarily mention ssl (depending on the os).
  */
trait GrpcConnectionErrorsIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  private lazy val certificatesPath = "./enterprise/app/src/test/resources/tls/public-api.crt"

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        // Prevent participant2 from auto-initializing.
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.init.identity).replace(IdentityConfig.Manual)
        ),
        ConfigTransforms.updateSequencerConfig("sequencer2")(
          _.focus(_.publicApi.tls).replace(
            Some(
              TlsBaseServerConfig(
                certChainFile = PemFile(ExistingFile.tryCreate(certificatesPath)),
                privateKeyFile = PemFile(
                  ExistingFile.tryCreate("./enterprise/app/src/test/resources/tls/public-api.pem")
                ),
              )
            )
          )
        ),
      )
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "participant2" // never started in this test
          )
        )
      )
      .withSetup { implicit env =>
        import env.*
        // Make ssl errors visible in the log.
        logging.set_level("io.netty", "DEBUG")
      }

  private lazy val hostname = "localhost"

  "A participant connecting to da (tls disabled)" when {
    def port(implicit env: TestConsoleEnvironment): Int = {
      import env.*
      sequencer1.config.publicApi.port.unwrap
    }

    "the url is incorrect" must {
      "fail with an informative error message" in { implicit env =>
        import env.*

        val wrongHostname = "wronghostname"
        val url = s"http://$wrongHostname:$port"

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect(synchronizerAlias = "da-wrong-host", connection = url),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include("Is the url correct?")
            entry.message should include(s"Unable to resolve host $wrongHostname")
          },
        )
      }
    }

    "the port is incorrect" must {
      "fail with an informative error message" in { implicit env =>
        import env.*

        // Using participant2's port, because we know that participant2 is not running.
        // So the port is not used by any other service.
        val incorrectPort = participant2.config.ledgerApi.port.unwrap
        val incorrectUrl = s"http://$hostname:$incorrectPort"

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers
            .connect(synchronizerAlias = "da-wrong-port", connection = incorrectUrl),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include("Is the server running?")
            entry.message should include regex s"Connection refused: localhost/.*:$incorrectPort"
          },
        )
      }
    }

    "using https instead of http" must {
      "fail with a descriptive error message" in { implicit env =>
        import env.*

        val incorrectUrl = s"https://$hostname:$port"

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers
            .connect(synchronizerAlias = "da-https", connection = incorrectUrl),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include(s"Are you using the right TLS settings?")
          },
        )
      }
    }

    "everything is correct" must {
      "succeed" in { implicit env =>
        import env.*

        val url = s"http://$hostname:$port"

        participant1.synchronizers.connect(synchronizerAlias = "da", connection = url)
        assertPingSucceeds(participant1, participant1, synchronizerId = Some(daId))
      }
    }
  }

  "A participant connecting to acme (tls enabled)" when {
    def port(implicit env: TestConsoleEnvironment): Int = {
      import env.*
      sequencer2.config.publicApi.port.unwrap
    }
    lazy val certificatesPath = "./enterprise/app/src/test/resources/tls/public-api.crt"

    "the certificates path is empty" must {
      "fail with an informative error message" in { implicit env =>
        import env.*

        val url = s"https://$hostname:$port"
        val emptyCertificatesPath = ""

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect(
            synchronizerAlias = "acme-no-cert",
            connection = url,
            certificatesPath = emptyCertificatesPath,
          ),
          entry => {
            // It is hard to come up with a better error message. The empty certificates path on its own is not
            // an error. It could be that the user wanted to use the certificates in the JVM's trust store.
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include(s"Are you using the right TLS settings?")
          },
        )
      }
    }

    "the certificates path points to an incorrect certificate" must {
      "fail with an informative error message" in { implicit env =>
        import env.*

        val url = s"https://$hostname:$port"
        val incorrectCertificatesPath = "./enterprise/app/src/test/resources/tls/some.crt"

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect(
            synchronizerAlias = "acme-wrong-cert",
            connection = url,
            certificatesPath = incorrectCertificatesPath,
          ),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include(s"Are you using the right TLS settings?")
          },
        )
      }
    }

    "the certificates path points to a non-existing file" must {
      "fail with an informative error message" in { implicit env =>
        import env.*

        val url = s"https://$hostname:$port"
        val incorrectCertificatesPath =
          "./enterprise/app/src/test/resources/tls/schnitzel-mit-pommes.crt"

        val ex = the[IllegalArgumentException] thrownBy
          participant1.synchronizers.connect(
            synchronizerAlias = "acme-no-cert-file",
            connection = url,
            certificatesPath = incorrectCertificatesPath,
          )
        ex.getMessage shouldBe s"failed to load $incorrectCertificatesPath: No such file [$incorrectCertificatesPath]."
      }
    }

    "using http instead of https" must {
      "fail with an informative error message" in { implicit env =>
        import env.*

        val url = s"http://$hostname:$port"

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect(
            synchronizerAlias = "acme-http",
            connection = url,
            certificatesPath = certificatesPath,
          ),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include(s"Are you using the right TLS settings?")
          },
        )
      }
    }

    "everything is correct" must {
      "succeed" in { implicit env =>
        import env.*

        val url = s"https://$hostname:$port"

        participant1.synchronizers.connect(
          synchronizerAlias = "acme",
          connection = url,
          certificatesPath = certificatesPath,
        )
        assertPingSucceeds(participant1, participant1, synchronizerId = Some(acmeId))
      }
    }
  }
}

class GrpcConnectionErrorsReferenceIntegrationTestInMemory
    extends GrpcConnectionErrorsIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
      ),
    )
  )
}

class GrpcConnectionErrorsBftOrderingIntegrationTestInMemory
    extends GrpcConnectionErrorsIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
      ),
    )
  )
}

// class GrpcConnectionErrorsReferenceIntegrationTestPostgres extends GrpcConnectionErrorsIntegrationTest {
//   registerPlugin(new UsePostgres(loggerFactory))
//   registerPlugin(
//     new UseReferenceBlockSequencer[DbConfig.Postgres](
//       loggerFactory,
//       sequencerGroups = MultiSynchronizer(
//         Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
//       ),
//     )
//   )
// }

// class GrpcConnectionErrorsBftOrderingIntegrationTestPostgres extends GrpcConnectionErrorsIntegrationTest {
//   registerPlugin(new UsePostgres(loggerFactory))
//   registerPlugin(
//     new UseBftOrderingBlockSequencer(
//       loggerFactory,
//       sequencerGroups = MultiSynchronizer(
//         Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
//       ),
//     )
//   )
// }
