// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.sequencer.SequencerConnectClient
import com.digitalasset.canton.common.sequencer.SequencerConnectClient.SynchronizerClientBootstrapInfo
import com.digitalasset.canton.common.sequencer.grpc.GrpcSequencerConnectClient
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  DbConfig,
  ProcessingTimeout,
  RequireTypes,
}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias, config}

trait SequencerConnectServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  // TODO(i16601): Remove after the shutdown rework
  override protected def destroyEnvironment(environment: TestConsoleEnvironment): Unit =
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      ConcurrentEnvironmentLimiter.destroy(getClass.getName, numPermits) {
        manualDestroyEnvironment(environment)
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        mayContain = Seq(
          // Handle DB queries during shutdown
          _.warningMessage should include("DB_STORAGE_DEGRADATION"),
          _.errorMessage should include("retryWithDelay failed unexpectedly"),
        ),
      ),
    )

  protected def sequencerPlugin: EnvironmentSetupPlugin
  registerPlugin(sequencerPlugin)

  protected def localSequencer(implicit
      env: TestConsoleEnvironment
  ): Option[LocalSequencerReference]

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  lazy val alias: SynchronizerAlias = SynchronizerAlias.tryCreate("sequencer1")

  protected def getSequencerConnectClient()(implicit
      env: TestConsoleEnvironment
  ): SequencerConnectClient

  "SequencerConnectService" should {
    "respond to handshakes" in { implicit env =>
      val grpcSequencerConnectClient = getSequencerConnectClient()

      val unsupportedPV = TestProtocolVersions.UnsupportedPV

      val includeAlphaVersions = testedProtocolVersion.isAlpha
      val includeBetaVersions = testedProtocolVersion.isBeta

      val successfulRequest =
        HandshakeRequest(
          ProtocolVersionCompatibility.supportedProtocols(
            includeAlphaVersions = includeAlphaVersions,
            includeBetaVersions = includeBetaVersions,
            release = ReleaseVersion.current,
          ),
          None,
        )
      val successfulRequestWithMinimumVersion =
        HandshakeRequest(
          ProtocolVersionCompatibility.supportedProtocols(
            includeAlphaVersions = includeAlphaVersions,
            includeBetaVersions = includeBetaVersions,
            release = ReleaseVersion.current,
          ),
          Some(testedProtocolVersion),
        )
      val failingRequest = HandshakeRequest(Seq(unsupportedPV), None)

      grpcSequencerConnectClient
        .handshake(alias, successfulRequest, dontWarnOnDeprecatedPV = true)
        .futureValueUS
        .value shouldBe HandshakeResponse.Success(testedProtocolVersion)

      grpcSequencerConnectClient
        .handshake(alias, successfulRequestWithMinimumVersion, dontWarnOnDeprecatedPV = true)
        .futureValueUS
        .value shouldBe HandshakeResponse.Success(testedProtocolVersion)

      inside(
        grpcSequencerConnectClient
          .handshake(alias, failingRequest, dontWarnOnDeprecatedPV = true)
          .futureValueUS
          .value
      ) { case HandshakeResponse.Failure(serverVersion, reason) =>
        serverVersion shouldBe testedProtocolVersion
        reason should include(unsupportedPV.toString)
      }
    }

    "respond to GetSynchronizerParameters requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      val fetchedSynchronizerParameters =
        grpcSequencerConnectClient.getSynchronizerParameters(alias.unwrap).futureValueUS.value
      val cryptoProvider = CryptoProvider.Jce

      val defaultSynchronizerParametersConfig = SynchronizerParametersConfig(
        requiredSigningAlgorithmSpecs = Some(cryptoProvider.signingAlgorithms.supported),
        requiredEncryptionAlgorithmSpecs = Some(cryptoProvider.encryptionAlgorithms.supported),
        requiredSymmetricKeySchemes = Some(cryptoProvider.symmetric.supported),
        requiredHashAlgorithms = Some(cryptoProvider.hash.supported),
        requiredCryptoKeyFormats =
          Some(cryptoProvider.supportedCryptoKeyFormatsForProtocol(testedProtocolVersion)),
        requiredSignatureFormats =
          Some(cryptoProvider.supportedSignatureFormatsForProtocol(testedProtocolVersion)),
      )
      val expectedSynchronizerParameters = defaultSynchronizerParametersConfig
        .toStaticSynchronizerParameters(CryptoConfig(), testedProtocolVersion)
        .value

      fetchedSynchronizerParameters shouldBe expectedSynchronizerParameters

      sequencer1.synchronizer_parameters.static
        .get()
        .toInternal shouldBe expectedSynchronizerParameters
    }

    "respond to SynchronizerClientBootstrapInfo requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      val bi = grpcSequencerConnectClient
        .getSynchronizerClientBootstrapInfo(alias)
        .futureValueUS
        .value
      bi shouldBe SynchronizerClientBootstrapInfo(daId.toPhysical, sequencer1.id)
    }

    "respond to GetSynchronizerId requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      grpcSequencerConnectClient
        .getSynchronizerId(daName.unwrap)
        .futureValueUS
        .value shouldBe daId.toPhysical
    }

    "respond to is active requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      grpcSequencerConnectClient
        .isActive(participant1.id, alias, waitForActive = false)
        .futureValueUS
        .value shouldBe false

      localSequencer.map { sequencer =>
        participant1.synchronizers.connect_local(sequencer, alias)

        utils.retry_until_true(Seq(participant1).forall(_.synchronizers.active(alias)))

        grpcSequencerConnectClient
          .isActive(participant1.id, alias, waitForActive = false)
          .futureValueUS
          .value shouldBe true
      }
    }
  }
}

trait GrpcSequencerConnectServiceIntegrationTest extends SequencerConnectServiceIntegrationTest {

  private def getSequencerConnectClientInternal(endpoint: Endpoint, timeouts: ProcessingTimeout)(
      implicit env: TestConsoleEnvironment
  ): GrpcSequencerConnectClient = {
    val grpcSequencerConnection =
      GrpcSequencerConnection(
        NonEmpty(Seq, endpoint),
        transportSecurity = false,
        customTrustCertificates = None,
        SequencerAlias.Default,
        None,
      )

    new GrpcSequencerConnectClient(
      sequencerConnection = grpcSequencerConnection,
      timeouts = timeouts,
      traceContextPropagation = TracingConfig.Propagation.Enabled,
      loggerFactory = loggerFactory,
    )(env.executionContext)
  }

  protected def getSequencerConnectClient()(implicit
      env: TestConsoleEnvironment
  ): GrpcSequencerConnectClient = {
    val publicApi = sequencerNodeConfig.publicApi
    val endpoint = Endpoint(publicApi.address, publicApi.port)
    getSequencerConnectClientInternal(endpoint, timeouts)
  }

  protected def localSequencer(implicit
      env: TestConsoleEnvironment
  ): Option[LocalSequencerReference] =
    Some(env.sequencer1)

  protected def sequencerNodeConfig(implicit
      env: TestConsoleEnvironment
  ): SequencerNodeConfig = env.sequencer1.config

  "GrpcSequencerConnectService" should {
    "report inability to connect to sequencer on is-active requests as a left rather than an exception" in {
      implicit env =>
        import env.*

        val publicApi = sequencerNodeConfig.publicApi
        val badSequencerNodeEndpoint =
          Endpoint(publicApi.address, RequireTypes.Port.tryCreate(publicApi.port.unwrap + 10000))
        val grpcSequencerConnectClient = getSequencerConnectClientInternal(
          badSequencerNodeEndpoint,
          // Lower the timeout to avoid lengthy retries of 60 seconds by default
          timeouts.copy(verifyActive = config.NonNegativeDuration.ofMillis(500)),
        )

        val errorFromLeft = grpcSequencerConnectClient
          .isActive(participant1.id, alias, waitForActive = false)
          .leftMap(_.message)
          .leftOrFail("expected a left")
          .futureValueUS
        errorFromLeft should include regex "Request failed for .*. Is the server running?"
    }
  }
}

// TODO(#12363) Add a DB sequencer test (env. default) when the DB Sequencer supports group addressing

//abstract class GrpcSequencerConnectServiceTestDefault extends GrpcSequencerConnectServiceIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//}

abstract class GrpcSequencerConnectServiceIntegrationTestPostgres
    extends GrpcSequencerConnectServiceIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}

class GrpcSequencerConnectServiceIntegrationTestPostgresReference
    extends GrpcSequencerConnectServiceIntegrationTestPostgres {

  override lazy val sequencerPlugin =
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)

  override protected def localSequencer(implicit
      env: TestConsoleEnvironment
  ): Option[LocalSequencerReference] = None
}

class GrpcSequencerConnectServiceIntegrationTestPostgresBft
    extends GrpcSequencerConnectServiceIntegrationTestPostgres {

  override lazy val sequencerPlugin =
    new UseBftSequencer(loggerFactory)
}
