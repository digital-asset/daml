// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.plugins.UseExternalProcess.RunVersion
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  LAPITTVersion,
  releasesFromArtifactory,
}
import com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiConformanceBase
import com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiConformanceBase.excludedTests
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.util.ReleaseUtils.TestedRelease
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseVersion}

trait MultiVersionLedgerApiConformanceBase extends LedgerApiConformanceBase {

  protected def ledgerApiTestToolVersions: Seq[String]

  protected val ledgerApiTestToolPlugins: Map[String, UseLedgerApiTestTool] =
    ledgerApiTestToolVersions
      .map(version =>
        version -> new UseLedgerApiTestTool(
          loggerFactory = loggerFactory,
          connectedSynchronizersCount = connectedSynchronizersCount,
          version = LAPITTVersion.Explicit(version),
        )
      )
      .toMap

  ledgerApiTestToolPlugins.values.foreach(registerPlugin)

  def runShardedTests(
      version: ReleaseVersion
  )(shard: Int, numShards: Int)(
      env: TestConsoleEnvironment
  ): String =
    ledgerApiTestToolPlugins(version.toString)
      .runShardedSuites(
        shard,
        numShards,
        exclude = excludedTests,
      )(env)
}

/** The Protocol continuity tests test that we don't accidentally break protocol compatibility with
  * respect to the Ledger API.
  *
  * To run them, see :
  *
  *   - AllProtocolContinuityConformanceTest Tests against all previously published releases.
  *
  *   - LatestProtocolContinuityConformanceTest Tests against the latest published release.
  */
trait ProtocolContinuityConformanceTest
    extends MultiVersionLedgerApiConformanceBase
    with IsolatedEnvironments
    with HasExecutionContext {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .addConfigTransforms(ConfigTransforms.clearMinimumProtocolVersion*)
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)

  protected def testedReleases: List[TestedRelease]
  override lazy val ledgerApiTestToolVersions: List[String] =
    testedReleases.map(_.releaseVersion.toString)

  protected def numShards: Int
  protected def shard: Int
}

/** For a given release R, the Ledger API conformance test suites at release R are run against:
  *   - 1x synchronizer of release R with the latest protocol version of release R
  *   - 4x participants based on current main
  */
trait ProtocolContinuityConformanceTestSynchronizer extends ProtocolContinuityConformanceTest {
  private lazy val externalSequencer =
    new UseExternalProcess(
      loggerFactory,
      externalSequencers = Set("sequencer1"),
      fileNameHint = this.getClass.getSimpleName,
    )

  private lazy val externalMediator =
    new UseExternalProcess(
      loggerFactory,
      externalMediators = Set("mediator1"),
      fileNameHint = this.getClass.getSimpleName,
    )

  registerPlugin(externalSequencer)
  registerPlugin(externalMediator)

  testedReleases.foreach { case TestedRelease(release, protocolVersions) =>
    lazy val binDir = ReleaseUtils.retrieve(release).futureValue
    lazy val pv = protocolVersions.max1

    s"run conformance tests of shard $shard with release $release and protocol $pv" in { env =>
      import env.*

      val cantonReleaseVersion = RunVersion.Release(binDir)

      externalMediator.start(remoteMediator1.name, cantonReleaseVersion)
      externalSequencer.start(remoteSequencer1.name, cantonReleaseVersion)

      remoteSequencer1.health.wait_for_ready_for_initialization()
      remoteMediator1.health.wait_for_ready_for_initialization()

      val staticParams = StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = pv)
      NetworkBootstrapper(
        Seq(
          NetworkTopologyDescription.createWithStaticSynchronizerParameters(
            daName,
            synchronizerOwners = Seq(remoteSequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(remoteSequencer1),
            mediators = Seq(remoteMediator1),
            staticSynchronizerParameters = staticParams,
          )
        )
      )(env).bootstrap()

      remoteSequencer1.health.wait_for_initialized()
      remoteMediator1.health.wait_for_initialized()
      participant1.health.wait_for_initialized()

      setupLedgerApiConformanceEnvironment(env)

      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        runShardedTests(release)(shard, numShards)(env)
      }

      // Shutdown
      shutdownLedgerApiConformanceEnvironment(env)

      externalMediator.stop(remoteMediator1.name)
    }
  }
}

/** For a given release R, these tests run the Ledger API compatibility tests against
  *   - 1x synchronizer based on current main with the latest protocol version of release R
  *   - 1x participant of release R
  */
trait ProtocolContinuityConformanceTestParticipant extends ProtocolContinuityConformanceTest {
  val external = new UseExternalProcess(
    loggerFactory,
    externalParticipants = Set("participant1"),
    fileNameHint = this.getClass.getSimpleName,
  )

  // TODO(i9548): Run with a single participant because currently there is no way to set a participant when using the Ledger API test tool.
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(1, 1, 1)
      .withManualStart

  registerPlugin(external)

  testedReleases.foreach { case TestedRelease(release, protocolVersions) =>
    lazy val binDir = ReleaseUtils.retrieve(release).futureValue
    lazy val pv = protocolVersions.max1

    s"run conformance tests of shard $shard with release $release and protocol $pv" in {
      implicit env =>
        import env.*

        val cantonReleaseVersion = RunVersion.Release(binDir)

        sequencer1.start()
        mediator1.start()
        mediator1.health.wait_for_ready_for_initialization()
        sequencer1.health.wait_for_ready_for_initialization()

        val staticParams = StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = pv)
        NetworkBootstrapper(
          Seq(
            EnvironmentDefinition.S1M1.copy(staticSynchronizerParameters = staticParams)
          )
        ).bootstrap()

        // Run the participant from the release binary
        external.start(remoteParticipant1.name, cantonReleaseVersion)
        remoteParticipant1.health.wait_for_initialized()

        setupLedgerApiConformanceEnvironment(env)

        runShardedTests(release)(shard, numShards)(env)

        // Shutdown
        shutdownLedgerApiConformanceEnvironment(env)
        external.stop(remoteParticipant1.name)
    }
  }
}

private[continuity] object ProtocolContinuityConformanceTest {
  // all patch versions that are supported
  def previousSupportedReleases(logger: TracedLogger)(implicit
      tc: TraceContext
  ): List[TestedRelease] =
    releasesFromArtifactory(logger)
      .map(ReleaseVersion.tryCreate)
      .map { releaseVersion =>
        TestedRelease(
          releaseVersion,
          ProtocolVersionCompatibility.supportedProtocols(
            includeAlphaVersions = false,
            includeBetaVersions = true,
            release = releaseVersion,
          ),
        )
      }
      // excluding protocol versions that are deleted
      .flatMap { case TestedRelease(releaseVersion, protocolVersions) =>
        NonEmpty
          .from(protocolVersions.filterNot(_.isDeleted))
          .map(pvs => TestedRelease(releaseVersion, pvs))
      }
      .toList
      .sortBy(_.releaseVersion)

  def latestSupportedRelease(logger: TracedLogger)(implicit
      tc: TraceContext
  ): List[TestedRelease] =
    previousSupportedReleases(logger).takeRight(1)
}
