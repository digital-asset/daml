// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.variations

import com.daml.ledger.api.testtool.Tests
import com.daml.ledger.api.testtool.infrastructure.{
  JsonSupported,
  LedgerTestCase,
  LedgerTestSuite,
  TestConstraints,
}
import com.daml.ledger.api.testtool.runner.{AvailableTests, Config, ConfiguredTests, TestRunner}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.ConfigTransforms.updateAllParticipantConfigs_
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  EnvVarTestOverrides,
  TestInclusions,
}
import com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiConformanceBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.time.{Seconds, Span}

// TODO(#21030): Unify with gRPC conformance tests
sealed trait JsonApiConformanceBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EnvVarTestOverrides {

  // ensure conformance tests have less noisy neighbours
  protected override def numPermits: PositiveInt = PositiveInt.tryCreate(2)

  // Set to false to compare with gRPC test run
  protected def useJson = true

  protected def inclusions: TestInclusions
  protected def exclusions: Set[String] = Set.empty

  // This is local patience config - used only to wait for a shard end
  protected val testSummariesPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(500, Seconds)))

  protected def testCaseName: String

  protected def setup()(implicit
      env: FixtureParam
  ): (
      Config,
      AvailableTests,
      List[LedgerTestCase],
      TestInclusions,
      /* Env arg inclusions */
  ) = {
    val participants = testParticipants
    val adminParticipants = testAdminParticipants
    val config = Config.default.copy(
      verbose = true,
      timeoutScaleFactor = 3.0,
      reportOnFailuresOnly = true,
      jsonApiMode = useJson,
      participantsEndpoints = participants,
      participantsAdminEndpoints = adminParticipants,
      concurrentTestRuns = VariationsConformanceTestUtils.ConcurrentTestRuns,
      connectedSynchronizers = env.environment.config.sequencers.size,
    )

    val availableTests = new AvailableTests {
      override def defaultTests: Vector[LedgerTestSuite] =
        Tests.default(timeoutScaleFactor = config.timeoutScaleFactor)

      override def optionalTests: Vector[LedgerTestSuite] =
        Tests.optional(config.tlsConfig)
    }

    val envArgInclusion = envArgTestsInclusion.getOrElse(TestInclusions.AllIncluded)

    val testsToRun =
      new ConfiguredTests(availableTests, config).defaultTests.view
        .flatMap(_.tests)
        .filter { testCase =>
          testCase.limitation match {
            case _: JsonSupported =>
              inclusions.testCaseEnabled(testCase.name) &&
              !(exclusions.contains(testCase.name) || exclusions.contains(testCase.suite.name))
            case _: TestConstraints.GrpcOnly => false
          }
        }
        .toList

    (config, availableTests, testsToRun, envArgInclusion)
  }

  private def testParticipants(implicit env: FixtureParam): Vector[(String, Int)] =
    if (useJson)
      env.participants.all.map {
        // TODO(#22349): Use the JSON API client config once exposed, similarly to how we extract the address and port for the gRPC API
        case localParticipantConfig: LocalParticipantReference =>
          val jsonApiServer = localParticipantConfig.config.httpLedgerApi
          jsonApiServer.address -> jsonApiServer.port.unwrap
        case _other =>
          fail(s"Expecting only local participant references but got ${_other}")
      }.toVector
    else
      env.participants.all.map { p =>
        val ledgerApiConfig = p.config.clientLedgerApi
        ledgerApiConfig.address -> ledgerApiConfig.port.unwrap
      }.toVector

  private def testAdminParticipants(implicit env: FixtureParam): Vector[(String, Int)] =
    env.participants.all.map { p =>
      val adminApiConfig = p.config.clientAdminApi
      adminApiConfig.address -> adminApiConfig.port.unwrap
    }.toVector

  def numShards: Int
  def shard: Int

  def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .prependConfigTransform(
        ConfigTransforms.enableHttpLedgerApi
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1_, alias = daName)
      }

  "Ledger JSON API" should {
    testCaseName in { implicit env =>
      import env.*

      val (config, availableTests, testsToRun, envArgInclusions) = setup()

      val testsInCurrentShard = testsToRun.zipWithIndex.collect {
        case (testCase, i) if i % numShards == shard => testCase
      }

      val testsToBeRun =
        testsInCurrentShard
          .filter(tc => envArgInclusions.testCaseEnabled(tc.name))
          .map(_.name)
          .toSet

      // If the env arg filtering leads to no tests in the current shard, fail
      if (testsToBeRun.isEmpty) {
        cancel(
          s"No tests to run in current shard $shard/$numShards. " +
            s"Original tests in current shard: ${testsInCurrentShard.map(_.name)}. " +
            s"Test selection from env var $LapittRunOnlyEnvVarName: $envArgInclusions."
        )
      } else {
        val testRunner = new TestRunner(
          availableTests,
          config.copy(included = testsToBeRun),
          Tests.lfVersion,
        )

        logger.debug(
          s"Running ${testsToBeRun.mkString("[", ", ", "]")} in current shard $shard/$numShards"
        )

        val (resultF, _testCases) = testRunner.runInProcess()
        resultF
          .map { summaries =>
            val failures = summaries
              .collect(summary =>
                summary.result.left
                  .map(failure => s"${summary.name} failed: $failure")
              )
              .collect { case Left(failure) => failure }
            if (failures.nonEmpty) {
              fail(
                s"Some JSON Ledger API conformance test cases have failed: ${failures
                    .mkString("\n\t", "\n\t", "")}"
              )
            }
            succeed
          }
          .futureValue(config = testSummariesPatience, pos = implicitly)
      }
    }
  }
}

sealed abstract class JsonApiConformanceIntegrationShardedTest(
    val shard: Int,
    val numShards: Int,
) extends JsonApiConformanceBase {

  // Multi-domain, multi-participant environment for full test coverage
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .prependConfigTransform(ConfigTransforms.enableHttpLedgerApi)
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
      }

  protected def inclusions: TestInclusions = TestInclusions.AllIncluded
  override protected def exclusions: Set[String] = LedgerApiConformanceBase.excludedTests.toSet ++
    Set(
      "HealthServiceIT", // Service not available in JSON,
      "PartyManagementServiceIT", // updatePartyIdentityProviderIs is not available in JSON API
      "UserManagementServiceIT", // Results in PERMISSION_DENIED (wrong _.userManagement.supported)
    )

  protected def testCaseName = "pass the Ledger API conformance tests"
}

final class JsonApiConformanceIntegrationShardedTest_Shard_0
    extends JsonApiConformanceIntegrationShardedTest(shard = 0, numShards = 5)

final class JsonApiConformanceIntegrationShardedTest_Shard_1
    extends JsonApiConformanceIntegrationShardedTest(shard = 1, numShards = 5)

final class JsonApiConformanceIntegrationShardedTest_Shard_2
    extends JsonApiConformanceIntegrationShardedTest(shard = 2, numShards = 5)

final class JsonApiConformanceIntegrationShardedTest_Shard_3
    extends JsonApiConformanceIntegrationShardedTest(shard = 3, numShards = 5)

final class JsonApiConformanceIntegrationShardedTest_Shard_4
    extends JsonApiConformanceIntegrationShardedTest(shard = 4, numShards = 5)

private[variations] trait NonSharded {
  this: JsonApiConformanceBase =>

  override def shard: Int = 0
  override def numShards: Int = 1
}

final class JsonApiOffsetCheckpointsConformanceTest extends JsonApiConformanceBase with NonSharded {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(
      updateAllParticipantConfigs_(
        _.focus(_.ledgerApi.indexService.offsetCheckpointCacheUpdateInterval)
          .replace(config.NonNegativeFiniteDuration(java.time.Duration.ofMillis(1000)))
          .focus(_.ledgerApi.indexService.idleStreamOffsetCheckpointTimeout)
          .replace(config.NonNegativeFiniteDuration(java.time.Duration.ofMillis(1000)))
      )
    )

  override def inclusions: TestInclusions = TestInclusions.SelectedTests(
    includedSuites = Set("CheckpointInTailingStreamsIT")
  )

  override def testCaseName: String =
    "pass CheckpointInTailingStreamsIT on a Ledger API with short offset checkpoint update interval"
}
