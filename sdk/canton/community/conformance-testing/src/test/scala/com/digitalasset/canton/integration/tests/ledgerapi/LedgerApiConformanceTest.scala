// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.ConfigTransforms.updateAllParticipantConfigs_
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.LAPITTVersion
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiConformanceBase.excludedTests
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*
import org.slf4j.event

trait SingleVersionLedgerApiConformanceBase extends LedgerApiConformanceBase {
  protected def lfVersion: UseLedgerApiTestTool.LfVersion = UseLedgerApiTestTool.LfVersion.Stable

  protected def lapittVersion: LAPITTVersion = LAPITTVersion.LocalJar

  protected val ledgerApiTestToolPlugin =
    new UseLedgerApiTestTool(
      loggerFactory,
      lfVersion = lfVersion,
      version = lapittVersion,
      connectedSynchronizersCount = connectedSynchronizersCount,
    )
  registerPlugin(ledgerApiTestToolPlugin)

  def runShardedTests(shard: Int, numShards: Int)(
      env: TestConsoleEnvironment
  ): String =
    ledgerApiTestToolPlugin.runShardedSuites(
      shard,
      numShards,
      exclude = excludedTests,
    )(env)
}

trait LedgerApiConformanceBase extends CommunityIntegrationTest with IsolatedEnvironments {

  protected def connectedSynchronizersCount: Int

  // ensure ledger api conformance tests have less noisy neighbours
  protected override def numPermits: PositiveInt = PositiveInt.tryCreate(2)
  /*
    When running the ProtocolContinuityConformance test, the protocol version can be different
    from the `testedProtocolVersion`.
   */
  protected def setupLedgerApiConformanceEnvironment(
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    require(env.environment.config.sequencers.sizeIs == connectedSynchronizersCount)

    implicit val e: TestConsoleEnvironment = env

    sequencer1_.topology.synchronizer_parameters.propose_update(
      daId,
      previous =>
        previous.update(
          confirmationResponseTimeout = NonNegativeFiniteDuration.ofMinutes(1),
          mediatorReactionTimeout = NonNegativeFiniteDuration.ofMinutes(1),
          ledgerTimeRecordTimeTolerance = NonNegativeFiniteDuration.ofMinutes(3),
          mediatorDeduplicationTimeout = NonNegativeFiniteDuration.ofMinutes(6),
          preparationTimeRecordTimeTolerance = NonNegativeFiniteDuration.ofMinutes(6 / 2),
          reconciliationInterval = PositiveDurationSeconds.ofSeconds(1),
        ),
    )

    participants.all.synchronizers.connect_local(sequencer1_, alias = daName)
  }

  def shutdownLedgerApiConformanceEnvironment(env: TestConsoleEnvironment): Unit = {
    import env.*
    implicit val e: TestConsoleEnvironment = env
    participants.all.synchronizers.disconnect("synchronizer1")
    participants.local.foreach(_.stop())
  }
}

class LedgerApiConformanceMultiSynchronizerTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments {

  private val connectedSynchronizersCount: Int = 2

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .withSetup(setupLedgerApiConformanceEnvironment)

  // ensure ledger api conformance tests have less noisy neighbours
  protected override def numPermits: PositiveInt = PositiveInt.tryCreate(2)

  protected def setupLedgerApiConformanceEnvironment(
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    implicit val e: TestConsoleEnvironment = env

    participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
  }

  protected val ledgerApiTestToolPlugin =
    new UseLedgerApiTestTool(
      loggerFactory,
      connectedSynchronizersCount = connectedSynchronizersCount,
      lfVersion = UseLedgerApiTestTool.LfVersion.Stable,
      version = LAPITTVersion.LocalJar,
    )
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(ledgerApiTestToolPlugin)
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  "Ledger API test tool on a multi-synchronizer setup" can {
    "pass multi-synchronizer related conformance tests" in { implicit env =>
      ledgerApiTestToolPlugin.runSuites(
        suites = LedgerApiConformanceBase.multiSynchronizerTests.mkString(","),
        exclude = Nil,
        concurrency = 2,
      )
    }
  }
}

class LedgerApiConformanceWithTrafficControlTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withTrafficControl()
      .withSetup(setupLedgerApiConformanceEnvironment(_))

  // ensure ledger api conformance tests have less noisy neighbours
  protected override def numPermits: PositiveInt = PositiveInt.tryCreate(2)

  protected def setupLedgerApiConformanceEnvironment(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    participants.all.synchronizers.connect_local(sequencer1, alias = daName)
  }

  protected val ledgerApiTestToolPlugin =
    new UseLedgerApiTestTool(
      loggerFactory,
      connectedSynchronizersCount = 1,
      lfVersion = UseLedgerApiTestTool.LfVersion.Stable,
      version = LAPITTVersion.LocalJar,
    )
  registerPlugin(ledgerApiTestToolPlugin)
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  "Ledger API test tool on a synchronizer with traffic control enabled" can {
    "pass traffic control related conformance tests" in { implicit env =>
      ledgerApiTestToolPlugin.runSuites(
        suites = LedgerApiConformanceBase.trafficControlTests.mkString(","),
        exclude = Nil,
        concurrency = 2,
      )
    }
  }
}

object LedgerApiConformanceBase {
  val multiSynchronizerTests = Seq(
    "CommandServiceIT:CSsubmitAndWaitPrescribedSynchronizerId",
    "CommandServiceIT:CSsubmitAndWaitForTransactionPrescribedSynchronizerId",
    "CommandSubmissionCompletionIT:CSCSubmitWithPrescribedSynchronizerId",
    "ExplicitDisclosureIT:EDFailOnDisclosedContractIdMismatchWithPrescribedSynchronizerId",
    "ExplicitDisclosureIT:EDRouteByDisclosedContractSynchronizerId",
    "VettingIT:PVListVettedPackagesMultiSynchronizer",
    "VettingIT:PVListVettedPackagesPagination",
  )
  val trafficControlTests = Seq(
    "InteractiveSubmissionServiceIT:ISSPrepareSubmissionRequestBasic",
    "InteractiveSubmissionServiceIT:ISSPrepareSubmissionRequestWithoutCostEstimation",
  )
  val excludedTests = Seq(
    "ClosedWorldIT", // Canton errors with "Some(Disputed: unable to parse party id 'unallocated': FailedSimpleStringConversion(LfError(Invalid unique identifier missing namespace unallocated)))"
    // Exclude tests which are run separately below
    "ParticipantPruningIT",
    "TLSOnePointThreeIT",
    "TLSAtLeastOnePointTwoIT",
    "CheckpointInTailingStreamsIT",
    // The following tests need pruning configuration, see LedgerApiParticipantPruningConformanceTest
    "ActiveContractsServiceIT:AcsAtPruningOffsetIsAllowed",
    "ActiveContractsServiceIT:AcsBeforePruningOffsetIsDisallowed",
    "CommandDeduplicationPeriodValidationIT:OffsetPruned",
    // Exclude ContractIdIT tests except: RejectNonSuffixedV1Cid, AcceptSuffixedV1Cid
    "ContractIdIT:AcceptNonSuffixedV1Cid",
    "ContractIdIT:AcceptSuffixedV1CidExerciseTarget", // Racy with: ABORTED: CONTRACT_NOT_FOUND(14,0): Contract could not be found with id
    "ContractIdIT:RejectNonSuffixedV1Cid", // With Daml 2.0.0-snapshot.20220124 started failing due to Actual error id (COMMAND_PREPROCESSING_FAILED) does not match expected error id
    "UserManagementServiceIT:RaceConditionGrantRights", // See LedgerApiConformanceSuppressedLogs
    "UserManagementServiceIT:RaceConditionCreateUsers", // See LedgerApiConformanceSuppressedLogs
    // Following value normalisation (https://github.com/digital-asset/daml/pull/19912), this throws a different, equally correct, error
    "CommandServiceIT:CSRefuseBadParameter",
  )
}

trait LedgerApiShardedConformanceBase extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup(setupLedgerApiConformanceEnvironment)

  protected val numShards: Int = 3
  protected def shard: Int

  "Ledger Api Test Tool" can {
    s"pass semantic tests block $shard" in { implicit env =>
      // suppress warnings for UserManagementServiceIT
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        runShardedTests(shard, numShards)(env)
      }
    }
  }
}

// Conformance test that need a suppressing rule on canton side
trait LedgerApiConformanceSuppressedLogs extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup(setupLedgerApiConformanceEnvironment)

  "Ledger Api Test Tool" can {
    "pass semantic tests block" in { implicit env =>
      /*
        UserManagementServiceIT:RaceConditionGrantRights logs:
        - An error on the daml side (violation of uniqueness constraint)
        - An error on canton (by the ApiRequestLogger)
       */
      loggerFactory.assertLogsSeq(SuppressionRule.Level(event.Level.ERROR))(
        ledgerApiTestToolPlugin.runSuitesSerially(
          suites = "UserManagementServiceIT:RaceConditionGrantRights",
          exclude = Nil,
        ),
        forEvery(_) {
          _.errorMessage should ((startWith(
            "Request c.d.l.a.v.a.UserManagementService/GrantUserRights"
          ) and include(
            s"failed with INTERNAL/${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API}"
          )) or include(
            "Processing the request failed due to a non-transient database error: ERROR: duplicate key value violates unique constraint"
          ))
        },
      )

      /*
        UserManagementServiceIT:RaceConditionCreateUsers logs:
        - An error on the daml side (violation of uniqueness constraint)
        - An error on canton (by the ApiRequestLogger)
       */
      loggerFactory.assertLogsSeq(SuppressionRule.Level(event.Level.ERROR))(
        ledgerApiTestToolPlugin.runSuitesSerially(
          suites = "UserManagementServiceIT:RaceConditionCreateUsers",
          exclude = Nil,
        ),
        forEvery(_) {
          _.errorMessage should ((startWith(
            "Request c.d.l.a.v.a.UserManagementService/CreateUser"
          ) and include(
            s"failed with INTERNAL/${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API}"
          )) or include(
            "Processing the request failed due to a non-transient database error: ERROR: duplicate key value violates unique constraint"
          ))
        },
      )
    }
  }
}

class LedgerApiConformanceSuppressedLogsPostgres extends LedgerApiConformanceSuppressedLogs {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiShard0ConformanceTest extends LedgerApiShardedConformanceBase {
  override def shard: Int = 0
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

class LedgerApiShard0ConformanceTestPostgres extends LedgerApiShard0ConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiShard1ConformanceTest extends LedgerApiShardedConformanceBase {
  override def shard: Int = 1
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

class LedgerApiShard1ConformanceTestPostgres extends LedgerApiShard1ConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiShard2ConformanceTest extends LedgerApiShardedConformanceBase {
  override def shard: Int = 2
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

class LedgerApiShard2ConformanceTestPostgres extends LedgerApiShard2ConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiExperimentalConformanceTest extends SingleVersionLedgerApiConformanceBase {

  override def lfVersion = UseLedgerApiTestTool.LfVersion.Dev

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup(setupLedgerApiConformanceEnvironment)

  "Ledger Api Test Tool" can {
    "pass experimental tests for 2.dev lf version" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      implicit env =>
        ledgerApiTestToolPlugin.runSuites(
          suites =
            "ContractKeysCommandDeduplicationIT,ContractKeysContractIdIT,ContractKeysDeeplyNestedValueIT," +
              "ContractKeysDivulgenceIT,ContractKeysExplicitDisclosureIT,ContractKeysMultiPartySubmissionIT," +
              "ContractKeysWronglyTypedContractIdIT,ContractKeysIT,RaceConditionIT,ExceptionsIT,ExceptionRaceConditionIT," +
              "EventsDescendantsIT,PrefetchContractKeysIT",
          exclude = Seq(
            // TODO(#16065)
            "ExceptionRaceConditionIT:RWRollbackCreateVsNonTransientCreate",
            "ExceptionRaceConditionIT:RWArchiveVsRollbackFailedLookupByKey",
            "ExceptionsIT:ExRollbackDuplicateKeyArchived",
            "ExceptionsIT:ExRollbackDuplicateKeyCreated",
            "ExceptionsIT:ExRollbackExerciseCreateLookup",
            // tests with divulged/disclosed contracts fail on Canton as does scoping by maintainer unless we're on a UCK synchronizer (see below)
            "ContractKeysIT:CKFetchOrLookup",
            "ContractKeysIT:CKMaintainerScoped",
            "ContractKeysIT:CKNoFetchUndisclosed",
            // tests with unique contract key assumption fail as does RWArchiveVsFailedLookupByKey (finding a lookup failure after contract creation)
            "RaceConditionIT:RWArchiveVsFailedLookupByKey",
            "RaceConditionIT:WWArchiveVsNonTransientCreate",
            "RaceConditionIT:WWDoubleNonTransientCreate",
            "RaceConditionIT:RWTransientCreateVsNonTransientCreate",
            // Exclude the "prepare endpoint" versions of contract key prefetching because
            // the external hashing algorithm for interactive submission only supports LF=2.1
            // When contract keys are not an LF dev feature anymore those can be enabled
            "PrefetchContractKeysIT:CSprefetchContractKeysPrepareEndpointBasic",
            "PrefetchContractKeysIT:CSprefetchContractKeysPrepareWronglyTyped",
            "PrefetchContractKeysIT:CSprefetchContractPrepareKeysMany",
          ),
          concurrency = 4,
        )
    }
  }
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

class LedgerApiExperimentalConformanceTest_Postgres extends LedgerApiExperimentalConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiParticipantPruningConformanceTest extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateMaxDeduplicationDurations(java.time.Duration.ofSeconds(1)),
        // Disable warnings about large ACS with consistency checking as the pruning tests create many contracts
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        ),
      )
      .withSetup(setupLedgerApiConformanceEnvironment)

  "On a synchronizer with expedited acs commitment reconciliation, Ledger Api Test Tool" can {
    "pass ParticipantPruning test" in { implicit env =>
      val suites = Seq(
        "ParticipantPruningIT",
        "CommandDeduplicationPeriodValidationIT:OffsetPruned",
        "ActiveContractsServiceIT:AcsAtPruningOffsetIsAllowed",
        "ActiveContractsServiceIT:AcsBeforePruningOffsetIsDisallowed",
      )

      ledgerApiTestToolPlugin.runSuitesSerially(
        suites = suites.mkString(","),
        exclude = Nil,
        kv = "--timeout-scale-factor" -> "4",
      )
    }
  }
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

class LedgerApiParticipantPruningConformanceTestPostgres
    extends LedgerApiParticipantPruningConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiOffsetCheckpointsConformanceTest extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.indexService.offsetCheckpointCacheUpdateInterval)
            .replace(config.NonNegativeFiniteDuration(java.time.Duration.ofMillis(1000)))
            .focus(_.ledgerApi.indexService.idleStreamOffsetCheckpointTimeout)
            .replace(config.NonNegativeFiniteDuration(java.time.Duration.ofMillis(1000)))
        )
      )
      .withSetup(setupLedgerApiConformanceEnvironment)

  "On a Ledger API with short offsetCheckpoint cache update interval, Ledger Api Test Tool" can {
    "pass CheckpointInTailingStreamsIT test" in { implicit env =>
      val suites = Seq(
        "CheckpointInTailingStreamsIT"
      )

      ledgerApiTestToolPlugin.runSuitesSerially(
        suites = suites.mkString(","),
        exclude = Nil,
      )
    }
  }
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

class LedgerApiOffsetCheckpointsConformanceTestPostgres
    extends LedgerApiOffsetCheckpointsConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

// simple class which can be used to test a single test in the Ledger API conformance suite
class LedgerApiSingleTest extends SingleVersionLedgerApiConformanceBase {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup(setupLedgerApiConformanceEnvironment)

  "Ledger Api Test Tool" can {
    "run a single test" in { implicit env =>
      ledgerApiTestToolPlugin.runSuites(
        suites = "PartyManagementServiceIT:PMGenerateExternalPartyTopologyTransaction",
        exclude = Nil,
        concurrency = 1,
      )
    }
  }
}
