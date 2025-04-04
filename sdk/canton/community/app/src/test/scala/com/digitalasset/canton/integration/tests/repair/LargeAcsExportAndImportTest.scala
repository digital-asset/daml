// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, RequireTypes}
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  PostgresDumpRestore,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, PartyToParticipantDeclarative}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.data.ContractIdImportMode
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.util.{MonadUtil, SingleUseCell}
import com.digitalasset.canton.{TempDirectory, config}
import monocle.Monocle.toAppliedFocusOps

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Given a large active contract set (ACS), we want to test the ACS export and import.
  *
  * Raison d'être – Have this test code readily available in the repository for on-demand
  * performance investigations. Thus, we're not so much interested in actually creating a large ACS
  * and asserting a particular performance target; executed regularly on CI.
  *
  * Test setup:
  *   - Topology: 3 Participants (P1, P2, P3) with a single mediator and a single sequencer
  *   - P1 hosts party Alice, P2 hosts party Bob
  *   - Alice creates a specified number of active IOU contracts with Bob
  *   - Alice authorizes to be hosted on P3
  *   - P1 exports Alice's ACS to a file
  *   - P3 imports Alice's ACS from the file
  *
  * Above is implemented by [[LargeAcsExportAndImportTest]].
  *
  * Creating a large ACS is time-consuming. There's a commented-out test,
  * `LargeAcsCreateContractsTest`, that creates active contracts as specified by
  * [[TestSet.acsSize]], and then dumps the nodes' persisted state as database dump files. When dump
  * files are present for a [[TestSet.name]], then the [[LargeAcsExportAndImportTest]] restores them
  * at the beginning of its test execution. This allows for faster, repeated test executions.
  * Without dump files, the test creates the active contracts as required by [[TestSet.acsSize]]. –
  * That's being executed on CI.
  *
  * Hint: For testing with an ACS size of 10'000 active contracts or larger, you definitively want
  * to use a previously created database dump of the test network. Some example creation times
  * (developer notebook):
  *   - 1s for 1000 active contracts
  *   - 72s (1min 12s) for 10_000
  *   - 437s (7min 17s) for 100_000
  *   - 817s (13min 37s) for 200_000
  *   - 1229s (20min 29s) for 300_000
  *   - extrapolation: T ≈ 0.00404 * N + 17.5, where N = desired number of active contracts and T is
  *     in seconds
  *
  * Empirically, the software starts to misbehave / expose issues starting at 100'000 or more active
  * contracts.
  *
  * Some example times (developer notebook) as reference:
  * {{{
  * ACS size | dump restore [s] | acs_export [s] |           acs_import [s] |
  * 1000     |               17 |            0.1 |                        1 |
  * 10_000   |               21 |            0.7 |                        6 |
  * 100_000  |               42 |              4 |                      111 |
  * 150_000  |               53 |              7 |                      126 |
  * 200_000  |               65 |              9 |                      142 |
  * 300_000  |               87 |             15 |                      174 |
  * N        |                  |                |   T ≈ 0.00059 * N + 18.5 |
  * }}}
  */
protected abstract class LargeAcsExportAndImportTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with OperabilityTestHelpers {

  /** Test definition, in particular how many active IOU contracts should be used */
  protected def testSet: TestSet

  protected case class TestSet(
      acsSize: Int,
      name: String,
      directory: File,
      creationBatchSize: Int = 500,
  ) {
    def dumpDirectory: TempDirectory =
      TempDirectory((directory / "dump").createDirectoryIfNotExists(createParents = true))

    def isEmptyDumpDirectory = dumpDirectory.directory.list.isEmpty

    def exportDirectory: File =
      (directory / "export").createDirectoryIfNotExists(createParents = true)

    /** bound estimation (linearly fitted); tripling it as a safety margin */
    def acsImportDurationBoundMs: Long = (3 * (0.59 * acsSize + 18500)).toLong
  }

  protected object TestSet {

    /** 1000 -> "1_000", 1000000 -> "1_000_000" */
    def formatWithUnderscores(number: Int): String =
      number.toString.reverse.grouped(3).mkString("_").reverse

    def apply(acsSize: Int) = {
      val testName = formatWithUnderscores(acsSize)
      val testDirectory =
        File.currentWorkingDirectory / "tmp" / s"${testName}_LargeAcsExportAndImportTest"
      new TestSet(acsSize, testName, testDirectory)
    }
  }

  // A party replication is involved, and we want to minimize the risk of warnings related to acs commitment mismatches
  protected val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  protected val baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_Manual
      .clearConfigTransforms() // Disable globally unique ports
      .addConfigTransforms(ConfigTransforms.allDefaultsButGloballyUniquePorts*)
      .addConfigTransforms(
        // Adding hard-coded ports because reconnecting to a synchronizer requires the synchronizer
        // ports to be the same, and we do so across node restarts as part of the tests
        ConfigTransforms.updateSequencerConfig("sequencer1")(cfg =>
          cfg
            .focus(_.publicApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9018)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9019)))
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9011)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9012)))
        ),
        ConfigTransforms.updateParticipantConfig("participant2")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9021)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9022)))
        ),
        ConfigTransforms.updateParticipantConfig("participant3")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9031)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9032)))
        ),
      )
      // Use distinct timeout values so that it is clear which timeout expired
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(31.minute))
      )
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.network)
          .replace(
            config.NonNegativeDuration.tryFromDuration(32.minute)
          ) // Addresses c.d.c.r.DbLockedConnection...=participant2/connId=pool-2 - Task connection check read-only did not complete within 2 minutes.
      )
      .addConfigTransform(
        _.focus(_.parameters.timeouts.console.bounded)
          .replace(
            config.NonNegativeDuration.tryFromDuration(33.minute)
          ) // Addresses import_acs GrpcClientGaveUp: DEADLINE_EXCEEDED/CallOptions in was 3 min for 100_000
      )
      .addConfigTransform(
        _.focus(
          _.parameters.timeouts.console.unbounded
        ) // Defaults to 3 minutes for tests (not enough for 250_000)
          .replace(
            config.NonNegativeDuration.tryFromDuration(34.minute)
          ) // Addresses import_acs GrpcClientGaveUp: DEADLINE_EXCEEDED/CallOptions for ParticipantAdministration$synchronizers$.reconnect in was 3 min for 100_000
      )

  override protected def environmentDefinition: EnvironmentDefinition = baseEnvironmentDefinition

  // Need the persistence for dumping and restoring large ACS
  protected val referenceSequencer = new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
    loggerFactory
  )
  registerPlugin(
    referenceSequencer
  )
  protected val pgPlugin = new UsePostgres(loggerFactory)
  registerPlugin(pgPlugin)
  protected lazy val dumpRestore: PostgresDumpRestore =
    PostgresDumpRestore(pgPlugin, forceLocal = true)

  protected def testSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    Seq(participant1, participant2, participant3).foreach(_.start())
    sequencers.local.start()
    mediators.local.start()

    NetworkBootstrapper(Seq(EnvironmentDefinition.S1M1))(env).bootstrap()

    Seq(participant1, participant2, participant3).foreach { participant =>
      participant.synchronizers.connect_local(sequencer1, daName)
      participant.dars.upload(CantonExamplesPath).discard
    }

    sequencer1.topology.synchronizer_parameters
      .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
  }

  protected def grabPartyId(participant: LocalParticipantReference, name: String): PartyId =
    clue(s"Grabbing party $name on ${participant.name}")(
      participant.parties
        .hosted(filterParty = name)
        .headOption
        .map(_.party)
        .valueOrFail(s"Where is party $name?")
    )

  protected def createContracts(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Enable parties
    participant1.parties.enable(
      "Alice"
    )
    participant2.parties.enable(
      "Bob"
    )

    // Create contracts
    val alice = grabPartyId(participant1, "Alice")
    val bob = grabPartyId(participant2, "Bob")

    val dataset = Range.inclusive(1, testSet.acsSize)
    val batches: Iterator[Seq[Int]] = dataset.grouped(testSet.creationBatchSize)

    batches.foreach { batch =>
      val start = System.nanoTime()
      val iousCommands = batch.flatMap { amount =>
        IouSyntax.testIou(alice, bob, amount.toDouble).create.commands.asScala.toSeq
      }
      participant1.ledger_api.javaapi.commands.submit(Seq(alice), iousCommands)
      participant1.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(alice).discard
      val ledgerEnd = participant1.ledger_api.state.end()
      val end = System.nanoTime()
      logger.info(
        s"Batch: ${batch.head} to ${batch.head + testSet.creationBatchSize} took ${TimeUnit.NANOSECONDS
            .toMillis(end - start)}ms and ledger end = $ledgerEnd"
      )
    }

    val aliceOnP1AcsSize =
      participant1.ledger_api.state.acs.of_party(alice, limit = PositiveInt.MaxValue).size
    withClue(s"Alice on P1 ACS size: $aliceOnP1AcsSize")(
      aliceOnP1AcsSize shouldBe testSet.acsSize
    )

    val bobOnP2AcsSize =
      participant2.ledger_api.state.acs.of_party(bob, limit = PositiveInt.MaxValue).size
    withClue(s"Bob on P2 ACS size: $bobOnP2AcsSize")(
      bobOnP2AcsSize shouldBe testSet.acsSize
    )
  }
}

/** A "test" that first creates an ACS for Alice and Bob on P1 and P2, and then stores that state as
  * a database dump.
  *
  * Restoring a database dump for an ACS with 10'000 or more contracts is much faster than
  * (re)creating those active contracts for every test run (see [[LargeAcsExportAndImportTest]]).
  *
  * The number of created active contracts is defined by the [[TestSet]].
  */
protected abstract class DumpTestSet extends LargeAcsExportAndImportTestBase {

  override protected def environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition.withSetup { implicit env =>
      testSetup
    }

  private def saveDumps(
      nodes: Seq[LocalInstanceReference]
  )(implicit
      env: TestConsoleEnvironment,
      executionContext: ExecutionContext,
  ): Future[Unit] =
    MonadUtil.sequentialTraverse_(nodes) { node =>
      val filename = s"${node.name}.pg_dump"
      dumpRestore.saveDump(node, testSet.dumpDirectory.toTempFile(filename))
    }

  private def createDatabaseDump(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
  )(implicit env: TestConsoleEnvironment, executionContext: ExecutionContext): Unit = {
    val nodes = env.mergeLocalInstances(participants, sequencers, mediators)

    // Stop participants, then synchronizer (order intentional)
    nodes.foreach(_.stop())
    val res =
      for {
        _ <- saveDumps(nodes)
        _ <- referenceSequencer.dumpDatabases(
          testSet.dumpDirectory,
          forceLocal = true,
        )
      } yield ()
    Await.result(res, 30.minutes) // DB dump can be a bit slow
    nodes.foreach(_.start())
    participants.foreach(_.synchronizers.reconnect_all())
  }

  s"create ${testSet.acsSize} active contracts" in { implicit env =>
    createContracts
  }

  s"create database dump" in { implicit env =>
    import env.*

    createDatabaseDump(
      Seq(sequencer1),
      Seq(mediator1),
      Seq(participant1, participant2, participant3),
    )
  }
}

protected abstract class EstablishTestSet extends LargeAcsExportAndImportTestBase {

  protected def testContractIdImportMode: ContractIdImportMode

  private def restoreDump(node: LocalInstanceReference)(implicit
      env: TestConsoleEnvironment
  ): Future[Unit] = {
    val filename = s"${node.name}.pg_dump"
    dumpRestore.restoreDump(node, (testSet.dumpDirectory / filename).path)
  }

  private def loadState(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
      networkTopologyDescriptions: Seq[NetworkTopologyDescription],
  )(implicit
      env: TestConsoleEnvironment,
      executionContext: ExecutionContext,
  ): Unit = handleStartupLogs {
    val nodes =
      (participants: Seq[LocalInstanceReference]) ++
        (sequencers: Seq[LocalInstanceReference]) ++
        (mediators: Seq[LocalInstanceReference])
    nodes.foreach(_.stop())
    clue("Restoring database") {
      val res =
        for {
          _ <- MonadUtil.sequentialTraverse_(nodes)(restoreDump(_))
          _ <- referenceSequencer.restoreDatabases(
            testSet.dumpDirectory,
            forceLocal = true,
          )
        } yield ()
      Await.result(res, 15.minutes)
    }
    clue("Starting all nodes") {
      nodes.foreach(_.start())
      new NetworkBootstrapper(networkTopologyDescriptions*).bootstrap()
      eventually(timeUntilSuccess = 2.minutes, maxPollInterval = 15.seconds) {
        nodes.forall(_.is_initialized)
      }
    }
    clue("Reconnecting synchronizers")(
      participants.foreach(_.synchronizers.reconnect_all())
    )
    clue(
      "Ensure that all participants are up-to-date with the state of the topology " +
        "at the given time as returned by the synchronizer"
    )(
      participants.foreach(_.testing.fetch_synchronizer_times())
    )
    clue("Successfully loaded dumps for all nodes")(
      participants.head.health.ping(participants.head.id)
    )
  }

  s"restore database dump or create contracts for ${testSet.name}" in { implicit env =>
    import env.*

    if (testSet.isEmptyDumpDirectory) {
      testSetup
      createContracts
    } else {
      loadState(
        Seq(sequencer1),
        Seq(mediator1),
        Seq(participant1, participant2, participant3),
        Seq(EnvironmentDefinition.S1M1),
      )
    }
  }

  val ledgerOffsetBeforePartyOnboarding = new SingleUseCell[Long]

  "authorize Alice on P3" in { implicit env =>
    import env.*
    val alice = grabPartyId(participant1, "Alice")

    ledgerOffsetBeforePartyOnboarding.putIfAbsent(participant1.ledger_api.state.end())

    PartyToParticipantDeclarative.forParty(Set(participant1, participant3), daId)(
      participant1,
      alice,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant3, PP.Submission),
      ),
    )
  }

  // Replicate Alice from P1 to P3

  val acsExportFile = new SingleUseCell[File]

  "export ACS for Alice from P1" in { implicit env =>
    import env.*

    val alice = grabPartyId(participant1, "Alice")

    val aliceAddedOnP3Offset = participant1.parties.find_party_max_activation_offset(
      partyId = alice,
      synchronizerId = daId,
      participantId = participant3.id,
      beginOffsetExclusive = ledgerOffsetBeforePartyOnboarding.getOrElse(
        throw new RuntimeException("missing begin offset")
      ),
      completeAfter = PositiveInt.one,
    )

    acsExportFile.putIfAbsent(
      File.newTemporaryFile(
        parent = Some(testSet.exportDirectory),
        prefix = "LargeAcsTest_Alice_",
      )
    )

    acsExportFile.get.foreach { acsExport =>
      clue("Get Alice ACS on P1 with export_acs")(
        participant1.parties.export_acs(
          parties = Set(alice),
          exportFilePath = acsExport.canonicalPath,
          ledgerOffset = aliceAddedOnP3Offset,
        )
      )
    }

  }

  "import ACS for Alice on P3" in { implicit env =>
    import env.*

    participant3.synchronizers.disconnect_all()

    acsExportFile.get.foreach { acsExportFile =>
      val startImport = System.nanoTime()
      clue("Import Alice ACS on P3")(
        participant3.repair.import_acs(
          acsExportFile.canonicalPath,
          contractIdImportMode = testContractIdImportMode,
        )
      )
      val importDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startImport)

      withClue("ACS import duration in milliseconds") {
        importDurationMs should be < testSet.acsImportDurationBoundMs
      }
    }

    clue(s"Reconnect P3")(
      participant3.synchronizers.reconnect(daName)
    )

    clue(s"Assert ACS on P3") {
      participant3.testing.state_inspection
        .contractCountInAcs(daName, CantonTimestamp.now())
        .futureValueUS shouldBe Some(testSet.acsSize)
    }
  }

}

/** Use this test to create a large ACS, and dump the test environment to file for subsequent
  * testing
  */
//final class LargeAcsCreateContractsTest extends DumpTestSet {
//  override protected def testSet: TestSet = TestSet(1000)
//}

/** The actual test */
final class LargeAcsExportAndImportTest extends EstablishTestSet {
  override protected def testSet: TestSet = TestSet(1000)

  override protected def testContractIdImportMode: ContractIdImportMode =
    ContractIdImportMode.Validation
}
