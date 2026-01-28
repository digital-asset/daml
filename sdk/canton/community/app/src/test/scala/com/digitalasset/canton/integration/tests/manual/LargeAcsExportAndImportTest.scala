// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.*
import com.digitalasset.canton.config.RequireTypes
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  PostgresDumpRestore,
  UseBftSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.participant.admin.data.ContractImportMode
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.util.{MonadUtil, SingleUseCell}
import com.digitalasset.canton.{TempDirectory, config}
import monocle.Monocle.toAppliedFocusOps

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

/** Given a large active contract set (ACS), we want to test the ACS export and import.
  *
  * IMPORTANT: This does NOT implement a proper offline party replication as this is NOT the test
  * focus.
  *
  * The tooling also allows to generate "transient contracts": these are contracts that are created
  * and archived immediately (and thus don't show up in the ACS snapshot). This is useful to check
  * how such archived but not-yet pruned contracts impact performance of the export. Hence, it is
  * important to disable (background pruning).
  *
  * Raison d'être – Have this test code readily available in the repository for on-demand
  * performance investigations. Thus, we're not so much interested in actually creating a large ACS
  * and asserting a particular performance target; executed regularly on CI.
  *
  * Test setup:
  *   - Topology: 3 Participants (P1, P2, P3) with a single mediator and a single sequencer
  *   - P1 hosts party Bank, P2 hosts party Owner-0, ..., Owner-19
  *   - Bank creates a specified number of active IOU contracts with the owners
  *   - Bank authorizes to be hosted on P3
  *   - P1 exports Bank's ACS to a file
  *   - P3 imports Bank's ACS from the file
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
    with SharedEnvironment {

  /** Test definition, in particular how many active Iou contracts should be used */
  protected def testSet: TestSet

  protected case class TestSet(
      acsSize: Int,
      transientContracts: Int,
      name: String,
      directory: File,
      creationBatchSize: Int,
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

    def apply(acsSize: Int, transientContracts: Int): TestSet = {
      val testName = formatWithUnderscores(acsSize)
      val creationBatchSize = 500

      val testDirectory =
        File.currentWorkingDirectory / "tmp" / s"$testName-transient=${transientContracts}_LargeAcsExportAndImportTest"
      new TestSet(
        acsSize = acsSize,
        transientContracts = transientContracts,
        testName,
        testDirectory,
        creationBatchSize = creationBatchSize,
      )
    }
  }

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // A party replication is involved, and we want to minimize the risk of warnings related to acs commitment mismatches
  protected val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  protected val baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Manual
      .addConfigTransforms(ConfigTransforms.allDefaultsButGloballyUniquePorts*)
      .addConfigTransforms(
        // Hard-coded ports ensure connectivity across node restarts. To save time,
        // participants are restored from database dumps which contain persisted
        // sequencer configurations. Static ports are required so these restored
        // nodes can successfully reconnect.
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
        // Disable background pruning
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(config.NonNegativeFiniteDuration.ofDays(365 * 100))
        ),
        // Use distinct timeout values so that it is clear which timeout expired
        _.focus(_.parameters.timeouts.processing.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(31.minute)),
        _.focus(_.parameters.timeouts.processing.network)
          // Addresses c.d.c.r.DbLockedConnection...=participant2/connId=pool-2 - Task connection check read-only did not complete within 2 minutes.
          .replace(config.NonNegativeDuration.tryFromDuration(32.minute)),
        _.focus(_.parameters.timeouts.console.bounded)
          // Addresses import_acs GrpcClientGaveUp: DEADLINE_EXCEEDED/CallOptions in was 3 min for 100_000
          .replace(config.NonNegativeDuration.tryFromDuration(33.minute)),
        _.focus(_.parameters.timeouts.console.unbounded)
          // Defaults to 3 minutes for tests (not enough for 250_000)
          // Addresses import_acs GrpcClientGaveUp: DEADLINE_EXCEEDED/CallOptions for ParticipantAdministration$synchronizers$.reconnect in was 3 min for 100_000
          .replace(config.NonNegativeDuration.tryFromDuration(34.minute)),
        // Disable the warnings for enabled consistency checks as we're importing a large ACS
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        ),
      )
      // Disabling LAPI verification to reduce test termination time
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "participant1",
            "participant2",
            "participant3",
          )
        )
      )

  override protected def environmentDefinition: EnvironmentDefinition = baseEnvironmentDefinition

  // Need the persistence for dumping and restoring large ACS
  protected val referenceSequencer = new UseBftSequencer(loggerFactory)
  registerPlugin(referenceSequencer)
  protected val pgPlugin = new UsePostgres(loggerFactory)
  registerPlugin(pgPlugin)
  protected lazy val dumpRestore: PostgresDumpRestore =
    PostgresDumpRestore(pgPlugin, forceLocal = true)

  protected def testSetup()(implicit env: TestConsoleEnvironment): Unit = {
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

  protected def createContracts()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Enable parties
    val bank = participant1.parties.enable("Bank")
    val ownersCount = 20
    val owners = (0 until ownersCount).map(i => participant2.parties.enable(s"Owner-$i")).toVector

    // Create contracts
    val contractsDataset = Range.inclusive(1, testSet.acsSize)
    val batches = contractsDataset.grouped(testSet.creationBatchSize).toList
    val batchesCount = batches.size
    val transientContractsPerBatch =
      Math.ceil(testSet.transientContracts.toDouble / batchesCount).toInt

    // Round-robin on the owners
    val ownerIdx = new AtomicInteger(0)

    batches.foreach { batch =>
      val start = System.nanoTime()
      val iousCommands = batch.map { amount =>
        val owner = owners(ownerIdx.getAndIncrement() % ownersCount)

        IouSyntax.testIou(bank, owner, amount.toDouble).create.commands.loneElement
      }
      participant1.ledger_api.javaapi.commands.submit(Seq(bank), iousCommands)
      val ledgerEnd = participant1.ledger_api.state.end()
      val end = System.nanoTime()
      logger.info(
        s"Batch: ${batch.head} to ${batch.head + testSet.creationBatchSize} took ${TimeUnit.NANOSECONDS
            .toMillis(end - start)}ms and ledger end = $ledgerEnd"
      )

      // Transient contracts
      if (transientContractsPerBatch > 0) {
        val transientContractsCreateCmds =
          Seq.fill(transientContractsPerBatch)(100.0).map { amount =>
            val owner = owners(ownerIdx.getAndIncrement() % ownersCount)
            IouSyntax.testIou(bank, owner, amount).create.commands.loneElement
          }
        val chip = JavaDecodeUtil.decodeAllCreated(M.iou.Iou.COMPANION)(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(bank), transientContractsCreateCmds)
        )

        val archiveCmds = chip.map(_.id.exerciseArchive().commands().loneElement)

        participant1.ledger_api.javaapi.commands.submit(Seq(bank), archiveCmds)
      }
    }

    // Sanity checks
    participant1.ledger_api.state.acs
      .of_party(bank, limit = PositiveInt.MaxValue)
      .size shouldBe testSet.acsSize
    participant2.ledger_api.state.acs
      .of_all(limit = PositiveInt.MaxValue)
      .size shouldBe testSet.acsSize
  }
}

/** A "test" that first creates an ACS for Bank on P1 and the owners on P2, and then stores that
  * state as a database dump.
  *
  * Restoring a database dump for an ACS with 10'000 or more contracts is much faster than
  * (re)creating those active contracts for every test run (see [[LargeAcsExportAndImportTest]]).
  *
  * The number of created active contracts is defined by the [[TestSet]].
  */
protected abstract class DumpTestSet extends LargeAcsExportAndImportTestBase {
  override protected def environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition.withSetup { implicit env =>
      testSetup()
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
    Await.result(saveDumps(nodes), 30.minutes) // DB dump can be a bit slow
    nodes.foreach(_.start())
    participants.foreach(_.synchronizers.reconnect_all())
  }

  s"create ${testSet.acsSize} active contracts" in { implicit env =>
    createContracts()
  }

  "create database dump" in { implicit env =>
    import env.*

    createDatabaseDump(
      Seq(sequencer1),
      Seq(mediator1),
      Seq(participant1, participant2, participant3),
    )
  }
}

@nowarn("cat=deprecation") // Usage of old acs export
protected abstract class EstablishTestSet extends LargeAcsExportAndImportTestBase {
  // If true, use legacy export/import (Canton internal instead of LAPI)
  def useLegacyExportImport: Boolean
  def useV2ExportImport: Boolean

  protected def testContractIdImportMode: ContractImportMode

  // Replicate Bank from P1 to P3
  private val acsExportFile = new SingleUseCell[File]
  private val ledgerOffsetBeforePartyOnboarding = new SingleUseCell[Long]

  protected def restoreDump(node: LocalInstanceReference)(implicit
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
    val nodes = (participants: Seq[LocalInstanceReference]) ++ sequencers ++ mediators
    nodes.foreach(_.stop())

    clue("Restoring database") {
      Await.result(MonadUtil.sequentialTraverse_(nodes)(restoreDump(_)), 15.minutes)
    }

    clue("Starting all nodes") {
      nodes.foreach(_.start())
      new NetworkBootstrapper(networkTopologyDescriptions*).bootstrap()
      eventually(timeUntilSuccess = 2.minutes, maxPollInterval = 15.seconds) {
        nodes.forall(_.is_initialized)
      }
    }

    participants.foreach(_.synchronizers.reconnect_all())

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
      testSetup()
      createContracts()
    } else {
      loadState(
        Seq(sequencer1),
        Seq(mediator1),
        Seq(participant1, participant2, participant3),
        Seq(EnvironmentDefinition.S1M1),
      )
    }
  }

  "authorize Bank on P3" in { implicit env =>
    import env.*
    val bank = grabPartyId(participant1, "Bank")

    ledgerOffsetBeforePartyOnboarding.putIfAbsent(participant1.ledger_api.state.end())

    PartyToParticipantDeclarative.forParty(Set(participant1, participant3), daId)(
      participant1,
      bank,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant3, PP.Submission),
      ),
    )
  }

  // Replicate Bank from P1 to P3
  "export ACS for Bank from P1" in { implicit env =>
    import env.*

    val bank = grabPartyId(participant1, "Bank")

    acsExportFile.putIfAbsent(
      File.newTemporaryFile(
        parent = Some(testSet.exportDirectory),
        prefix = "LargeAcsTest_Bank_",
      )
    )

    if (useLegacyExportImport) {
      acsExportFile.get.foreach { acsExport =>
        participant1.repair.export_acs_old(
          Set(bank),
          partiesOffboarding = false,
          outputFile = acsExport.canonicalPath,
        )
      }
    } else {
      // no need to check the flag useV2ExportImport,
      // because the data exported via export_acs works for both import endpoints
      val bankAddedOnP3Offset = participant1.parties.find_party_max_activation_offset(
        partyId = bank,
        synchronizerId = daId,
        participantId = participant3.id,
        beginOffsetExclusive = ledgerOffsetBeforePartyOnboarding.getOrElse(
          throw new RuntimeException("missing begin offset")
        ),
        completeAfter = PositiveInt.one,
      )

      acsExportFile.get.foreach { acsExport =>
        participant1.repair.export_acs(
          parties = Set(bank),
          exportFilePath = acsExport.canonicalPath,
          ledgerOffset = bankAddedOnP3Offset,
        )
      }
    }
  }

  "import ACS for Bank on P3" in { implicit env =>
    import env.*

    val synchronizerId = participant1.synchronizers.list_registered().loneElement._2.toOption.value

    participant1.stop()
    participant2.stop()

    participant3.synchronizers.disconnect_all()

    acsExportFile.get.foreach { acsExportFile =>
      val startImport = System.nanoTime()

      if (useLegacyExportImport) {
        participant3.repair.import_acs(
          acsExportFile.canonicalPath,
          contractImportMode = testContractIdImportMode,
        )
      } else if (useV2ExportImport) {
        participant3.parties.import_party_acsV2(
          acsExportFile.canonicalPath,
          contractImportMode = testContractIdImportMode,
          synchronizerId = synchronizerId.logical,
        )
      } else {
        participant3.parties.import_party_acs(
          acsExportFile.canonicalPath,
          contractImportMode = testContractIdImportMode,
        )
      }

      val importDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startImport)

      importDurationMs should be < testSet.acsImportDurationBoundMs
    }
  }

  "reconnect P3" in { implicit env =>
    import env.*
    participant3.synchronizers.reconnect(daName)
  }

  "assert ACS on P3" in { implicit env =>
    import env.*

    participant3.testing.state_inspection
      .contractCountInAcs(daName, CantonTimestamp.now())
      .futureValueUS shouldBe Some(testSet.acsSize)
  }
}

/** Use this test to create a large ACS, and dump the test environment to file for subsequent
  * testing
  */
final class LargeAcsCreateContractsTest extends DumpTestSet {
  override protected def testSet: TestSet = TestSet(250000, transientContracts = 0)
}

/** The actual test */
final class LargeAcsExportAndImportTest extends EstablishTestSet {
  override protected def testSet: TestSet = TestSet(250000, transientContracts = 0)

  override def useLegacyExportImport: Boolean = false
  override def useV2ExportImport: Boolean = false

  override protected def testContractIdImportMode: ContractImportMode =
    ContractImportMode.Validation
}

/** The actual test */
final class LargeAcsExportAndImportTestV2 extends EstablishTestSet {
  override protected def testSet: TestSet = TestSet(250000, transientContracts = 0)

  override def useLegacyExportImport: Boolean = false
  override def useV2ExportImport: Boolean = true

  override protected def testContractIdImportMode: ContractImportMode =
    ContractImportMode.Validation
}

/** The actual test using legacy ACS export / import endpoints */
final class LargeAcsExportAndImportTestLegacy extends EstablishTestSet {
  override protected def testSet: TestSet = TestSet(10, transientContracts = 1)

  override def useLegacyExportImport: Boolean = true
  override def useV2ExportImport: Boolean = false

  override protected def testContractIdImportMode: ContractImportMode =
    ContractImportMode.Validation
}
