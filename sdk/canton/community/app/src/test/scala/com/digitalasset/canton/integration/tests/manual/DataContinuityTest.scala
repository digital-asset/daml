// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.*
import com.daml.ledger.api.v2.CommandsOuterClass
import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel.Ignore
import com.digitalasset.canton.config.{DbConfig, RequireTypes, StorageConfig}
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.examples.java.{cycle as C, trailingnone as T}
import com.digitalasset.canton.integration.EnvironmentDefinition.{S1M1, S1M1_S1M1}
import com.digitalasset.canton.integration.IntegrationTestUtilities.grabCounts
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.tests.*
import com.digitalasset.canton.integration.tests.benchmarks.BongTestScenarios
import com.digitalasset.canton.integration.tests.manual.DataContinuityTest.*
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.ContinuityDumpRef
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  HasCycleUtils,
  HasTrailingNoneUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, TracedLogger}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BinaryFileUtil, MonadUtil, Mutex}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory, TempDirectory, config}
import monocle.macros.syntax.lens.*
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.nio.file.Files
import java.util.{Calendar, TimeZone}
import scala.annotation.unused
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*
import scala.sys.process.*

/** These tests aim to ensure that a customer can upgrade a Canton component within a major version
  * (e.g. from 1.1.2 to 1.3.0) by just replacing the binary and pointing it to the same database
  * without any data corruption/loss of data (in other words: data continuity should hold).
  *
  * To achieve this, each scenario is split into two tests: Test 1 (located in
  * CreateDataContinuityDumps.scala): Using the current version (all version digits including
  * suffix), initialize some state (e.g. create some contracts) and then save the state of the
  * running Canton nodes in DB dumps. IMPORTANT: In general, these dumps should only be
  * automatically generated as part of a release and not manually, e.g., by running these tests
  * locally. Only generate new DB dumps manually if you know what you are doing.
  *
  * Test 2 (located in this file): For each DB dump directory that has the same major version as the
  * current version, load all dumps in the directory and initialize the corresponding Canton nodes
  * with them. Then execute some operations (e.g. execute a choice on the contract that was created
  * in Test 1) and validate that the nodes behave as expected.
  *
  * The dumps are done either locally or inside a docker container (if a TestContainer is
  * available). We try to have these two flows as similar as possible. In particular, we use the
  * same directory for the dumps (see `DataContinuityTest.baseDbDumpPath`).
  *
  * See CONTRIBUTING.md for steps to take if these tests fail
  */
trait DataContinuityTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfterAll
    with HasTempDirectory
    with HasExecutionContext
    with S3Synchronization {
  implicit def plugin: EnvironmentSetupPlugin

  protected val referenceBlockSequencerPlugin: UseReferenceBlockSequencer[
    ? <: StorageConfig
  ]

  def dumpRestore: DbDumpRestore

  protected def dbName: String // e.g., postgres
  protected def dumpFileExtension: String

  // Set to true if you want to persist the dumps locally even if a test container is found
  lazy val forceLocalDumps = false

  override val logsToBeHandledAtStartup: Option[Seq[LogEntry] => Assertion] = Some(
    LogEntry.assertLogSeq(
      Seq.empty,
      // Handle db backend initialization delay
      Seq(e => assert(e.warningMessage.contains("Failed to initialize pool"))),
    )
  )

  protected def withNewProtocolVersion(
      oldEnv: TestConsoleEnvironment,
      protocolVersion: ProtocolVersion,
  )(f: TestConsoleEnvironment => Unit): Unit = {
    oldEnv.nodes.local.foreach(_.stop())
    val newEnv = manualCreateEnvironment(
      initialConfig = oldEnv.environment.config,
      configTransform = config =>
        ConfigTransforms
          .setProtocolVersion(protocolVersion)
          .foldLeft(config)((newConfig, transform) => transform(newConfig)),
      // as for outer env the verification is disabled as not applicable, here it is explicitly enabled
      testConfigTransform = _.copy(participantsWithoutLapiVerification = Set.empty),
    )
    try {
      logger.info(s"About to run with protocol version $protocolVersion")
      f(newEnv)
    } finally {
      // we need to properly destroy the environment here in order to ensure that the db is wiped.
      destroyEnvironment(newEnv)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val local = DataContinuityTest.localBaseDbDumpPath
    // Files in DataContinuityTest.localBaseDbDumpPath are copied to
    // DataContinuityTest.baseDbDumpPath (inside /tmp/), either locally
    // or inside the docker image.
    local.list.toList.foreach(dumpRestore.copy(_, DataContinuityTest.baseDbDumpPath))
    referenceBlockSequencerPlugin.pgPlugin.foreach { plugin =>
      val dumpRestore = PostgresDumpRestore(plugin, forceLocal = forceLocalDumps)
      local.list.toList.foreach(dumpRestore.copy(_, DataContinuityTest.baseDbDumpPath))
    }
  }

  protected def saveDumps(
      nodes: Seq[LocalInstanceReference],
      protocolVersion: ProtocolVersion,
  )(implicit folderName: FolderName, env: TestConsoleEnvironment): Future[Unit] =
    MonadUtil.sequentialTraverse_(nodes) { node =>
      val (directory, filename) = getDumpSavePath(node.name, protocolVersion)
      dumpRestore.saveDump(node, directory.toTempFile(filename))
    }

  protected def restoreDump(node: LocalInstanceReference, dumpDirectory: File)(implicit
      env: TestConsoleEnvironment,
      testFolderName: FolderName,
  ): Future[Unit] = {
    val filename = s"${node.name}.$dumpFileExtension"
    dumpRestore.restoreDump(node, (dumpDirectory / testFolderName.name / dbName / filename).path)
  }

  protected def getDumpSaveDirectory(protocolVersion: ProtocolVersion)(implicit
      folderName: FolderName
  ): TempDirectory =
    DataContinuityTest.baseDumpForVersion(protocolVersion) / folderName.name / dbName

  protected def getDumpSaveConfigDirectory: TempDirectory =
    DataContinuityTest.baseDumpForRelease / "config"

  protected def getDisclosureSaveDirectory(protocolVersion: ProtocolVersion)(implicit
      folderName: FolderName
  ): TempDirectory =
    DataContinuityTest.baseDumpForVersion(protocolVersion) / folderName.name

  protected def getDumpSavePath(nodeName: String, protocolVersion: ProtocolVersion)(implicit
      folderName: FolderName
  ): (TempDirectory, String) =
    (getDumpSaveDirectory(protocolVersion), nodeName + s".$dumpFileExtension")

  protected def getDisclosureSavePath(disclosureName: String, protocolVersion: ProtocolVersion)(
      implicit folderName: FolderName
  ): (TempDirectory, String) =
    (getDisclosureSaveDirectory(protocolVersion), disclosureName + s"-disclosure.binpb")

  protected def noDumpFilesOfConfiguredVersionExist(
      nodes: Seq[LocalInstanceReference],
      protocolVersion: ProtocolVersion,
  )(implicit folderName: FolderName): Assertion = {
    val (directories, expectedFiles) =
      nodes.map(node => getDumpSavePath(node.name, protocolVersion)).unzip
    val foundFiles =
      directories.distinct
        .flatMap(tempDir => dumpRestore.listFiles(tempDir.directory))
        .distinct
        .map(_.name)

    clue(s"directories ${directories.distinct} should not contain dump $expectedFiles") {
      forAll(expectedFiles) { file =>
        foundFiles should not contain file
      }
    }
  }

  protected def dumpStateOfConfiguredVersion(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
      protocolVersion: ProtocolVersion,
  )(implicit folderName: FolderName, env: TestConsoleEnvironment): Unit = {
    val nodes = env.mergeLocalInstances(participants, sequencers, mediators)

    // stop participants, then synchronizer (order intentional)
    nodes.foreach(_.stop())
    val res =
      for {
        _ <- saveDumps(nodes, protocolVersion)
        _ <- referenceBlockSequencerPlugin.dumpDatabases(getDumpSaveDirectory(protocolVersion))
      } yield ()
    Await.result(res, 30.minutes) // DB dump can be a bit slow
    nodes.foreach(_.start())
    participants.foreach(_.synchronizers.reconnect_all())
  }

  protected def dumpDisclosure(
      disclosure: CommandsOuterClass.DisclosedContract,
      protocolVersion: ProtocolVersion,
      disclosureName: String,
  )(implicit folderName: FolderName): Unit = {
    val (directory, filename) = getDisclosureSavePath(disclosureName, protocolVersion)
    BinaryFileUtil.writeByteStringToFile(
      directory.toTempFile(filename).toString,
      disclosure.toByteString,
    )
  }

  protected def loadState(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
      networkTopologyDescriptions: Seq[NetworkTopologyDescription],
      dumpDirectory: File,
  )(implicit
      env: TestConsoleEnvironment,
      folderName: FolderName,
  ): Unit = handleStartupLogs {
    val nodes =
      (participants: Seq[LocalInstanceReference]) ++
        (sequencers: Seq[LocalInstanceReference]) ++
        (mediators: Seq[LocalInstanceReference])
    nodes.foreach(_.stop())
    val res =
      for {
        _ <- MonadUtil.sequentialTraverse_(nodes)(restoreDump(_, dumpDirectory))
        _ <- referenceBlockSequencerPlugin.restoreDatabases(
          TempDirectory(dumpDirectory) / folderName.name / dbName
        )
      } yield ()
    Await.result(res, 15.minutes)
    nodes.foreach(_.db.migrate())
    nodes.foreach(_.start())
    new NetworkBootstrapper(networkTopologyDescriptions*).bootstrap()
    participants.foreach(_.synchronizers.reconnect_all())
    // Ensure that all participants are up-to-date with the state of
    // the topology at the given time as returned by the synchronizer
    participants.foreach(_.testing.fetch_synchronizer_times())
    logger.info("Successfully loaded dumps for all nodes")
  }

  protected def loadDisclosure(
      disclosureName: String,
      dumpDirectory: File,
  )(implicit testFolderName: FolderName): CommandsOuterClass.DisclosedContract =
    CommandsOuterClass.DisclosedContract.parseFrom(
      Files.readAllBytes(
        (dumpDirectory / testFolderName.name / s"$disclosureName-disclosure.binpb").path
      )
    )

  protected def ensureDumpsDontExist(): Unit = {
    val nonEmptyDirectories = Seq(
      DataContinuityTest.localBaseDbDumpPath / DataContinuityTest.releaseVersion.fullVersion,
      DataContinuityTest.baseDbDumpPath.directory / DataContinuityTest.releaseVersion.fullVersion,
    ).filter(_.exists)

    if (nonEmptyDirectories.nonEmpty)
      throw new RuntimeException(
        s"Data continuity dumps should be empty. Dumps found in directories: $nonEmptyDirectories"
      )
  }
}

trait DataContinuityTestFixturePostgres extends DataContinuityTest {
  implicit lazy val plugin: UsePostgres = new UsePostgres(loggerFactory)

  lazy val dumpRestore: PostgresDumpRestore =
    PostgresDumpRestore(plugin, forceLocal = forceLocalDumps)

  val dumpFileExtension: String = "pg_dump"
  val dbName: String = "postgres"

  override def beforeAll(): Unit = {
    super.beforeAll()

    ensureDBUsernameIsTest()
  }

  private def ensureDBUsernameIsTest(): Unit =
    assert(
      plugin.getDbUsernameOrThrow == "test",
      "You need to use the DB username `test` when using the data continuity tests because otherwise the tests will fail on CI. " +
        "Most likely you changed the username as described in `UsePostgres`. " +
        "You can disable this check if you only care about running the tests locally. ",
    )
}

trait BasicDataContinuityTestEnvironment extends CommunityIntegrationTest with SharedEnvironment {

  protected val referenceBlockSequencerPlugin =
    new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  registerPlugin(referenceBlockSequencerPlugin)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .clearConfigTransforms() // to disable globally unique ports
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
      )
      .addConfigTransforms(
        // We don't need it and it is one less port to worry about
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.httpLedgerApi.enabled).replace(false)
        )
      )
      .addConfigTransforms(ConfigTransforms.setBetaSupport(testedProtocolVersion.isBeta)*)
      .addConfigTransform(ConfigTransforms.setStartupMemoryReportLevel(Ignore))
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            // as these participants are not actually used in the tests (new environment will be spawned) verification is disabled
            // please note: these will be tested for the spawned environment (this test config will be overridden)
            "participant1",
            "participant2",
          )
        )
      )
}

trait BasicDataContinuityTestSetup
    extends DataContinuityTest
    with BasicDataContinuityTestEnvironment
    with HasCycleUtils
    with HasTrailingNoneUtils
    with BongTestScenarios
    with BeforeAndAfterAll
    with EntitySyntax {
  override val defaultParticipant = "participant1"
}

trait BasicDataContinuityTest extends BasicDataContinuityTestSetup {

  def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)]

  "Data continuity with simple contracts" should {
    implicit val folder: FolderName = FolderName("0-simple")
    "act as expected when loading the saved DB dumps" in { env =>
      dumpDirectories().foreach { case (dumpDirectory, protocolVersion) =>
        withNewProtocolVersion(env, protocolVersion) { implicit newEnv =>
          import newEnv.*
          logger.info("Testing dumps found in directory " + dumpDirectory)
          loadState(
            Seq(sequencer1),
            Seq(mediator1),
            Seq(participant1, participant2),
            Seq(S1M1),
            dumpDirectory.localDownloadPath,
          )
          val alice = participant1.parties.list(filterParty = "Alice").headOption.value.party
          val bob = participant1.parties.list(filterParty = "Bob").headOption.value.party
          actOnCycleData(alice)

          actOnDisclosedContract(
            alice,
            bob,
            DisclosedContract.fromProto(
              loadDisclosure("trailing-none", dumpDirectory.localDownloadPath)
            ),
          )
        }
      }

      @unused
      def actOnCycleData(alice: PartyId)(implicit env: TestConsoleEnvironment): Unit = {
        import env.*
        // act on state - executing the cycle contract on participant2
        val coid = participant2.ledger_api.javaapi.state.acs.await(C.Cycle.COMPANION)(alice)

        val cycleEx = coid.id.exerciseArchive().commands.asScala.toSeq

        participant2.ledger_api.javaapi.commands.submit(Seq(alice), cycleEx)
        eventually() {
          participant2.ledger_api.state.acs
            .of_party(alice)
            .filter(_.templateId.isModuleEntity("Cycle", "Cycle")) shouldBe empty
        }
      }

      def actOnDisclosedContract(
          alice: PartyId,
          bob: PartyId,
          disclosure: DisclosedContract,
      )(implicit env: TestConsoleEnvironment): Unit = {
        import env.*

        val coid = new T.TrailingNone.ContractId(disclosure.contractId.asScala.value)
        val cmd =
          coid.exerciseArchiveMe(new T.ArchiveMe(alice.toProtoPrimitive)).commands.asScala.toSeq
        // Alice is not a signatory of coid: Bob is. The contract isn't even hosted on participant 2 because
        // Bob is only authorized on participant 1. But because we pass the contract as a disclosure, the
        // exercise is expected to succeed.
        // This situation is meant to force participant 2 to read the disclosure's content instead of possibly
        // retrieving coid from the contract store as it could choose to do: we want to test the decoding of
        // disclosures written by previous versions of canton.
        participant2.ledger_api.javaapi.commands
          .submit(Seq(alice), cmd, disclosedContracts = Seq(disclosure))
        eventually() {
          participant1.ledger_api.state.acs
            .of_party(bob)
            .filter(_.templateId.isModuleEntity("TrailingNone", "TrailingNone")) shouldBe empty
        }
      }
    }
  }

  "Data continuity with BongScenario from BongTestScenarios" should {
    implicit val folder: FolderName = FolderName("3-bong")
    "act as expected when loading the saved DB dumps" in { env =>
      dumpDirectories().foreach { case (dumpDirectory, protocolVersion) =>
        withNewProtocolVersion(env, protocolVersion) { implicit newEnv =>
          import newEnv.*
          logger.info("Testing dumps from dump directory " + dumpDirectory)
          loadState(
            Seq(sequencer1),
            Seq(mediator1),
            Seq(participant1, participant2),
            Seq(S1M1),
            dumpDirectory.localDownloadPath,
          )
          // initialize needed state - sadly unable to decouple this from implementation details of the workflow
          val p1_count = grabCounts(daName, participant1)
          val p2_count = grabCounts(daName, participant2)
          runBongTest(p1_count, p2_count, 3)
        }
      }
    }
  }
}

trait SynchronizerChangeDataContinuityTestSetup
    extends AbstractSynchronizerChangeRealClockIntegrationTest
    with DataContinuityTest
    with HasExecutionContext {

  override protected val referenceBlockSequencerPlugin =
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )

  registerPlugin(referenceBlockSequencerPlugin)
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  // overriding this to disable globally unique ports
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5_S1M1_S1M1_Manual
      .clearConfigTransforms()
      .addConfigTransforms(ConfigTransforms.allDefaultsButGloballyUniquePorts*)
      .addConfigTransform(simClockTransform)
      // required such that late message processing warning isn't emitted
      .addConfigTransform(cfg =>
        cfg
          .focus(_.monitoring.logging.delayLoggingThreshold)
          .replace(config.NonNegativeFiniteDuration.ofDays(100))
      )
      .addConfigTransform(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory)
      )
      .addConfigTransforms(
        ConfigTransforms.updateSequencerConfig("sequencer1")(cfg =>
          cfg
            .focus(_.publicApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11018)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11019)))
        ),
        ConfigTransforms.updateSequencerConfig("sequencer2")(cfg =>
          cfg
            .focus(_.publicApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11028)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11029)))
        ),
        ConfigTransforms.updateMediatorConfig("mediator1")(cfg =>
          cfg
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11038)))
        ),
        ConfigTransforms.updateMediatorConfig("mediator2")(cfg =>
          cfg
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11048)))
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11011)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11012)))
        ),
        ConfigTransforms.updateParticipantConfig("participant2")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11021)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11022)))
        ),
        ConfigTransforms.updateParticipantConfig("participant3")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11031)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11032)))
        ),
        ConfigTransforms.updateParticipantConfig("participant4")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11041)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11042)))
        ),
        ConfigTransforms.updateParticipantConfig("participant5")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11051)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(11052)))
        ),
      )
      .addConfigTransforms(
        // We don't need it and it is one less port to worry about
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.httpLedgerApi.enabled).replace(false)
        )
      )
      .addConfigTransform(ConfigTransforms.setStartupMemoryReportLevel(Ignore))
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            // as these participants are not actually used in the tests (new environment will be spawned) verification is disabled
            // please note: these will be tested for the spawned environment (this test config will be overridden)
            "participant1",
            "participant2",
            "participant3",
            "participant4",
            "participant5",
          )
        )
      )

}

// Extra class because SynchronizerChangeDataContinuityTest use an elaborate set-up with 5 participants and 2 synchronizers
// which isn't needed for the other data continuity tests/would clash with them
trait SynchronizerChangeDataContinuityTest extends SynchronizerChangeDataContinuityTestSetup {
  import DataContinuityTest.*

  def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)]

  "Data continuity when reassignment of PaintOffer is started and then continued" should {
    implicit val folder: FolderName = FolderName("4-synchronizer-change")
    "act as expected when loading the saved DB dumps" in { env =>
      clue("Database dumps detected, using them to load participant and synchronizer state") {
        dumpDirectories().foreach { case (dumpDirectory, protocolVersion) =>
          withNewProtocolVersion(env, protocolVersion) { implicit newEnv =>
            import newEnv.*

            val Alice = "Alice"
            val Bank = "Bank"
            val Painter = "Painter"

            val participants =
              Seq(participant1, participant2, participant3, participant4, participant5)
            val sequencers = Seq(sequencer1, sequencer2)
            val mediators = Seq(mediator1, mediator2)

            logger.info("Testing dumps from dump directory " + dumpDirectory)

            loadState(
              sequencers,
              mediators,
              participants,
              S1M1_S1M1,
              dumpDirectory.localDownloadPath,
            )

            // initialize needed state - sadly unable to decouple this from implementation details of the workflow
            val incompleteUnassignedEvents =
              participant4.ledger_api.state.acs
                .incomplete_unassigned_of_party(Painter.toPartyId())

            val unassignedEvent = incompleteUnassignedEvents.loneElement
            // act on state
            clue("starting assignment and paint offer acceptance") {
              assignmentAndPaintOfferAcceptance(
                Alice.toPartyId(),
                Bank.toPartyId(),
                Painter.toPartyId(),
                unassignedEvent.entry.getUnassignedEvent,
              )
            }
          }
        }
      }
    }
  }
}

object DataContinuityTest {
  // We always use this folder for dumps, regardless whether it is locally or inside docker
  lazy val baseDbDumpPath: TempDirectory = TempDirectory(File("/tmp/canton/data-continuity-dumps/"))
  lazy val localBaseDbDumpPath: File =
    File.currentWorkingDirectory / "community/app/src/test/data-continuity-dumps"
  private val lock = new Mutex()
  val releaseVersion: ReleaseVersion = ReleaseVersion.current
  val protocolVersionPrefix = "pv="

  // /tmp/canton/data-continuity-dumps/release version
  lazy val baseDumpForRelease: TempDirectory =
    DataContinuityTest.baseDbDumpPath /
      DataContinuityTest.releaseVersion.fullVersion

  // /tmp/canton/data-continuity-dumps/release version/pv=pv
  def baseDumpForVersion(protocolVersion: ProtocolVersion): TempDirectory =
    baseDumpForRelease /
      (DataContinuityTest.protocolVersionPrefix + protocolVersion.v)

  lazy val baseDumpForConfig: TempDirectory = DataContinuityTest.baseDumpForRelease / "config"

  // IO around data continuity tests share directories. Allows to lock when required
  def synchronizedOperation(f: => Unit): Unit = lock.exclusive(f)

  final case class FolderName(name: String)

  def logDebugInformation(
      logger: TracedLogger
  )(implicit folderName: FolderName, traceContext: TraceContext): Unit = {
    val text = getDebugInformation()
      .map { case (k, v) => s"$k: $v" }
      .mkString("\n")

    logger.debug(s"State while running the data continuity tests for ${folderName.name}: $text")
  }

  private def getDebugInformation(): Map[String, String] = {
    val uname = "uname -a".!!
    val version = releaseVersion.fullVersion
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val now = Calendar.getInstance().getTime.toString
    val usernameOS = System.getProperty("user.name")
    val hostname = java.net.InetAddress.getLocalHost.getHostName

    Map(
      "UTC Date" -> now,
      "hostname" -> hostname,
      "OS username" -> usernameOS,
      "Canton version" -> version,
      "uname -a" -> uname,
    )
  }
}
