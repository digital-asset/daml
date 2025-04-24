// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.MediatorError.InvalidMessage
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, LoggerSuppressionHelpers}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.SubmissionAlreadyInFlight
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.UnsafeToPrune
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.{SyncServiceError, SynchronizerMigrationError}
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerConnectionConfig,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import monocle.macros.syntax.lens.*
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}

final class ParticipantMigrateSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OperabilityTestHelpers
    with AcsInspection
    with ParticipantMigrateSynchronizerIntegrationTestHelpers {

  // need persistence to test that we can migrate if the source synchronizer is offline
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  private def sourceProtocol: ProtocolVersion = testedProtocolVersion
  private def targetProtocol: ProtocolVersion =
    // The goal is to test: n -> n+1 and last -> last
    ProtocolVersion.fromProtoPrimitive(testedProtocolVersion.v + 1).getOrElse(testedProtocolVersion)

  private val deprecatedProtocolMessage =
    "This node is connecting to a sequencer using the deprecated protocol"

  private val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
  private val maxDedupDuration = java.time.Duration.ofSeconds(1)

  // depending on the protocol version, this might squeek
  private lazy val optionalDeprecatedProtocolVersionLog
      : (LogEntryOptionality, LogEntry => Assertion) =
    LogEntryOptionality.Optional -> (_.warningMessage should include(
      deprecatedProtocolMessage
    ))

  private def ignoreDeprecatedProtocolMessage[A](within: => A): A =
    loggerFactory.assertLogsUnorderedOptional(
      within,
      optionalDeprecatedProtocolVersionLog,
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(targetProtocol.isAlpha)*)
      .addConfigTransforms(ConfigTransforms.setBetaSupport(targetProtocol.isBeta)*)
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)
      .addConfigTransforms(
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.minimumProtocolVersion)
            .replace(Some(ParticipantProtocolVersion(sourceProtocol)))
            .focus(_.parameters.alphaVersionSupport)
            .replace(targetProtocol.isAlpha)
        ),
      )

  private val remedy = operabilityTest("Participant.RepairService")("ProtocolVersion") _

  "participants connect and create a few contracts" in { implicit env =>
    import env.*
    sequencer1.topology.synchronizer_parameters
      .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
    sequencer2.topology.synchronizer_parameters
      .propose_update(acmeId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

    Seq(participant1, participant2).foreach { participant =>
      participant.start()

      ignoreDeprecatedProtocolMessage(
        participant.synchronizers.connect_local(sequencer1, alias = daName)
      )
      participant.dars.upload(CantonExamplesPath)
      participant.dars.upload(CantonTestsPath)
    }

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
      synchronizer = daName,
    )
    val bob = participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
      synchronizer = daName,
    )

    // temporarily connect to synchronizer 2 and allocate parties there
    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
      synchronizer = acmeName,
    )
    participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
      synchronizer = acmeName,
    )
    participant1.synchronizers.disconnect(acmeName)
    participant2.synchronizers.disconnect(acmeName)

    IouSyntax.createIou(participant1)(alice, bob)

    participant1.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(alice).discard
    participant2.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(bob).discard
  }

  "Migration command doesn't run while synchronizer is connected" in { implicit env =>
    import env.*
    val config = getSynchronizerConfig(acmeName, sequencer2)
    assertThrowsAndLogsCommandFailures(
      participant1.repair.migrate_synchronizer(source = daName, target = config),
      _.shouldBeCantonErrorCode(SyncServiceError.SyncServiceSynchronizersMustBeOffline),
    )
  }

  private val commandId = "in-flight-command-id"
  private val submissionId = "submission-broken-by-migration"

  "create an in-flight command on the source synchronizer" in { implicit env =>
    import env.*

    val alice = grabParty(participant1, "Alice")
    val bob = grabParty(participant2, "Bob")
    val coid = clue("awaiting old iou") {
      participant2.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(bob)
    }

    // Disconnect the confirming participant so that the transaction will not go through
    participant1.synchronizers.disconnect(daName)

    participant2.ledger_api.javaapi.commands.submit_async(
      Seq(bob),
      coid.id.exerciseTransfer(alice.toProtoPrimitive).commands.asScala.toSeq,
      commandId = commandId,
      submissionId = submissionId,
    )
    // The command should be now in-flight because the submit_async returns only after in-flight submission checking
    // Let's nevertheless wait a bit so that it's more likely that the transaction actually gets sent to the sequencer and back
    Threading.sleep(500)
    participant2.synchronizers.disconnect(daName)
  }

  // TODO(#17334): unignore
  // These tests are currently only implemented for the BFT ordering sequencer.
  "Migration command checks properly for invalid input" ignore { implicit env =>
    import env.*

    val config = getSynchronizerConfig(acmeName, sequencer2)

    // check that our implicit functions work. we should be able to create a synchronizer connection config with a simple string
    // if this doesn't work anymore, we break the UX on the console / backwards compatibility
    // note: we automatically include ConsoleEnvironment.Implicits._ in our console such that users
    // don't have to use too many types if they don't want to
    SynchronizerConnectionConfig("dummy", "https://localhost:12345").discard

    // target config is wrong
    assertThrowsAndLogsCommandFailures(
      participant1.repair.migrate_synchronizer(
        source = daName,
        target = config.copy(sequencerConnections =
          SequencerConnections.single(GrpcSequencerConnection.tryCreate("http://localhost:80"))
        ),
      ),
      _.shouldBeCantonErrorCode(
        SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencer
      ),
    )

    // wrong target synchronizer id
    assertThrowsAndLogsCommandFailures(
      participant1.repair.migrate_synchronizer(
        source = daName,
        target = config.copy(synchronizerId = Some(daId)),
      ),
      _.shouldBeCantonErrorCode(SynchronizerMigrationError.InvalidArgument),
    )

    // non-existent source id
    assertThrowsAndLogsCommandFailures(
      participant1.repair.migrate_synchronizer("wurst", config),
      _.shouldBeCantonErrorCode(SyncServiceError.SyncServiceUnknownSynchronizer),
    )
  }

  "Migration command aborts when in-flight transactions exist on the source synchronizer" in {
    implicit env =>
      import env.*
      val config = getSynchronizerConfig(acmeName, sequencer2)

      assertThrowsAndLogsCommandFailures(
        participant2.repair.migrate_synchronizer(daName, config),
        _.shouldBeCantonErrorCode(
          SyncServiceError.SyncServiceSynchronizerMustNotHaveInFlightTransactions
        ),
      )

      participant2.health.count_in_flight(daName).exists shouldBe true
  }

  s"Participants using a synchronizer with protocol version ${sourceProtocol.toString}" when_ {
    setting =>
      s"a new synchronizer with protocol ${targetProtocol.toString} is deployed" must_ { cause =>
        remedy(setting)(cause)("Be able to migrate contracts to the new synchronizer") in {
          implicit env =>
            import env.*
            val alice = grabParty(participant1, "Alice")

            // now stop the origin synchronizer so we can be sure that we can migrate
            // even if the origin synchronizer is offline
            sequencer1.stop()
            mediator1.stop()

            val config = getSynchronizerConfig(acmeName, sequencer2)

            val reassignmentCounterBefore = participant1.ledger_api.state.acs
              .active_contracts_of_party(alice)
              .map(_.reassignmentCounter)
            reassignmentCounterBefore should not be empty

            Seq(participant1, participant2).foreach { p =>
              // need to force the synchronizer migration because of in-flight transactions
              p.repair.migrate_synchronizer(source = daName, target = config, force = true)
            }

            val logAssertions: Seq[(LogEntryOptionality, LogEntry => Assertion)] =
              // suppress potential ACS commitment warnings during migration
              LoggerSuppressionHelpers.suppressOptionalAcsCmtMismatchAndNoSharedContracts() ++
                Seq(LogEntryOptionality.Optional -> (_.shouldBeCantonErrorCode(InvalidMessage)))
            loggerFactory.assertLogsUnorderedOptional(
              {
                Seq(participant1, participant2).foreach(_.synchronizers.reconnect(acmeName))

                val expectedAssignationAfter =
                  reassignmentCounterBefore.map(counter => (acmeId.toProtoPrimitive, counter + 1))
                val assignationAfter = participant1.ledger_api.state.acs
                  .active_contracts_of_party(alice)
                  .map(c => (c.synchronizerId, c.reassignmentCounter))
                assignationAfter shouldBe expectedAssignationAfter

                // ping several times such that we have higher confidence that the acs commitment processor caught up
                forAll(0 to 3)(_ => participant1.health.ping(participant2))

                // turn synchronizer on again
                sequencer1.start()
                mediator1.start()
              },
              (logAssertions)*
            )

          // TODO(i9557) check that commitments match again
        }

        "test we can progress on existing contracts" in { implicit env =>
          import env.*
          val alice = grabParty(participant1, "Alice")
          val bob = grabParty(participant2, "Bob")

          val coid = clue("awaiting old io") {
            participant2.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(bob)
          }

          clue("transferring") {
            participant2.ledger_api.javaapi.commands
              .submit_flat(
                Seq(bob),
                coid.id.exerciseTransfer(alice.toProtoPrimitive).commands.asScala.toSeq,
              )
          }

          val after = clue("observing reassignment") {
            findIOU(participant1, alice, _.data.owner == alice.toProtoPrimitive)
          }

          after.data.owner shouldBe alice.toProtoPrimitive

        }

        // TODO(#14242) We can't reuse the command ID :-(
        "test that we cannot use the command ID" in { implicit env =>
          import env.*
          val bob = grabParty(participant2, "Bob")
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant2.ledger_api.javaapi.commands.submit(
              Seq(bob),
              IouSyntax.testIou(bob, bob).create.commands.asScala.toSeq,
              commandId = commandId,
              submissionId = "resubmission",
            ),
            _.shouldBeCantonErrorCode(SubmissionAlreadyInFlight),
          )
        }
      }
  }

  "Sanely deal with repetitive migration requests" in { implicit env =>
    import env.*
    participant1.synchronizers.disconnect(acmeName)
    val config = getSynchronizerConfig(acmeName, sequencer2)
    assertThrowsAndLogsCommandFailures(
      participant1.repair.migrate_synchronizer(source = daName, target = config),
      _.shouldBeCantonErrorCode(SynchronizerMigrationError.InvalidArgument),
    )

    // now manipulate the stores, pretending that we aborted the migration request
    val store =
      participant1.underlying
        .valueOrFail("failed to grab node")
        .sync
        .synchronizerConnectionConfigStore
    store.setStatus(daName, SynchronizerConnectionConfigStore.Vacating).value.futureValueUS.value
    store
      .setStatus(acmeName, SynchronizerConnectionConfigStore.MigratingTo)
      .value
      .futureValueUS
      .value

    // should work subsequently
    participant1.repair.migrate_synchronizer(source = daName, target = config)
  }

  "Can not reconnect to inactive synchronizer" in { implicit env =>
    import env.*

    assertThrowsAndLogsCommandFailures(
      participant1.synchronizers.reconnect(daName),
      _.shouldBeCantonErrorCode(SyncServiceError.SyncServiceSynchronizerIsNotActive),
    )

  }

  "The participant can be pruned after the migration offset" in { implicit env =>
    import env.*
    val ledgerEndP2 =
      participant2.ledger_api.state.end()
    clue(s"Attempting to prune participant2 at $ledgerEndP2") {
      eventually() {
        // Create some dummy transactions before attempting to prune
        participant2.health.ping(participant2)
        loggerFactory.assertLogsUnorderedOptional(
          Try(participant2.pruning.prune(ledgerEndP2)) shouldBe a[Success[?]],
          LogEntryOptionality.Optional -> (_.shouldBeCantonErrorCode(UnsafeToPrune)),
        )
      }
    }
  }

  "Can not migrate back to the inactive synchronizer" in { implicit env =>
    import env.*

    val config = getSynchronizerConfig(daName, sequencer1)

    loggerFactory.assertLogsUnorderedOptional(
      a[CommandFailure] shouldBe thrownBy(
        participant1.repair.migrate_synchronizer(source = acmeName, target = config)
      ),
      LogEntryOptionality.Required -> (_.errorMessage should (include("Inactive") and include(
        SynchronizerMigrationError.InvalidArgument.id
      ))),
      optionalDeprecatedProtocolVersionLog,
    )
  }

  "Can purge inactive synchronizer" in { implicit env =>
    import env.*

    Seq(participant1, participant2).foreach { p =>
      clue(s"purging participant ${p.name} deactivated synchronizer") {
        p.repair.purge_deactivated_synchronizer(daName)

        // Check that the expected synchronizer stores have been purged
        val inspection = p.testing.state_inspection

        val sequencerClientEvents =
          inspection.findMessages(daName, from = None, to = None, limit = Some(10))
        sequencerClientEvents shouldBe empty

        val acs = valueOrFail(inspection.findAcs(daName))("ACS").futureValueUS
        acs shouldBe empty

        val activeContracts = inspection
          .findContracts(
            daName,
            exactId = None,
            filterPackage = None,
            filterTemplate = None,
            limit = 1000,
          )
          .filter { case (active, _) => active }

        activeContracts shouldBe empty

        inspection.requestJournalSize(daName) shouldBe Some(UnlessShutdown.Outcome(0))

        // ACS commitments should be purged
        def check[A](
            f: (
                SynchronizerAlias,
                CantonTimestamp,
                CantonTimestamp,
                Option[ParticipantId],
            ) => Iterable[A]
        ) = f(daName, CantonTimestamp.Epoch, CantonTimestamp.MaxValue, None) shouldBe empty
        check(inspection.findReceivedCommitments)
        check(inspection.findComputedCommitments)
        check(inspection.outstandingCommitments)
        inspection.bufferedCommitments(daName, CantonTimestamp.MaxValue) shouldBe empty
      }
    }

    mediator1.stop()
  }
}

final class ParticipantMigrateSynchronizerCrashRecoveryIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OperabilityTestHelpers
    with AcsInspection
    with ParticipantMigrateSynchronizerIntegrationTestHelpers {

  // need persistence to test that we can migrate if the source synchronizer is offline
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  // A party replication is involved and we want to minimize the risk of warnings related to acs commitment mismatches
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1.withSetup { implicit env =>
      import env.*

      Seq(participant1, participant2, participant3).foreach { participant =>
        participant.synchronizers.connect_local(sequencer1, daName)
        participant.synchronizers.connect_local(sequencer2, acmeName)
        participant.dars.upload(CantonExamplesPath)
        participant.dars.upload(CantonTestsPath).discard
      }

      sequencer1.topology.synchronizer_parameters
        .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
      sequencer2.topology.synchronizer_parameters
        .propose_update(acmeId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
    }

  private def authorizeOnP3(party: PartyId)(implicit env: TestConsoleEnvironment) = {
    import env.*

    for {
      p <- Seq(participant1, participant3)
      synchronizerId <- Seq(daId, acmeId)
    } yield p.topology.party_to_participant_mappings
      .propose_delta(
        party,
        adds = List((participant3.id, ParticipantPermission.Submission)),
        store = synchronizerId,
      )

    eventually() {
      Seq(participant1, participant3).foreach { p =>
        p.topology.party_to_participant_mappings.is_known(
          daId,
          party,
          Seq(participant3),
        ) shouldBe true

        p.topology.party_to_participant_mappings.is_known(
          acmeId,
          party,
          Seq(participant3),
        ) shouldBe true
      }
    }
  }

  "participants connect and create a few contracts" in { implicit env =>
    import env.*

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
      synchronizer = daName,
    )
    participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
      synchronizer = acmeName,
    )
    val bob = participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
      synchronizer = daName,
    )
    participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
      synchronizer = acmeName,
    )

    val iousCommands = Seq(100.0, 101.0).flatMap { amount =>
      IouSyntax.testIou(alice, bob, amount).create.commands.asScala.toSeq
    }

    participant1.ledger_api.javaapi.commands.submit(Seq(alice), iousCommands)
    participant1.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(alice).discard
  }

  /*
  The goal is to simulate an interrupted migration.
  For this, we:
    - import one contract
    - run the migration (da -> acme)
    - import the second contract
    - finish the migration

  Migration is done on P3 and the contracts are grabbed from Alice ACS on P1.
   */
  "Allow to finish interrupted migration" in { implicit env =>
    import env.*

    // Connect to acme so that Alice is known on the synchronizer (required for the migration)
    participant3.synchronizers.connect_local(sequencer2, acmeName)

    val acmeConfig = getSynchronizerConfig(acmeName, sequencer2)
    val alice = grabParty(participant1, "Alice")

    val ledgerEnd = participant1.ledger_api.state.end()

    // Authorize Alice on P3
    authorizeOnP3(alice)

    // Get Alice ACS
    val aliceAddedOnP3Offset = participant1.parties.find_party_max_activation_offset(
      partyId = alice,
      participantId = participant3.id,
      synchronizerId = daId,
      beginOffsetExclusive = ledgerEnd,
      completeAfter = PositiveInt.one,
    )

    val source =
      participant1.underlying.value.sync.internalStateService.value.activeContracts(
        Set(alice.toLf),
        Offset.fromLong(aliceAddedOnP3Offset.unwrap).toOption,
      )
    val aliceACS =
      source
        .runWith(Sink.seq)
        .futureValue
        .map(resp => RepairContract.toRepairContract(resp.getActiveContract).value)
        .toList

    participant3.synchronizers.disconnect_all()

    def assertContractAssigned(
        contractId: LfContractId,
        synchronizerAlias: SynchronizerAlias,
    ): Unit =
      assertInAcsSync(
        participantRefs = Seq(participant3),
        synchronizerAlias = synchronizerAlias,
        contractId = contractId,
      )

    def addAndMigrate(c: RepairContract): Unit = {
      participant3.repair.add(
        // We want to perform the migration, so we specify da.
        synchronizerId = daId,
        protocolVersion = testedProtocolVersion,
        contracts = Seq(c),
      )
      participant3.synchronizers.reconnect_all()
      assertContractAssigned(c.contractId, daName)
      participant3.synchronizers.disconnect_all()
      participant3.repair.migrate_synchronizer(daName, acmeConfig)
      assertContractAssigned(c.contractId, acmeName)
    }

    addAndMigrate(aliceACS(0))

    // now manipulate the stores, pretending that we aborted the migration request
    val store =
      participant3.underlying
        .valueOrFail("failed to grab node")
        .sync
        .synchronizerConnectionConfigStore
    store.setStatus(daName, SynchronizerConnectionConfigStore.Vacating).value.futureValueUS.value
    store
      .setStatus(acmeName, SynchronizerConnectionConfigStore.MigratingTo)
      .value
      .futureValueUS
      .value

    addAndMigrate(aliceACS(1))
  }

}

trait ParticipantMigrateSynchronizerIntegrationTestHelpers { self: BaseTest =>
  protected def grabParty(participant: LocalParticipantReference, name: String): PartyId =
    participant.parties
      .hosted(filterParty = name)
      .headOption
      .map(_.party)
      .valueOrFail(s"Failed to find party with name: $name")

  protected def getSynchronizerConfig(
      synchronizerAlias: SynchronizerAlias,
      instance: SequencerReference,
  ): SynchronizerConnectionConfig =
    SynchronizerConnectionConfig(
      synchronizerAlias,
      SequencerConnections.single(instance.sequencerConnection),
      manualConnect = false,
      None,
      priority = 10,
      None,
      maxRetryDelay = None,
      SynchronizerTimeTrackerConfig(),
    )
}
