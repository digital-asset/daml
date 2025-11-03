// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.PartyActivationFlow.authorizeWithTargetDisconnect
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.data.ContractImportMode
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.InvalidState.AbortAcsExportForMissingOnboardingFlag
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.InvalidTimestamp
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  ParticipantPermission as PP,
}
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory, config}

import java.time.Instant

private[multihostedparties] object PartyActivationFlow {

  def authorizeOnly(
      party: PartyId,
      synchronizerId: PhysicalSynchronizerId,
      source: ParticipantReference,
      target: ParticipantReference,
  ): Long =
    authorizeWithTargetDisconnect(
      party,
      synchronizerId,
      source,
      target,
      disconnectTarget = false,
    )

  def authorizeWithTargetDisconnect(
      party: PartyId,
      synchronizerId: PhysicalSynchronizerId,
      source: ParticipantReference,
      target: ParticipantReference,
      disconnectTarget: Boolean = true,
  ): Long = {
    target.topology.party_to_participant_mappings
      .propose_delta(
        party = party,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = synchronizerId,
        requiresPartyToBeOnboarded = true,
      )

    if (disconnectTarget) {
      target.synchronizers.disconnect_all()
    }

    val sourceLedgerEnd = source.ledger_api.state.end()

    source.topology.party_to_participant_mappings.propose_delta(
      party = party,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = synchronizerId,
      requiresPartyToBeOnboarded = true,
    )

    sourceLedgerEnd
  }

  /** Unilateral clears the onboarding flag.
    *
    * As opposed to the `parties.complete_party_onboarding` endpoint, this does not wait for the
    * appropriate time to remove the onboarding flag, but does so immediately.
    */
  def removeOnboardingFlag(
      party: PartyId,
      synchronizerId: PhysicalSynchronizerId,
      target: ParticipantReference,
  ): Unit =
    target.topology.party_to_participant_mappings
      .propose_delta(
        party = party,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = synchronizerId,
        requiresPartyToBeOnboarded = false,
      )
      .discard

}

trait OfflinePartyReplicationIntegrationTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasTempDirectory
    with HasExecutionContext {

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Alice's replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to one year.
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  protected var source: LocalParticipantReference = _
  protected var target: LocalParticipantReference = _
  protected var alice: PartyId = _
  protected var bob: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

        alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("Bob", synchronizeParticipants = Seq(participant1))
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private val acsSnapshot =
    tempDirectory.toTempFile("offline_party_replication_test_acs_snapshot.gz")

  protected val acsSnapshotPath: String = acsSnapshot.toString

  protected def authorizeAliceWithTargetDisconnect(
      synchronizerId: PhysicalSynchronizerId,
      source: ParticipantReference,
      target: ParticipantReference,
  ): Long =
    authorizeWithTargetDisconnect(
      alice,
      synchronizerId,
      source,
      target,
    )

  protected def authorizeAliceWithoutOnboardingFlag(
      permission: PP,
      source: ParticipantReference,
      target: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit env: TestEnvironment): Unit =
    PartyToParticipantDeclarative.forParty(Set(source, target), synchronizerId)(
      source.id,
      alice,
      PositiveInt.one,
      Set(
        (source.id, PP.Submission),
        (target.id, permission),
      ),
    )

  protected def assertAcsAndContinuedOperation(
      participant: LocalParticipantReference
  ): Unit = {
    participant.ledger_api.state.acs.active_contracts_of_party(alice) should have size 5

    // Archive contract and create another one to assert regular operation after the completed party replication
    val iou = participant.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
    IouSyntax.archive(participant)(iou, alice)

    IouSyntax.createIou(participant)(alice, bob, 42.95).discard
  }

}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2 (P2)
  *   - 5 active IOU contracts between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Replicate Alice to target
  *   - Authorize Alice on target with observation permission
  *   - Export ACS for Alice on source
  *   - Disconnect source from the synchronizer
  *   - Import ACS for Alice on target
  *   - Reconnect target to the synchronizer
  *   - Change Alice's permission from observation to submission on target
  *
  *   - Assert expected number of active contracts for Alice on target.
  *   - Assert operation can continue on target, that is Alice can archive and create contracts on
  *     target.
  *
  * Test variations: Tests vary in the way the ledger offset or timestamp is determined for the ACS
  * export.
  */
sealed trait OfflinePartyReplicationIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      import env.*

      source = participant1
      target = participant3

      IouSyntax.createIou(source)(alice, bob, 1.95).discard
      IouSyntax.createIou(source)(alice, bob, 2.95).discard
      IouSyntax.createIou(source)(alice, bob, 3.95).discard
      IouSyntax.createIou(source)(alice, bob, 4.95).discard
      IouSyntax.createIou(source)(alice, bob, 5.95).discard
    }
}

final class OfflinePartyReplicationAtOffsetIntegrationTest
    extends OfflinePartyReplicationIntegrationTest {

  "Missing party activation on the target participant aborts ACS export" in { implicit env =>
    import env.*

    val ledgerEndP1 = source.ledger_api.state.end()

    // no authorization for alice

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = ledgerEndP1,
        exportFilePath = acsSnapshotPath,
        waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofMillis(5)),
      ),
      _.errorMessage should include regex "The stream has not been completed in.*– Possibly missing party activation?",
    )
  }

  "Party activation on the target participant with missing onboarding flag aborts ACS export" in {
    implicit env =>
      import env.*

      val ledgerEndP1 = source.ledger_api.state.end()

      authorizeAliceWithoutOnboardingFlag(Submission, source, target, daId)

      forAll(
        source.topology.party_to_participant_mappings
          .list(
            daId,
            filterParty = alice.toProtoPrimitive,
            filterParticipant = target.toProtoPrimitive,
          )
          .loneElement
          .item
          .participants
      ) { p =>
        p.onboarding shouldBe false
      }

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        source.parties.export_party_acs(
          party = alice,
          synchronizerId = daId,
          targetParticipantId = target.id,
          beginOffsetExclusive = ledgerEndP1,
          exportFilePath = acsSnapshotPath,
          waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofMillis(5)),
        ),
        _.errorMessage should include(AbortAcsExportForMissingOnboardingFlag(alice, target).cause),
      )

      // Undo activating Alice on the target participant without onboarding flag set; for the following test
      source.topology.party_to_participant_mappings.propose_delta(
        party = alice,
        removes = Seq(target.id),
        store = daId,
        mustFullyAuthorize = true,
      )

      // Wait for party deactivation to propagate to both source and target participants
      eventually() {
        val sourceMapping = source.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.toProtoPrimitive)
          .loneElement
          .item

        sourceMapping.participants should have size 1
        forExactly(1, sourceMapping.participants) { p =>
          p.participantId shouldBe source.id
          p.onboarding should be(false)
        }

        // Ensure the target has processed the "remove" transaction to prevent flakes
        val targetMapping = target.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.toProtoPrimitive)
          .loneElement
          .item

        targetMapping.participants should have size 1
        forExactly(1, targetMapping.participants) { p =>
          p.participantId shouldBe source.id
          p.onboarding should be(false)
        }
      }

  }

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using ledger offset" in {
    implicit env =>
      import env.*

      val beforeActivationOffset = authorizeAliceWithTargetDisconnect(daId, source, target)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )

      target.parties.import_party_acs(acsSnapshotPath)

      target.synchronizers.reconnect(daName)

      eventually() {
        target.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.filterString)
          .loneElement
          .item
          .participants should contain(
          HostingParticipant(target.id, ParticipantPermission.Submission, onboarding = true)
        )
      }

      // To prevent flakes when trying to archive contracts on the target in the next step
      PartyActivationFlow.removeOnboardingFlag(alice, daId, target)

      assertAcsAndContinuedOperation(target)
  }

  "Replicating a party with shared contracts filters contracts in the export ACS" in {
    implicit env =>
      import env.*

      val beforeActivationOffset = authorizeAliceWithTargetDisconnect(daId, source, participant2)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = participant2.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )

      source.ledger_api.state.acs.of_party(alice).size should be > 0
      repair.acs.read_from_file(acsSnapshotPath).size shouldBe 0
  }
}

/** Purpose: Verify that the major upgrade approach works end to end, observing these key aspects:
  *   - Silence the synchronizer (confirmationRequestsMaxRate=0)
  *   - Get the timestamp form that topology transaction (i.e. setting
  *     confirmationRequestsMaxRate=0)
  *   - Find the ledger offset for that timestamp
  *   - Export the ACS for Alice on P1 for that ledger offset
  *   - Import the ACS for Alice on P3
  *   - Assertions
  *
  * Why is the major upgrade test not enough? The major upgrade test performs an ACS export, but it
  * does not immediately import it again. This test here is more direct, exports and imports the ACS
  * immediately, and ensures that contract archival and creation continues to work using the
  * replicated party Alice.
  */
final class OfflinePartyReplicationWithSilentSynchronizerIntegrationTest
    extends OfflinePartyReplicationIntegrationTest
    with UseSilentSynchronizerInTest {

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using a silent synchronizer" in {
    implicit env =>
      import env.*

      adjustTimeouts(sequencer1)

      authorizeAliceWithoutOnboardingFlag(Observation, source, target, daId)

      val silentSynchronizerValidFrom =
        silenceSynchronizerAndAwaitEffectiveness(daId, sequencer1, source, simClock = None)

      val ledgerOffset =
        source.parties.find_highest_offset_by_timestamp(daId, silentSynchronizerValidFrom)

      ledgerOffset should be > NonNegativeLong.zero

      source.repair.export_acs(
        Set(alice),
        ledgerOffset = ledgerOffset,
        synchronizerId = Some(daId),
        exportFilePath = acsSnapshotPath,
      )

      target.synchronizers.disconnect_all()

      target.repair.import_acs(
        acsSnapshotPath,
        contractImportMode = ContractImportMode.Accept,
      )

      target.synchronizers.reconnect(daName)

      resumeSynchronizerAndAwaitEffectiveness(daId, sequencer1, source, simClock = None)

      authorizeAliceWithoutOnboardingFlag(Submission, source, target, daId)

      assertAcsAndContinuedOperation(target)
  }

  "Find ledger offset by timestamp can be forced, but not return a larger ledger offset with subsequent transactions" in {
    implicit env =>
      import env.*

      val requestedTimestamp = Instant.now
      val startLedgerEndOffset = source.ledger_api.state.end()

      // Creation of this contract is unnecessary, but it speeds up this test execution
      IouSyntax.createIou(source)(alice, bob, 99.95).discard

      val foundOffset = loggerFactory.assertLogsUnorderedOptional(
        eventually(retryOnTestFailuresOnly = false) {
          val offset =
            source.parties.find_highest_offset_by_timestamp(daId, requestedTimestamp).value
          offset should be > 0L
          offset
        },
        (
          LogEntryOptionality.OptionalMany,
          _.shouldBeCantonErrorCode(InvalidTimestamp),
        ),
      )

      val forcedFoundOffset =
        source.parties
          .find_highest_offset_by_timestamp(daId, requestedTimestamp, force = true)
          .value

      // The following cannot check for equality because find_highest_offset_by_timestamp skips over unpersisted
      // SequencerIndexMoved updates.
      forcedFoundOffset should be <= startLedgerEndOffset
      foundOffset should be < startLedgerEndOffset
      foundOffset shouldBe forcedFoundOffset
  }

}

final class OfflinePartyReplicationFilterAcsExportIntegrationTest
    extends OfflinePartyReplicationIntegrationTest {

  "ACS export filters active contracts only for parties which are already hosted on the target participant" in {
    implicit env =>
      import env.*

      val charlie = target.parties.enable("Charlie")

      IouSyntax.createIou(source)(alice, charlie, 99.99).discard

      val beforeActivationOffset = authorizeAliceWithTargetDisconnect(daId, source, target)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId.logical,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )

      // Only contracts that don't have stakeholders that are already hosted on the target participant
      val contracts = repair.acs.read_from_file(acsSnapshotPath)

      // Alice has 5 active contracts with Bob who is hosted on participant2,
      // and one active with Charlie already hosted on target
      contracts.size shouldBe 5
      forAll(contracts) { c =>
        val event = c.getCreatedEvent
        val stakeholders = (event.signatories ++ event.observers).toSet
        stakeholders.intersect(Set(charlie.toProtoPrimitive)).isEmpty
      }

      target.parties.import_party_acs(acsSnapshotPath)

      target.synchronizers.reconnect(daName)

      target.ledger_api.state.acs.active_contracts_of_party(alice) should have size 6
  }
}
