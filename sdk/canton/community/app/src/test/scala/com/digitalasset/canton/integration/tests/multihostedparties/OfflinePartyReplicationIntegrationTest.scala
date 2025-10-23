// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.ConfigTransforms.zeroReassignmentTimeProofFreshnessProportion
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.data.ContractIdImportMode
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.InvalidTimestamp
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory}

import java.time.Instant

/** Setup:
  *   - Alice is hosted on participant1 (P1)
  *   - Bob is hosted on participant2 (P2)
  *   - 5 active IOU contracts between Alice and Bob
  *   - Participant3 (P3) is empty, and the target participant for replicating Alice
  *
  * Test: Replicate Alice to P3
  *   - Authorize Alice on P3 with observation permission
  *   - Export ACS for Alice on P1
  *   - Disconnect P3 from the synchronizer
  *   - Import ACS for Alice on P3
  *   - Reconnect P3 to the synchronizer
  *   - Change Alice's permission from observation to submission on P3
  *
  *   - Assert expected number of active contracts for Alice on P3.
  *   - Assert operation can continue on P3, that is Alice can archive and create contracts on P3.
  *
  * Test variations: Tests vary in the way the ledger offset or timestamp is determined for the ACS
  * export.
  */
private sealed trait OfflinePartyReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasTempDirectory
    with HasExecutionContext {

  // Alice's replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to one year.
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  protected var alice: PartyId = _
  protected var bob: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransform(zeroReassignmentTimeProofFreshnessProportion)
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

        alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("Bob", synchronizeParticipants = Seq(participant1))

        IouSyntax.createIou(participant1)(alice, bob, 1.95).discard
        IouSyntax.createIou(participant1)(alice, bob, 2.95).discard
        IouSyntax.createIou(participant1)(alice, bob, 3.95).discard
        IouSyntax.createIou(participant1)(alice, bob, 4.95).discard
        IouSyntax.createIou(participant1)(alice, bob, 5.95).discard
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private val acsSnapshot =
    tempDirectory.toTempFile("offline_party_replication_test_acs_snapshot.gz")

  protected val acsSnapshotPath: String = acsSnapshot.toString

  protected def authorizeAlice(
      permission: PP,
      p1: ParticipantReference,
      p3: ParticipantReference,
      synchronizer: SynchronizerId,
  ): Unit =
    PartyToParticipantDeclarative.forParty(Set(p1, p3), synchronizer)(
      p1.id,
      alice,
      PositiveInt.one,
      Set(
        (p1.id, PP.Submission),
        (p3.id, permission),
      ),
    )

  protected def assertAcsAndContinuedOperation(participant: LocalParticipantReference): Unit = {
    participant.ledger_api.state.acs.active_contracts_of_party(alice) should have size 5

    // Archive contract and create another one to assert regular operation after the completed party replication
    val iou = participant.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
    IouSyntax.archive(participant)(iou, alice)

    IouSyntax.createIou(participant)(alice, bob, 42.95).discard
  }

}

final private class OfflinePartyReplicationAtOffsetIntegrationTest
    extends OfflinePartyReplicationIntegrationTest {

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using ledger offset" in {
    implicit env =>
      import env.*
      val ledgerEndP1 = participant1.ledger_api.state.end()

      authorizeAlice(Observation, participant1, participant3, daId)

      // Find the ledger offset for the party-to-participant mapping topology transaction authorizing Alice on P3
      val aliceAddedOnP3Offset = participant1.parties.find_party_max_activation_offset(
        partyId = alice,
        participantId = participant3.id,
        synchronizerId = daId,
        beginOffsetExclusive = ledgerEndP1,
        completeAfter = PositiveInt.one,
      )

      participant1.parties.export_acs(
        Set(alice),
        ledgerOffset = aliceAddedOnP3Offset,
        exportFilePath = acsSnapshotPath,
      )

      participant3.synchronizers.disconnect_all()

      participant3.repair.import_acs(
        acsSnapshotPath,
        contractIdImportMode = ContractIdImportMode.Accept,
      )

      participant3.synchronizers.reconnect(daName)

      authorizeAlice(Submission, participant1, participant3, daId)

      assertAcsAndContinuedOperation(participant3)
  }
}

final private class OfflinePartyReplicationAtTimestampIntegrationTest
    extends OfflinePartyReplicationIntegrationTest {

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using topology transaction effective time" in {
    implicit env =>
      import env.*

      authorizeAlice(Observation, participant1, participant3, daId)

      val onboardingTx = participant1.topology.party_to_participant_mappings
        .list(
          synchronizerId = daId,
          filterParty = alice.filterString,
          filterParticipant = participant3.filterString,
        )
        .loneElement
        .context

      participant1.parties.export_acs_at_timestamp(
        Set(alice),
        daId,
        onboardingTx.validFrom,
        exportFilePath = acsSnapshotPath,
      )

      participant3.synchronizers.disconnect_all()

      participant3.repair.import_acs(
        acsSnapshotPath,
        contractIdImportMode = ContractIdImportMode.Accept,
      )

      participant3.synchronizers.reconnect(daName)

      authorizeAlice(Submission, participant1, participant3, daId)

      assertAcsAndContinuedOperation(participant3)
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
final private class OfflinePartyReplicationWithSilentSynchronizerIntegrationTest
    extends OfflinePartyReplicationIntegrationTest
    with UseSilentSynchronizerInTest {

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using a silent synchronizer" in {
    implicit env =>
      import env.*

      adjustTimeouts(sequencer1)

      authorizeAlice(Observation, participant1, participant3, daId)

      val silentSynchronizerValidFrom =
        silenceSynchronizerAndAwaitEffectiveness(daId, sequencer1, participant1, simClock = None)

      val ledgerOffset =
        participant1.parties.find_highest_offset_by_timestamp(daId, silentSynchronizerValidFrom)

      ledgerOffset should be > NonNegativeLong.zero

      participant1.parties.export_acs(
        Set(alice),
        ledgerOffset = ledgerOffset,
        synchronizerId = Some(daId),
        exportFilePath = acsSnapshotPath,
      )

      participant3.synchronizers.disconnect_all()

      participant3.repair.import_acs(
        acsSnapshotPath,
        contractIdImportMode = ContractIdImportMode.Accept,
      )

      participant3.synchronizers.reconnect(daName)

      resumeSynchronizerAndAwaitEffectiveness(daId, sequencer1, participant1, simClock = None)

      authorizeAlice(Submission, participant1, participant3, daId)

      assertAcsAndContinuedOperation(participant3)
  }

  "Find ledger offset by timestamp can be forced, but not return a larger ledger offset with subsequent transactions" in {
    implicit env =>
      import env.*

      val requestedTimestamp = Instant.now
      val startLedgerEndOffset = participant1.ledger_api.state.end()

      // Creation of this contract is unnecessary, but it speeds up this test execution
      IouSyntax.createIou(participant1)(alice, bob, 99.95).discard

      val foundOffset = loggerFactory.assertLogsUnorderedOptional(
        eventually(retryOnTestFailuresOnly = false) {
          val offset =
            participant1.parties.find_highest_offset_by_timestamp(daId, requestedTimestamp).value
          offset should be > 0L
          offset
        },
        (
          LogEntryOptionality.OptionalMany,
          _.shouldBeCantonErrorCode(InvalidTimestamp),
        ),
      )

      val forcedFoundOffset =
        participant1.parties
          .find_highest_offset_by_timestamp(daId, requestedTimestamp, force = true)
          .value

      // The following cannot check for equality because find_highest_offset_by_timestamp skips over unpersisted
      // SequencerIndexMoved updates.
      forcedFoundOffset should be <= startLedgerEndOffset
      foundOffset should be < startLedgerEndOffset
      foundOffset shouldBe forcedFoundOffset
  }
}
