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
  TestEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.data.ContractIdImportMode
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.InvalidTimestamp
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory, config}

import java.time.Instant

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
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
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
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private val acsSnapshot =
    tempDirectory.toTempFile("offline_party_replication_test_acs_snapshot.gz")

  protected val acsSnapshotPath: String = acsSnapshot.toString

  protected def authorizeAlice(
      permission: PP,
      p1: ParticipantReference,
      p3: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit env: TestEnvironment): Unit =
    PartyToParticipantDeclarative.forParty(Set(p1, p3), synchronizerId)(
      p1.id,
      alice,
      PositiveInt.one,
      Set(
        (p1.id, PP.Submission),
        (p3.id, permission),
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
      _.commandFailureMessage should include regex "The stream has not been completed in.*– Possibly missing party activation?",
    )
  }

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using ledger offset" in {
    implicit env =>
      import env.*

      val ledgerEndP1 = source.ledger_api.state.end()

      authorizeAlice(Observation, source, target, daId)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = ledgerEndP1,
        exportFilePath = acsSnapshotPath,
      )

      target.synchronizers.disconnect_all()

      target.repair.import_acs(
        acsSnapshotPath,
        contractIdImportMode = ContractIdImportMode.Accept,
      )

      target.synchronizers.reconnect(daName)

      authorizeAlice(Submission, source, target, daId)

      assertAcsAndContinuedOperation(target)
  }

  "Replicating a party with shared contracts filters contracts in the export ACS" in {
    implicit env =>
      import env.*

      val ledgerEndP1 = source.ledger_api.state.end()

      authorizeAlice(Observation, source, participant2, daId)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = participant2.id,
        beginOffsetExclusive = ledgerEndP1,
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

      authorizeAlice(Observation, source, target, daId)

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
        contractIdImportMode = ContractIdImportMode.Accept,
      )

      target.synchronizers.reconnect(daName)

      resumeSynchronizerAndAwaitEffectiveness(daId, sequencer1, source, simClock = None)

      authorizeAlice(Submission, source, target, daId)

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

      val ledgerEndP1 = source.ledger_api.state.end()

      authorizeAlice(Observation, source, target, daId)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId.logical,
        targetParticipantId = target.id,
        beginOffsetExclusive = ledgerEndP1,
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

      target.synchronizers.disconnect_all()

      target.repair.import_acs(acsSnapshotPath)

      target.synchronizers.reconnect(daName)

      target.ledger_api.state.acs.active_contracts_of_party(alice) should have size 6
  }
}
