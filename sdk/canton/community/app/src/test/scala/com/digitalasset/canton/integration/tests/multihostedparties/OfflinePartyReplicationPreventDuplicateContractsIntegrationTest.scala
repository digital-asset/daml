// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceAlarm,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory, config}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Setup:
  *   - Alice is hosted on participant1 (Source)
  *   - Bob is hosted on participant2 (Target)
  *   - Charlie is hosted on participant3
  *   - 1 active IOU contract between Alice (signatory) and Bob (observer)
  *   - 1 active IOU contract between Alice (signatory) and Charlie (observer)
  *
  * Test: Replicate Alice to target hosting already Bob (shared contract, contract duplication
  * issue); see also https://docs.google.com/document/d/1qXj0UoaOE1Pjdx0M_cD0_zJA0d1J2W2aaLxzWhYUuIs
  *   - Target participant authorizes Alice->target
  *   - Target participant disconnects from the synchronizer
  *   - A creation transaction to create a contract with Alice as signatory and Bob as observer is
  *     sequenced
  *   - Source participant approves/confirms transaction
  *   - Source participant authorizes Alice->target
  *   - ACS snapshot for Alice is taken on source participant
  *   - ACS is imported on the target participant, which then reconnects to the synchronizers
  *   - Assert success and failure outcomes depending on how the ACS is exported, and imported,
  *     respectively
  */
sealed trait OfflinePartyReplicationPreventDuplicateContractsIntegrationTest
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
  protected var ledgerEndP1: Long = _
  protected var activationOffset: NonNegativeLong = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*

      participants.local.synchronizers.connect_local(sequencer1, daName)
      participants.local.dars.upload(CantonExamplesPath)
      sequencer1.topology.synchronizer_parameters
        .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

      source = participant1
      target = participant2

      alice = source.parties.enable("Alice")
      bob = target.parties.enable("Bob")
      val charlie = participant3.parties.enable("Charlie")

      IouSyntax.createIou(participant1)(alice, charlie, 3.33).discard

      target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq(target.id -> ParticipantPermission.Submission),
          store = daId,
        )

      target.synchronizers.disconnect_all()

      val createIouCmd = testIou(alice, bob, 2.20).create().commands().asScala.toSeq

      source.ledger_api.javaapi.commands.submit(
        Seq(alice),
        createIouCmd,
        daId,
        optTimeout = None,
      )

      ledgerEndP1 = source.ledger_api.state.end()

      source.topology.party_to_participant_mappings.propose_delta(
        party = alice,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = daId,
      )

      activationOffset = source.parties.find_party_max_activation_offset(
        partyId = alice,
        participantId = target.id,
        synchronizerId = daId,
        beginOffsetExclusive = ledgerEndP1,
        completeAfter = PositiveInt.one,
      )
    }

  private val acsSnapshot =
    tempDirectory.toTempFile("offline_party_replication_prevent_dup_contracts_snapshot.gz")

  protected val acsSnapshotPath: String = acsSnapshot.toString

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

}

/** Test: Assert failure after importing the shared IOU between Alice and Bob to target participant
  * because the target already knows about that (shared) contract.
  *
  * This case triggers the duplicate contract issue when replicating a party.
  */
final class OffPRPreventDupContractsFailureIntegrationTest
    extends OfflinePartyReplicationPreventDuplicateContractsIntegrationTest {

  "Party replication without ACS export filtering fails with duplicated contract error" in {
    implicit env =>
      import env.*

      source.repair.export_acs(Set(alice), activationOffset, acsSnapshotPath)

      repair.acs.read_from_file(acsSnapshotPath) should have size 2

      target.repair.import_acs(acsSnapshotPath)

      loggerFactory.assertLogsUnorderedOptional(
        target.synchronizers.reconnect(daName),
        (
          LogEntryOptionality.Required,
          (entry: LogEntry) =>
            entry.shouldBeCantonError(
              SyncServiceAlarm,
              _ should include("with failed activeness check is approved"),
            ),
        ),
        (
          LogEntryOptionality.Optional,
          (entry: LogEntry) =>
            entry.shouldBeCantonError(
              SyncServiceSynchronizerDisconnect,
              _ should include regex "(?s)fatally disconnected because of handler returned error.*with failed activeness check is approved",
            ),
        ),
        (
          LogEntryOptionality.OptionalMany,
          (entry: LogEntry) =>
            entry.errorMessage should include("Transaction: Failed to process result"),
        ),
        (
          LogEntryOptionality.OptionalMany,
          (entry: LogEntry) =>
            entry.errorMessage should include("Asynchronous event processing failed"),
        ),
        (
          LogEntryOptionality.OptionalMany,
          (entry: LogEntry) =>
            entry.errorMessage should include("with failed activeness check is approved"),
        ),
      )
  }
}

/** Test: Assert success after importing an ACS snapshot containing the shared IOU contract between
  * Alice and Bob, and filtering it out upon import.
  */
final class OffPRPreventDupContractsSuccessOnAcsImportFilteringIntegrationTest
    extends OfflinePartyReplicationPreventDuplicateContractsIntegrationTest {

  "Party replication without ACS export filtering succeeds when filtering shared contract upon ACS import" in {
    implicit env =>
      import env.*

      source.repair.export_acs(Set(alice), activationOffset, acsSnapshotPath)

      repair.acs.read_from_file(acsSnapshotPath) should have size 2

      target.repair.import_acs(acsSnapshotPath, excludedStakeholders = Set(bob))

      target.synchronizers.reconnect(daName)

      source.health.ping(target)
      target.ledger_api.state.acs.of_party(alice) should have size 2
  }

}

/** Test: Assert success after importing an ACS snapshot that does not contain the shared contract.
  */
final class OffPRPreventDupContractsUponAcsImportIntegrationTest
    extends OfflinePartyReplicationPreventDuplicateContractsIntegrationTest {

  "Party replication with ACS export filtering succeeds upon ACS import" in { implicit env =>
    import env.*

    source.repair.export_acs(
      Set(alice),
      activationOffset,
      acsSnapshotPath,
      excludedStakeholders = Set(bob),
    )

    repair.acs.read_from_file(acsSnapshotPath) should have size 1

    target.repair.import_acs(acsSnapshotPath)

    target.synchronizers.reconnect(daName)

    source.health.ping(target)
    target.ledger_api.state.acs.of_party(alice).size shouldBe 2
  }

}

/** Test: Assert success after importing an ACS snapshot that has been created through the party
  * replication focused ACS export (which filters out shared contracts internally).
  */
final class OffPRPreventDupContractsIntegrationTest
    extends OfflinePartyReplicationPreventDuplicateContractsIntegrationTest {

  "Party replication with party replication focussed ACS export succeeds" in { implicit env =>
    import env.*

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target,
      beginOffsetExclusive = ledgerEndP1,
      exportFilePath = acsSnapshotPath,
      waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(10)),
    )

    repair.acs.read_from_file(acsSnapshotPath) should have size 1

    target.repair.import_acs(acsSnapshotPath)

    target.synchronizers.reconnect(daName)

    source.health.ping(target)
    target.ledger_api.state.acs.of_party(alice).size shouldBe 2
  }

}
