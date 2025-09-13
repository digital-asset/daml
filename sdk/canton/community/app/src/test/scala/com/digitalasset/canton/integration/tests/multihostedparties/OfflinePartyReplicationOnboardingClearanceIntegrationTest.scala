// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.transaction.ParticipantPermission

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Setup:
  *   - Alice is hosted on participant1 (Source)
  *   - Bob is hosted on participant2 (Target)
  *   - 2 active IOU contract between Alice (signatory) and Bob (observer)
  *
  * Test: Replicate Alice to target using the onboarding flag
  *   - Target participant authorizes Alice->target setting the onboarding flag
  *   - Target participant disconnects from the synchronizer
  *   - A creation transaction to create a contract with Alice as signatory and Bob as observer is
  *     sequenced
  *   - Source participant approves/confirms transaction
  *   - Source participant authorizes Alice->target setting the onboarding flag
  *   - ACS snapshot for Alice is taken on source participant
  *   - ACS is imported on the target participant, which then reconnects to the synchronizers
  *   - Target participant clears the onboarding flag unilaterally
  *   - Assert successful clearing of onboarding flag by the target participant
  */
sealed trait OfflinePartyReplicationOnboardingClearanceIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      import env.*
      source = participant1
      target = participant2
    }
}

final class OffPROnboardingClearanceIntegrationTest
    extends OfflinePartyReplicationOnboardingClearanceIntegrationTest {

  "Party replication sets and clears the onboarding flag successfully" in { implicit env =>
    import env.*

    IouSyntax.createIou(participant1)(alice, bob, 3.33).discard

    target.topology.party_to_participant_mappings
      .propose_delta(
        party = alice,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = daId,
        requiresPartyToBeOnboarded = true,
      )

    target.synchronizers.disconnect_all()

    val createIouCmd = testIou(alice, bob, 2.20).create().commands().asScala.toSeq

    source.ledger_api.javaapi.commands.submit(
      Seq(alice),
      createIouCmd,
      daId,
      optTimeout = None,
    )

    val ledgerEndP1 = source.ledger_api.state.end()

    source.topology.party_to_participant_mappings.propose_delta(
      party = alice,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target,
      beginOffsetExclusive = ledgerEndP1,
      exportFilePath = acsSnapshotPath,
    )

    target.repair.import_acs(acsSnapshotPath)

    assertOnboardingFlag(daId, setOnTarget = true)

    target.synchronizers.reconnect(daName)

    source.health.ping(target)
    target.ledger_api.state.acs.of_party(alice).size shouldBe 2

    // unilateral onboarding flag clearance
    target.topology.party_to_participant_mappings.propose_delta(
      party = alice,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = false,
    )

    assertOnboardingFlag(daId, setOnTarget = false)

  }

  private def assertOnboardingFlag(daId: => PhysicalSynchronizerId, setOnTarget: Boolean) =
    eventually() {
      val lastPTP =
        source.topology.party_to_participant_mappings.list(synchronizerId = daId).last.item

      lastPTP.partyId shouldBe alice
      lastPTP.participants should have size 2

      forExactly(1, lastPTP.participants) { p =>
        p.participantId shouldBe source.id
        p.onboarding should be(false)
      }
      forExactly(1, lastPTP.participants) { p =>
        p.participantId shouldBe target.id
        p.onboarding should be(setOnTarget)
      }
    }
}
