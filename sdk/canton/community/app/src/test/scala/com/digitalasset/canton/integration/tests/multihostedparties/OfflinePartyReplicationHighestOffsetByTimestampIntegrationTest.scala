// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.transaction.ParticipantPermission

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Intention:
  *   - Model the party replication as CN is currently doing to assert/investigate (see #29121)
  *   - Follow the recommended party replication flow as closely as possibly until the
  *     find_highest_offset_by_timestamp endpoint is used
  *
  * Setup:
  *   - Alice is hosted on participant1 (Source)
  *   - Bob is hosted on participant2 (Target)
  *   - 2 active IOU contract between Alice (signatory) and Bob (observer)
  *
  * Test: Activate Alice on dtarget
  *   - Target participant authorizes Alice->target
  *   - Target participant disconnects from the synchronizer
  *   - A creation transaction to create a contract with Alice as signatory and Bob as observer is
  *     sequenced
  *   - Source participant approves/confirms transaction
  *   - Source participant authorizes Alice->target
  *   - Find the offset for the ACS export using the validForm timestamp of the effective topology
  *     transaction
  *   - Assert that the found offset for the given timestamp is as expected
  */
sealed trait OfflinePartyReplicationHighestOffsetByTimestampIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant2
      }
}

final class OffPRHighestOffsetByTimestampIntegrationTest
    extends OfflinePartyReplicationHighestOffsetByTimestampIntegrationTest {

  "Party replication using the find highest offset by timestamp endpoint" in { implicit env =>
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

    val sourceLedgerEnd = source.ledger_api.state.end()

    alice.topology.party_to_participant_mappings.propose_delta(
      node = source,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )

    val effectiveTimestamp = eventually() {
      getOnboardingEffectiveAt(daId)
    }

    val offsetByTimestamp =
      source.parties.find_highest_offset_by_timestamp(daId, effectiveTimestamp.toInstant)

    val maxActivationOffset = source.parties.find_party_max_activation_offset(
      alice,
      target,
      daId,
      Some(effectiveTimestamp.toInstant),
      beginOffsetExclusive = sourceLedgerEnd,
      completeAfter = PositiveInt.one,
    )

    offsetByTimestamp shouldBe maxActivationOffset
  }

  /** Gets the `validFrom` timestamp of the PTP transaction that onboarded the party to the target.
    */
  private def getOnboardingEffectiveAt(
      daId: PhysicalSynchronizerId
  ): CantonTimestamp =
    CantonTimestamp
      .fromInstant(
        source.topology.party_to_participant_mappings
          .list(
            synchronizerId = daId,
            filterParty = alice.filterString,
            filterParticipant = target.id.filterString,
          )
          .loneElement
          .context
          .validFrom
      )
      .value

}
