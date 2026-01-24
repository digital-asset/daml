// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.admin.api.client.data.FlagSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersHistory,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.transaction.ParticipantPermission

import java.time.Duration
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
sealed trait OfflinePartyReplicationOnboardingFlagClearanceIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant2
      }
}

final class OffPROnboardingFlagClearanceIntegrationTest
    extends OfflinePartyReplicationOnboardingFlagClearanceIntegrationTest {

  "Party replication sets and clears the onboarding flag successfully" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value

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

    // Alice also needs to authorize the transaction
    alice.topology.party_to_participant_mappings.propose_delta(
      source,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target,
      beginOffsetExclusive = sourceLedgerEnd,
      exportFilePath = acsSnapshotPath,
    )

    target.parties.import_party_acsV2(acsSnapshotPath, daId)

    // Advance time to allow topology transactions to be processed
    clock.advance(Duration.ofSeconds(30))
    checkOnboardingFlag(daId, setOnTarget = true)

    val targetLedgerEnd = target.ledger_api.state.end()

    target.synchronizers.reconnect(daName)

    source.health.ping(target)
    target.ledger_api.state.acs.of_party(alice).size shouldBe 2

    // Trigger the asynchronous onboarding flag clearance process
    val status = target.parties.clear_party_onboarding_flag(alice, daId, targetLedgerEnd)
    val expectedTimestamp = computeExpectedDecisionDeadline(getOnboardingEffectiveAt(daId))

    status shouldBe FlagSet(expectedTimestamp)

    // Advance time to allow the asynchronous onboarding flag clearance to complete
    clock.advance(Duration.ofMinutes(2).plusSeconds(10))

    // Force both participants to process all messages up to the new clock time.
    // This ensures `source` processes the flag-clearing transaction sent by `target`.
    source.health.ping(target)
    target.health.ping(source)

    // Now that we've advanced time and synced, the onboarding flag on the target participant should be cleared
    checkOnboardingFlag(daId, setOnTarget = false)
  }

  private def checkOnboardingFlag(daId: => PhysicalSynchronizerId, setOnTarget: Boolean): Unit = {
    val lastPTP =
      source.topology.party_to_participant_mappings.list(synchronizerId = daId).last.item

    lastPTP.partyId shouldBe alice.partyId
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

  private def computeExpectedDecisionDeadline(
      effectiveAt: CantonTimestamp
  ): CantonTimestamp = {
    val paramsWithValidity = DynamicSynchronizerParametersWithValidity(
      parameters = DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
      validFrom = CantonTimestamp.MinValue,
      validUntil = None,
    )

    DynamicSynchronizerParametersHistory
      .latestDecisionDeadlineEffectiveAt(
        Seq(paramsWithValidity),
        effectiveAt,
      )
  }
}
