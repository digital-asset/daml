// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

final class FindPartyActivationOffsetsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  // A party gets activated on the multiple participants without being replicated (= ACS mismatch),
  // and we want to minimize the risk of warnings related to acs commitment mismatches
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
      }

  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory)
  )

  "Alice has 2 activations on P1 and P3" in { implicit env =>
    import env.*

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
    )
    val bob = participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
    )

    IouSyntax.createIou(participant1)(alice, bob, 9999.95).discard

    val ledgerEndP1 = participant1.ledger_api.state.end()

    PartyToParticipantDeclarative.forParty(Set(participant1, participant3), daId)(
      participant1,
      alice,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant3, PP.Submission),
      ),
    )

    clue(
      "Finds 1 topology transaction for Alice on P1 for when she has been authorized on P3"
    ) {
      participant1.parties
        .find_party_max_activation_offset(
          partyId = alice,
          participantId = participant3.id,
          synchronizerId = daId,
          beginOffsetExclusive = ledgerEndP1,
          completeAfter = PositiveInt.one,
        )
        .unwrap should be > 0L

    }
  }
}
