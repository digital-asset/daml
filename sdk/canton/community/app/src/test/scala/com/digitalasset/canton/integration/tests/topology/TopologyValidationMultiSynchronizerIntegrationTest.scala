// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.examples.java.cycle as C
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.util.{
  EntitySyntax,
  PartiesAllocator,
  PartyToParticipantDeclarative,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.topology.ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty
import com.digitalasset.canton.topology.ForceFlags
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.{BaseTest, config}

class TopologyValidationMultiSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with EntitySyntax {

  registerPlugin(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        // Disable automatic assignment so that we really control it
        def disableAutomaticAssignment(
            sequencer: LocalSequencerReference
        ): Unit =
          sequencer.topology.synchronizer_parameters
            .propose_update(
              sequencer.synchronizer_id,
              _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
            )

        disableAutomaticAssignment(sequencer1)
        disableAutomaticAssignment(sequencer2)

        val alice = "alice"
        // Enable alice on other participants, on all synchronizers
        new PartiesAllocator(participants.all.toSet)(
          Seq(alice -> participant1),
          Map(
            alice -> Map(
              daId -> (PositiveInt.one, Set((participant1, Submission))),
              acmeId -> (PositiveInt.one, Set((participant1, Submission))),
            )
          ),
        ).run()

      }

  "disable party with incomplete reassignments" in { implicit env =>
    import env.*

    val aliceId = "alice".toPartyId(participant1)
    createCycleContract(participant1, aliceId, "alice-contract")

    val cid = participant1.ledger_api.javaapi.state.acs.await(C.Cycle.COMPANION)(aliceId)

    participant1.ledger_api.commands
      .submit_unassign(aliceId, Seq(cid.id.toLf), daId, acmeId)

    assertThrowsAndLogsCommandFailures(
      participant1.topology.party_to_participant_mappings.propose_delta(
        aliceId,
        removes = List(participant1.id),
        store = acmeId,
      ),
      _.commandFailureMessage should include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/TOPOLOGY_INSUFFICIENT_SIGNATORY_ASSIGNING_PARTICIPANTS\\(9,.*\\): Disable party $aliceId " +
        s"failed because there are incomplete reassignments",
    )

    participant1.topology.party_to_participant_mappings.propose_delta(
      aliceId,
      removes = List(participant1.id),
      store = acmeId,
      forceFlags = ForceFlags(AllowInsufficientSignatoryAssigningParticipantsForParty),
    )

  }

  "disable multi-hosted party with incomplete reassignments" in { implicit env =>
    import env.*

    val bob = "bob"
    new PartiesAllocator(participants.all.toSet)(
      Seq(bob -> participant1),
      Map(
        bob -> Map(
          daId -> (PositiveInt.one, Set((participant1, Submission))),
          acmeId -> (PositiveInt.one, Set((participant1, Submission), (participant2, Submission))),
        )
      ),
    ).run()

    val bobId = bob.toPartyId(participant1)
    createCycleContract(participant1, bobId, "bob-contract")

    val cid = participant1.ledger_api.javaapi.state.acs.await(C.Cycle.COMPANION)(bobId)

    participant1.ledger_api.commands
      .submit_unassign(bobId, Seq(cid.id.toLf), daId, acmeId)
      .unassignId

    participant1.topology.party_to_participant_mappings.propose_delta(
      bobId,
      removes = List(participant2.id),
      store = acmeId,
    )

    assertThrowsAndLogsCommandFailures(
      PartyToParticipantDeclarative.forParty(Set(participant1, participant2), acmeId)(
        participant1,
        bobId,
        PositiveInt.two,
        Set(participant1.id -> Submission, participant2.id -> Submission),
      ),
      _.commandFailureMessage should include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/TOPOLOGY_INSUFFICIENT_SIGNATORY_ASSIGNING_PARTICIPANTS\\(9,.*\\): Increasing the threshold to 2 for the party $bobId " +
        s"would result in insufficient signatory-assigning participants for reassignment",
    )

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), acmeId)(
      participant1,
      bobId,
      PositiveInt.two,
      Set(participant1.id -> Submission, participant2.id -> Submission),
      forceFlags = ForceFlags(AllowInsufficientSignatoryAssigningParticipantsForParty),
    )
  }

  "change permission to observer" in { implicit env =>
    import env.*

    new PartiesAllocator(participants.all.toSet)(
      Seq("alex" -> participant1),
      Map(
        "alex" -> Map(
          daId -> (PositiveInt.one, Set((participant1, Submission))),
          acmeId -> (PositiveInt.one, Set((participant1, Submission))),
        )
      ),
    ).run()
    val alexId = "alex".toPartyId(participant1)
    createCycleContract(participant1, alexId, "alex-contract")

    val cid = participant1.ledger_api.javaapi.state.acs.await(C.Cycle.COMPANION)(alexId)

    participant1.ledger_api.commands
      .submit_unassign(alexId, Seq(cid.id.toLf), daId, acmeId)
      .unassignId

    assertThrowsAndLogsCommandFailures(
      PartyToParticipantDeclarative.forParty(Set(participant1, participant2), acmeId)(
        participant1,
        alexId,
        PositiveInt.one,
        Set(participant1.id -> Observation),
      ),
      _.commandFailureMessage should include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/TOPOLOGY_INSUFFICIENT_SIGNATORY_ASSIGNING_PARTICIPANTS\\(9,.*\\): Changing the party to participant mapping for party $alexId " +
        s"would result in insufficient signatory-assigning participants for reassignment",
    )

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), acmeId)(
      participant1,
      alexId,
      PositiveInt.one,
      Set(participant1.id -> Observation),
      forceFlags = ForceFlags(AllowInsufficientSignatoryAssigningParticipantsForParty),
    )

  }
}
