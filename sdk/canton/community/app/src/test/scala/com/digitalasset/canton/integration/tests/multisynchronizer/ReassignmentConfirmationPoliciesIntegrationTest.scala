// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  EntitySyntax,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
  PartiesAllocator,
  PartyToParticipantDeclarative,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty
import com.digitalasset.canton.topology.ForceFlags
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.{BaseTest, config}

sealed trait ReassignmentConfirmationPoliciesPartyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with EntitySyntax {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

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

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)
      }

  "confirmation policies for reassignments" should {
    "confirmation of the assignment only depends on the target topology" in { implicit env =>
      import env.*

      /** Desired topology:
        *   - signatory is hosted on p1: da (submission), acme (observation)
        *   - signatory is hosted on p2: da (observation), acme (submission)
        *   - observer is hosted on p2: da (observation), acme (observation)
        *
        * Roles:
        *   - p1 and p2 are reassigning for signatory
        *   - p2 is reassigning for observer
        *   - p1 is signatory unassigning for signatory (even though it cannot confirm on acme on
        *     behalf of signatory)
        *   - p2 is signatory assigning for signatory (even though it cannot confirm on da on behalf
        *     of signatory)
        */
      PartiesAllocator(Set(participant1, participant2))(
        newParties = Seq(
          "signatory" -> participant1,
          "observer" -> participant2,
        ),
        targetTopology = Map(
          "signatory" -> Map(
            daId -> (PositiveInt.one, Set((participant1, Submission), (participant2, Observation))),
            acmeId -> (PositiveInt.one, Set(
              (participant1, Observation),
              (participant2, Confirmation),
            )),
          ),
          "observer" -> Map(
            daId -> (PositiveInt.one, Set((participant2, Observation))),
            acmeId -> (PositiveInt.one, Set((participant2, Observation))),
          ),
        ),
      )
      val signatory = "signatory".toPartyId(participant1)
      val observer = "observer".toPartyId(participant2)

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      participant1.ledger_api.commands
        .submit_reassign(signatory, LfContractId.assertFromString(iou.id.contractId), daId, acmeId)
    }
  }

  "signatory assigning participant can confirm for all the parties they host" in { implicit env =>
    import env.*

    /*
      Contract being reassigned: Iou with Alice as signatory, Bob as observer
      Topology is changed between unassignment and assignment:

           P1 -------------------------> acme                           P1 -------------------------> acme
           |           Alice             ▲                              |                             ▲
           |                             |                              |                             | Bob
      Alice|                             |Bob   -------------------->   |                             | Alice
           |                             |                              |                             |
           ▼                             |                              ▼                             |
           da <------------------------ P2                              da <------------------------ P2
                        Bob                                                          Bob


    Assignment succeeds because P2 is a reassigning participant for Bob and
    can confirm on behalf of Alice on acme.
     */

    val alice = participant1.parties.enable(
      "alice",
      synchronizeParticipants = Seq(participant2),
    )
    val bob = participant2.parties.enable(
      "bob",
      synchronizeParticipants = Seq(participant1),
    )

    val iou = IouSyntax.createIou(participant1, Some(daId))(alice, bob)

    val unassignId = participant1.ledger_api.commands
      .submit_unassign(alice, LfContractId.assertFromString(iou.id.contractId), daId, acmeId)
      .unassignId

    PartyToParticipantDeclarative(Set(participant1, participant2), Set(daId, acmeId))(
      owningParticipants = Map(alice -> participant1),
      targetTopology = Map(
        // Only: alice -> P2 -> acme
        alice -> Map(
          acmeId -> (PositiveInt.one, Set((participant2, Confirmation)))
        )
      ),
      forceFlags = ForceFlags(AllowInsufficientSignatoryAssigningParticipantsForParty),
    )

    // Alice can submit the assignment
    participant2.ledger_api.commands.submit_assign(alice, unassignId, daId, acmeId)
  }
}

class ReassignmentConfirmationPoliciesPartyIntegrationTestPostgres
    extends ReassignmentConfirmationPoliciesPartyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}
