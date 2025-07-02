// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, LocalSequencerReference}
import com.digitalasset.canton.data.ReassignmentRef
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
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.participant.state.ReassignmentCommandsBatch.NoCommands
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.NotHostedOnParticipant
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.{BaseTest, config}
import monocle.macros.syntax.lens.*

sealed trait ReassignmentSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer
    with EntitySyntax {

  private var signatory: PartyId = _
  private var observer1: PartyId = _
  private var decentralizedParty: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      // We want to trigger time out
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          // Make sure that unassignment picks a recent target synchronizer topology snapshot
          // TODO(#25110): Remove this configuration once the correct snapshot is used in computing
          //               the vetting checks for the target synchronizer
          _.focus(_.parameters.reassignmentTimeProofFreshnessProportion)
            .replace(NonNegativeInt.zero)
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        disableAutomaticAssignment(sequencer1)
        disableAutomaticAssignment(sequencer2)

        PartiesAllocator(participants.all.toSet)(
          Seq("dso" -> participant1, "signatory" -> participant1, "observer1" -> participant2),
          Map(
            "dso" -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1, Submission),
                (participant2, Submission),
              )),
              acmeId -> (PositiveInt.one, Set(
                (participant1, Submission),
                (participant2, Submission),
              )),
            ),
            "signatory" -> Map(
              daId -> (PositiveInt.one, Set((participant1, Submission))),
              acmeId -> (PositiveInt.one, Set((participant1, Submission))),
            ),
            "observer1" -> Map(
              daId -> (PositiveInt.one, Set((participant2, Submission))),
              acmeId -> (PositiveInt.one, Set((participant2, Submission))),
            ),
          ),
        )

        signatory = "signatory".toPartyId(participant1)
        observer1 = "observer1".toPartyId(participant2)
        decentralizedParty = "dso".toPartyId(participant1)
      }

  "check that a decentralized party can submit a reassignment" in { implicit env =>
    import env.*

    val iou = IouSyntax.createIou(participant1, Some(daId))(decentralizedParty, observer1)

    // increase the threshold to 2
    Seq(daId, acmeId).foreach { synchronizerId =>
      PartyToParticipantDeclarative.forParty(Set(participant1, participant2), synchronizerId)(
        participant1,
        decentralizedParty,
        PositiveInt.two,
        Set(
          (participant1.id, ParticipantPermission.Submission),
          (participant2.id, ParticipantPermission.Submission),
        ),
      )
    }

    participant2.ledger_api.commands
      .submit_reassign(decentralizedParty, Seq(iou.id.toLf), daId, acmeId)

    participant2.ledger_api.state.acs
      .active_contracts_of_party(party = decentralizedParty)
      .find(_.createdEvent.value.contractId == iou.id.contractId)
      .map(_.synchronizerId) shouldBe Some(acmeId.logical.toProtoPrimitive)
  }

  "check that reassignment can be submitted by any participant hosting a stakeholder" in {
    implicit env =>
      import env.*

      // change ParticipantPermission to Observation for observer1
      Seq(daId, acmeId).foreach { synchronizerId =>
        PartyToParticipantDeclarative.forParty(Set(participant2), synchronizerId)(
          participant2,
          observer1,
          PositiveInt.one,
          Set(participant2.id -> ParticipantPermission.Observation),
        )
      }

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer1)
      participant2.ledger_api.commands
        .submit_reassign(observer1, Seq(iou.id.toLf), daId, acmeId)

      participant2.ledger_api.state.acs
        .active_contracts_of_party(party = observer1)
        .find(_.createdEvent.value.contractId == iou.id.contractId)
        .map(_.synchronizerId) shouldBe Some(acmeId.logical.toProtoPrimitive)

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant1.ledger_api.commands
          .submit_reassign(observer1, Seq(iou.id.toLf), acmeId, daId),
        forAll(_)(
          _.message should include(
            NotHostedOnParticipant(
              ReassignmentRef(iou.id.toLf),
              observer1.toLf,
              participant1.id,
            ).message
          )
        ),
      )
  }

  "check that reassignment does not expose the reassignment events if called without an event format" in {
    implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer1)
      val unassigned = participant2.ledger_api.commands
        .submit_unassign_with_format(observer1, Seq(iou.id.toLf), daId, acmeId, eventFormat = None)

      unassigned.reassignment.events shouldBe empty
  }

  "check that we can reassign two contracts in one command" in { implicit env =>
    import env.*

    val iou1 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer1)
    val iou2 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer1)

    val (unassigned, assigned) = participant1.ledger_api.commands.submit_reassign(
      signatory,
      Seq(iou1.id.toLf, iou2.id.toLf),
      daId,
      acmeId,
    )

    unassigned.reassignmentId shouldBe assigned.reassignmentId

    unassigned.events.size shouldBe 2
    assigned.events.size shouldBe 2
  }

  "check that we fail if unassignment contains no contract ids" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
      participant1.ledger_api.commands
        .submit_unassign(
          submitter = signatory,
          contractIds = Seq.empty,
          source = daId,
          target = acmeId,
        ),
      forAll(_)(
        _.message should include(NoCommands.error)
      ),
    )
  }

  // Disable automatic assignment so that we really control it
  private def disableAutomaticAssignment(
      sequencer: LocalSequencerReference
  ): Unit = {
    sequencer.topology.synchronizer_parameters
      .propose_update(
        sequencer.synchronizer_id,
        _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
      )

    eventually() {
      sequencer.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(sequencer.synchronizer_id)
        .assignmentExclusivityTimeout shouldBe config.NonNegativeFiniteDuration.Zero
    }
  }
}

class ReassignmentSubmissionIntegrationTestPostgres extends ReassignmentSubmissionIntegrationTest {
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
