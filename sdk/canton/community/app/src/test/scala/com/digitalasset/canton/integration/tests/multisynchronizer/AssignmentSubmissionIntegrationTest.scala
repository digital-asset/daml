// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
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
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.{BaseTest, config}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt

sealed trait AssignmentSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer
    with EntitySyntax {

  private var signatory: PartyId = _
  private var observer1: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      // We want to trigger time out
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

        PartiesAllocator(participants.all.toSet)(
          Seq("signatory" -> participant1, "observer1" -> participant2),
          Map(
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
      }

  "check that we fail if the assignment is submitted by non initiator party within the exclusivity timeout" in {
    implicit env =>
      import env.*

      // change the exclusivity timeout to 1 minute
      sequencer2.topology.synchronizer_parameters
        .propose_update(
          sequencer2.synchronizer_id,
          _.update(assignmentExclusivityTimeout = 1.minute),
        )

      eventually() {
        participant2.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(sequencer2.synchronizer_id)
          .assignmentExclusivityTimeout shouldBe config.NonNegativeFiniteDuration(1.minute)

        participant1.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(sequencer2.synchronizer_id)
          .assignmentExclusivityTimeout shouldBe config.NonNegativeFiniteDuration(1.minute)
      }

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer1)
      val reassignmentId = participant1.ledger_api.commands
        .submit_unassign(
          submitter = signatory,
          contractIds = Seq(iou.id.toLf),
          source = daId,
          target = acmeId,
        )
        .reassignmentId

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant2.ledger_api.commands
          .submit_assign(
            submitter = observer1,
            source = daId,
            target = acmeId,
            reassignmentId = reassignmentId,
          ),
        forAll(_)(
          _.message should include(
            s"only ${signatory.toLf} can initiate before exclusivity timeout"
          )
        ),
      )

      participant1.ledger_api.commands
        .submit_assign(
          submitter = signatory,
          source = daId,
          target = acmeId,
          reassignmentId = reassignmentId,
        )
  }
}

class AssignmentSubmissionIntegrationTestPostgres extends AssignmentSubmissionIntegrationTest {
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
