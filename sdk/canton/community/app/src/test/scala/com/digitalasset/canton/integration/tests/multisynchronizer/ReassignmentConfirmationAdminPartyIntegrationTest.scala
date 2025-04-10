// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.{BaseTest, SynchronizerAlias, config}

import java.time.Duration
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/*
This test checks that the admin party of the submitting participant is required for reassignments.
 */
sealed trait ReassignmentConfirmationAdminPartyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer {

  private var signatory: PartyId = _
  private var observer: PartyId = _

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      // We want to trigger time out
      .addConfigTransforms(ConfigTransforms.useStaticTime)
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

        signatory = participant1.parties.enable(
          "signatory",
          synchronizeParticipants = Seq(participant2),
        )
        observer = participant2.parties.enable(
          "observer",
          synchronizeParticipants = Seq(participant1),
        )

        programmableSequencers.put(
          daName,
          getProgrammableSequencer(sequencer1.name),
        )
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))
      }

  "admin party of a submitting participant" should {
    "confirm a reassignment" in { implicit env =>
      import env.*

      val daConfirmations = new TrieMap[ParticipantId, Int]()
      val acmeConfirmations = new TrieMap[ParticipantId, Int]()

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      programmableSequencers(daName).setPolicy_("confirmations count")(
        countConfirmationResponsesPolicy(daConfirmations)
      )
      programmableSequencers(acmeName).setPolicy_("confirmations count")(
        countConfirmationResponsesPolicy(acmeConfirmations)
      )

      val unassignId = participant2.ledger_api.commands
        .submit_unassign(observer, iou.id.toLf, daId, acmeId)
        .unassignId

      // The observer doesn't confirm the unassignment, but the admin party of participant2 will confirm. Therefor the counter will increase
      daConfirmations(participant2) shouldBe 1

      participant2.ledger_api.commands
        .submit_assign(observer, unassignId, daId, acmeId)

      acmeConfirmations(participant2) shouldBe 1
    }

    "fails if the admin party does not confirm the reassignment" in { implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      programmableSequencers(daName).setPolicy_("drop confirmation response")(
        dropConfirmationRequest(participant2)
      )

      assertMediatorTimout(
        commandId =>
          participant2.ledger_api.commands
            .submit_unassign_async(
              observer,
              iou.id.toLf,
              daId,
              acmeId,
              commandId = commandId,
            ),
        daName,
        daId,
      )

      val unassignId = participant2.ledger_api.commands
        .submit_unassign(observer, iou.id.toLf, daId, acmeId)
        .unassignId

      programmableSequencers(acmeName).setPolicy_("drop confirmation response")(
        dropConfirmationRequest(participant2)
      )

      assertMediatorTimout(
        commandId =>
          participant2.ledger_api.commands
            .submit_assign_async(
              observer,
              unassignId,
              daId,
              acmeId,
              commandId = commandId,
            ),
        acmeName,
        acmeId,
      )

      programmableSequencers(acmeName).resetPolicy()
      participant2.ledger_api.commands
        .submit_assign(
          observer,
          unassignId,
          daId,
          acmeId,
        )

      participant2.ledger_api.state.acs
        .active_contracts_of_party(party = observer)
        .find(_.createdEvent.value.contractId == iou.id.contractId)
        .map(_.synchronizerId) shouldBe Some(acmeId.toProtoPrimitive)
    }
  }

  // Count the number of confirmation responses sent by each participant
  private def countConfirmationResponsesPolicy(
      confirmations: TrieMap[ParticipantId, Int]
  ): SendPolicyWithoutTraceContext = submissionRequest =>
    submissionRequest.sender match {
      case pid: ParticipantId
          if ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest) =>
        val newValue = confirmations.getOrElse(pid, 0) + 1
        confirmations.put(pid, newValue)

        SendDecision.Process

      case _ => SendDecision.Process
    }

  private def dropConfirmationRequest(
      from: ParticipantId
  )(implicit env: TestConsoleEnvironment): SendPolicyWithoutTraceContext =
    submissionRequest =>
      submissionRequest.sender match {
        case participantId: ParticipantId
            if participantId == from && ProgrammableSequencerPolicies.isConfirmationResponse(
              submissionRequest
            ) =>
          logger.debug(s"Dropping confirmation response from $participantId")
          // advancing time to trigger the decision timeout (60s = default decision timeout)
          env.environment.simClock.value.advance(Duration.ofSeconds(60).plusSeconds(1))
          SendDecision.Drop
        case _ => SendDecision.Process
      }

  private def assertMediatorTimout(
      submit: String => Unit,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val ledgerEndBefore = participant2.ledger_api.state.end()
        val commandId = UUID.randomUUID().toString
        submit(commandId)
        val completion = participant2.ledger_api.completions
          .list(
            partyId = observer,
            atLeastNumCompletions = 1,
            beginOffsetExclusive = ledgerEndBefore,
            filter = _.commandId == commandId,
          )(0)
        completion.status
          .map(_.message)
          .foreach(
            _ should include("Rejected transaction due to a participant determined timeout")
          )

        programmableSequencers(synchronizerAlias).resetPolicy()
        participant2.health.ping(participant2, synchronizerId = Some(synchronizerId))
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (
            _.warningMessage should include regex "Response message for request .* timed out",
            "participant timeout",
          ),
          (_.warningMessage shouldBe "Sequencing result message timed out.", "mediator timeout"),
        )
      ),
    )
  }
}

class ReassignmentConfirmationAdminPartyIntegrationTestPostgres
    extends ReassignmentConfirmationAdminPartyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
