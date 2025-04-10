// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{LocalSequencerReference, ParticipantReference}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
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
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.{BaseTest, SynchronizerAlias, config}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/*
This test checks that confirmation are respected for unassignments and assignments

Topology for this test:
- Synchronizers: da, acme
- Parties: Alice with confirmation threshold 2, Bob with confirmation threshold 1
- Topology
  - da:
    - P1: Alice, Bob
    - P2: Alice, Bob
    - P3: Alice
  - acme:
    - P1: Alice, Bob
    - P2: Alice
    - P3: Alice

Reassigning participants:
- For Alice: P1, P2, P3
- For Bob: P1
 */
sealed trait ReassignmentsConfirmationThresholdIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer
    with EntitySyntax {

  private var alice: PartyId = _
  private var bob: PartyId = _

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.reassignmentTimeProofFreshnessProportion)
            .replace(NonNegativeInt.zero)
        ),
      )
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

        programmableSequencers.put(
          daName,
          getProgrammableSequencer(sequencer1.name),
        )
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))

        setupTopology()

        IouSyntax.createIou(participant1, Some(daId))(alice, alice)
        IouSyntax.createIou(participant1, Some(daId))(alice, alice)
        IouSyntax.createIou(participant1, Some(daId))(alice, alice)
        IouSyntax.createIou(participant1, Some(daId))(bob, bob)

        changeAliceConfirmationThresholds()
      }

  // Allocate the multi-hosted parties
  private def setupTopology()(implicit env: TestConsoleEnvironment) = {
    import env.*

    val allParticipants = List(participant1, participant2, participant3)
    val allParticipantsIds = List(participant1, participant2, participant3).map(_.id)

    PartiesAllocator(allParticipants.toSet)(
      newParties = Seq("alice" -> participant1, "bob" -> participant2),
      targetTopology = Map(
        "alice" -> Map(
          // Enable alice on other participants, on all Synchronizers
          daId -> (PositiveInt.one, allParticipantsIds
            .map(_ -> ParticipantPermission.Submission)
            .toSet),
          acmeId -> (PositiveInt.one, allParticipantsIds
            .map(_ -> ParticipantPermission.Submission)
            .toSet),
        ),
        "bob" -> Map(
          // Enable bob on P2 -> da, P3 -> acme
          daId -> (PositiveInt.one, Set(
            participant1.id -> ParticipantPermission.Submission,
            participant2.id -> ParticipantPermission.Submission,
          )),
          acmeId -> (PositiveInt.one, Set(
            participant1.id -> ParticipantPermission.Submission,
            participant3.id -> ParticipantPermission.Submission,
          )),
        ),
      ),
    )
    alice = "alice".toPartyId(participant1)
    bob = "bob".toPartyId(participant2)

    val expectedTopology = Map(
      alice -> Map(daId -> allParticipantsIds, acmeId -> allParticipantsIds),
      bob -> Map(
        daId -> Seq(participant1.id, participant2.id),
        acmeId -> Seq(participant1.id, participant3.id),
      ),
    )

    eventually() {
      for {
        (party, synchronizerToParticipants) <- expectedTopology
        (synchronizerId, participants) <- synchronizerToParticipants
      } yield participant1.topology.party_to_participant_mappings.is_known(
        synchronizerId,
        party,
        participants,
      ) shouldBe true
    }
  }

  private def changeAliceConfirmationThresholds()(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val allParticipants = Set[ParticipantReference](participant1, participant2, participant3)
    val targetTopology: (PositiveInt, Set[(ParticipantId, ParticipantPermission)]) =
      (PositiveInt.two, allParticipants.map(p => (p.id, ParticipantPermission.Submission)))

    PartyToParticipantDeclarative(allParticipants, Set(acmeId, daId))(
      Map(alice -> participant1),
      Map(alice -> Map(daId -> targetTopology, acmeId -> targetTopology)),
    )

    participant1.testing.fetch_synchronizer_times()
    participant2.testing.fetch_synchronizer_times()
    participant3.testing.fetch_synchronizer_times()
  }

  private lazy val reassignmentFailureLogAssertions: Seq[LogEntry] => Assertion =
    LogEntry.assertLogSeq(
      Seq(
        (
          _.warningMessage should include regex "Response message for request .* timed out",
          "participant timeout",
        )
      ),
      Seq(
        _.warningMessage should include regex "has exceeded the max-sequencing-time .* of the send request",
        _.warningMessage should include("Sequencing result message timed out"),
        _.warningMessage should include(
          "Rejected transaction due to a participant determined timeout"
        ),
      ),
    )

  "Confirmation thresholds for reassignments" should {
    // Count the number of confirmation responses sent by each participant
    def countConfirmationResponsesPolicy(
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

    /** Drop confirmation responses of participants
      *
      * @param denied
      *   Participants whose confirmation responses are set
      * @param completeAfterResponses
      *   If defined, the specified promise will be completed once sufficiently many responses have
      *   been through
      */
    def dropConfirmationResponsesFromPolicy(
        denied: Set[ParticipantId],
        completeAfterResponses: Option[(AtomicLong, Int, Promise[Unit])] = None,
    ): SendPolicyWithoutTraceContext = submissionRequest =>
      submissionRequest.sender match {
        case _: ParticipantId =>
          if (ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest)) {
            val sender = ParticipantId(submissionRequest.sender.uid)

            val shouldProcess = !denied.contains(sender)

            if (shouldProcess) {
              completeAfterResponses match {
                case Some((counter, confirmationsBeforeCompletion, promise)) =>
                  val newValue = counter.incrementAndGet()
                  if (newValue >= confirmationsBeforeCompletion) promise.success(())

                case None => ()
              }

              logger.debug(s"Processing confirmation response from $sender")
              SendDecision.Process
            } else {
              logger.debug(s"Dropping confirmation response from $sender")
              SendDecision.Drop
            }

          } else
            SendDecision.Process

        case _ => SendDecision.Process
      }

    "only reassigning participants should send confirmation responses" in { implicit env =>
      import env.*

      val daConfirmations = new TrieMap[ParticipantId, Int]()
      val acmeConfirmations = new TrieMap[ParticipantId, Int]()

      programmableSequencers(daName).setPolicy_("confirmations count")(
        countConfirmationResponsesPolicy(daConfirmations)
      )

      programmableSequencers(acmeName).setPolicy_("confirmations count")(
        countConfirmationResponsesPolicy(acmeConfirmations)
      )

      val bobIou = participant1.ledger_api.javaapi.state.acs.await(Iou.COMPANION)(bob)

      val unassignId =
        participant1.ledger_api.commands
          .submit_unassign(bob, bobIou.id.toLf, daId, acmeId)
          .unassignId

      participant1.ledger_api.commands.submit_assign(bob, unassignId, daId, acmeId)

      // Only P1 is a reassigning participant
      daConfirmations shouldBe Map(participant1.id -> 1)
      acmeConfirmations shouldBe Map(participant1.id -> 1)

      programmableSequencers(daName).resetPolicy()
      programmableSequencers(acmeName).resetPolicy()
    }

    "not require confirmation on both synchronizers" in { implicit env =>
      import env.*
      environment.simClock.value.advance(Duration.ofMinutes(1))

      /*
        Scenario:
        - da: charlie hosted on p1 and p2 with confirmation rights, threshold=2
        - acme: charlie hosted on p1 with confirmation rights and p2 with observations rights, threshold=1

        There are:
        - Two reassigning participants (p1, p2)
        - Two signatory unassigning participants (p1, p2)
        - One signatory assigning participant (p1)

        This is sufficient for the reassignment to go through
       */

      PartiesAllocator(Set(participant1, participant2))(
        newParties = Seq("charlie" -> participant1),
        targetTopology = Map(
          "charlie" -> Map(
            daId -> (PositiveInt.one, Set(
              participant1.id -> ParticipantPermission.Submission,
              participant2.id -> ParticipantPermission.Confirmation,
            )),
            acmeId -> (PositiveInt.one, Set(
              participant1.id -> ParticipantPermission.Confirmation,
              participant2.id -> ParticipantPermission.Observation,
            )),
          )
        ),
      )
      val charlie = "charlie".toPartyId(participant1)

      val iou = IouSyntax.createIou(participant1, Some(daId))(charlie, charlie)

      // This is just to increase the threshold to 2
      PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
        participant1,
        charlie,
        PositiveInt.two,
        Set(
          (participant1.id, ParticipantPermission.Confirmation),
          (participant2.id, ParticipantPermission.Confirmation),
        ),
      )

      // advance the time to force a new time proof
      environment.simClock.value.advance(Duration.ofSeconds(1))

      participant1.testing.fetch_synchronizer_times()

      participant1.ledger_api.commands.submit_reassign(
        charlie,
        LfContractId.assertFromString(iou.id.contractId),
        daId,
        acmeId,
      )
    }

    "unavailable participants don't block reassigning provided threshold is met" in {
      implicit env =>
        import env.*

        programmableSequencers(daName).setPolicy_("da policy")(
          dropConfirmationResponsesFromPolicy(Set(participant3))
        )
        programmableSequencers(acmeName).setPolicy_("acme policy")(
          dropConfirmationResponsesFromPolicy(Set(participant3))
        )

        val aliceIou = participant1.ledger_api.javaapi.state.acs
          .await(Iou.COMPANION)(alice, synchronizerFilter = Some(daId))

        val unassignId =
          participant1.ledger_api.commands
            .submit_unassign(alice, aliceIou.id.toLf, daId, acmeId)
            .unassignId

        participant1.ledger_api.commands.submit_assign(alice, unassignId, daId, acmeId)

        programmableSequencers(daName).resetPolicy()
        programmableSequencers(acmeName).resetPolicy()
    }

    "sufficiently many unavailable participants block unassignment" in { implicit env =>
      import env.*

      val aliceIou = participant1.ledger_api.javaapi.state.acs
        .await(Iou.COMPANION)(alice, synchronizerFilter = Some(daId))

      val parameters =
        sequencer1.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(daId)
      val expiredDecisionTimeout = parameters.decisionTimeout.plusSeconds(1)
      val progressTimeP = Promise[Unit]()

      // Unassignment will not go through because we need two confirmations
      val failingUnassignmentConfirmationCounter = new AtomicLong(0)
      programmableSequencers(daName).setPolicy_("da policy")(
        dropConfirmationResponsesFromPolicy(
          Set(participant2, participant3),
          // We don't want to progress time before P1 confirmation response
          Some((failingUnassignmentConfirmationCounter, 1, progressTimeP)),
        )
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val failingUnassignmentF = Future {
            failingUnassignment(
              cid = aliceIou.id.toLf,
              source = daId,
              target = acmeId,
              submittingParty = alice,
              participantOverrideO = Some(participant1),
            )
          }

          progressTimeP.future.futureValue
          environment.simClock.value.advance(expiredDecisionTimeout.asJava)
          mediator1.testing.fetch_synchronizer_time() // Needed to trigger the negative verdict

          val commandRejectedStatus = failingUnassignmentF.futureValue.status.value
          commandRejectedStatus.code should not be 0
          commandRejectedStatus.message should include(
            "Rejected transaction due to a participant determined timeout"
          )

          // Unassignment should now go through
          programmableSequencers(daName).resetPolicy()
          participant1.ledger_api.commands.submit_unassign(alice, aliceIou.id.toLf, daId, acmeId)
        },
        reassignmentFailureLogAssertions,
      )

      programmableSequencers(daName).resetPolicy()
      programmableSequencers(acmeName).resetPolicy()
    }

    "sufficiently many unavailable participants block assignment" in { implicit env =>
      import env.*

      val aliceIou = participant1.ledger_api.javaapi.state.acs
        .await(Iou.COMPANION)(alice, synchronizerFilter = Some(daId))

      val parameters =
        sequencer2.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(acmeId)
      val expiredDecisionTimeout = parameters.decisionTimeout.plusSeconds(1)
      val progressTimeP = Promise[Unit]()

      val unassignId = participant1.ledger_api.commands
        .submit_unassign(alice, aliceIou.id.toLf, daId, acmeId)
        .unassignId

      // Assignment will not go through because we need two confirmations
      val failingAssignmentConfirmationCounter = new AtomicLong(0)
      programmableSequencers(acmeName).setPolicy_("acme policy")(
        dropConfirmationResponsesFromPolicy(
          Set(participant2, participant3),
          // We don't want to progress time before P1 confirmation response
          Some((failingAssignmentConfirmationCounter, 1, progressTimeP)),
        )
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val failingAssignmentF = Future {
            failingAssignment(
              unassignId = unassignId,
              source = daId,
              target = acmeId,
              submittingParty = alice,
              participantOverrideO = Some(participant1),
            )
          }

          progressTimeP.future.futureValue
          environment.simClock.value.advance(expiredDecisionTimeout.asJava)
          mediator2.testing.fetch_synchronizer_time() // Needed to trigger the negative verdict

          val commandRejectedStatus = failingAssignmentF.futureValue.status.value
          commandRejectedStatus.code should not be 0
          commandRejectedStatus.message should include(
            "Rejected transaction due to a participant determined timeout"
          )

          // Unassignment should now go through
          programmableSequencers(acmeName).resetPolicy()
          participant1.ledger_api.commands.submit_assign(alice, unassignId, daId, acmeId)
        },
        reassignmentFailureLogAssertions,
      )

    }
  }
}

class ReassignmentsConfirmationThresholdIntegrationTestPostgres
    extends ReassignmentsConfirmationThresholdIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
