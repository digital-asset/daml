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
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.protocol.LocalRejectError.UnassignmentRejects
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.{BaseTest, SynchronizerAlias, config}
import com.google.rpc.status.Status

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

trait ReassignmentServiceConcurrentReassignmentsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer {

  private val party1a = "party1"
  private val party1b = "party1b"
  private val party2 = "party2"

  private var party1aId: PartyId = _
  private var party2Id: PartyId = _

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

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

        party1aId = participant1.parties.enable(
          party1a
        )
        participant1.parties.enable(
          party1b
        )
        party2Id = participant2.parties.enable(
          party2
        )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        programmableSequencers.put(
          daName,
          getProgrammableSequencer(sequencer1.name),
        )
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))
      }

  "ReassignmentService" should {
    "emit command rejected for concurrent assignment (contract is locked)" in { implicit env =>
      import env.*

      val signatory = party1aId
      val observer = party2Id

      // Create contract that will be used
      val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val cid = contract.id.toLf

      val unassignId =
        participant1.ledger_api.commands
          .submit_unassign(signatory, Seq(cid), daId, acmeId)
          .unassignId

      // Check unassignment
      assertNotInLedgerAcsSync(
        participantRefs = List(participant1),
        partyId = signatory,
        synchronizerId = daId,
        cid = cid,
      )

      // Start of policy
      val submission1Phase3Done =
        Promise[Unit]() // Completed when phase 3 for P1 submission is done

      programmableSequencers(acmeName).setPolicy("concurrent assignment (locked contract)")(
        ProgrammableSequencerPolicies.lockedContract(submission1Phase3Done)
      )

      // Submit assignments
      val assignmentCompletion1F = Future {
        failingAssignment(
          unassignId = unassignId,
          source = daId,
          target = acmeId,
          submittingParty = signatory.toLf,
          participantOverrideO = Some(participant1),
        )
      }

      val assignmentCompletion2F = submission1Phase3Done.future.map { _ =>
        failingAssignment(
          unassignId = unassignId,
          source = daId,
          target = acmeId,
          submittingParty = observer.toLf,
          participantOverrideO = Some(participant2),
        )
      }

      // Checks
      val status1 = assignmentCompletion1F.futureValue.status.value
      val status2 = assignmentCompletion2F.futureValue.status.value

      status1 shouldBe Status() // Success for P1

      val expectedRejection = LocalRejectError.ConsistencyRejections.LockedContracts
        .Reject(Seq(s"coid=$cid"))
      status2.code should not be 0
      status2.message should include(expectedRejection._causePrefix)

      // Cleaning
      IouSyntax.archive(participant1)(contract, signatory)
    }

    "emit command rejected for concurrent unassignment (contract is locked)" in { implicit env =>
      import env.*

      val signatory = party1aId
      val observer = party2Id

      // Create contract
      val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val cid = contract.id.toLf

      // Completed when phase 3 for P1 submission is done
      val submission1Phase3Done = Promise[Unit]()

      programmableSequencers(daName).setPolicy("concurrent assignment (locked contract)")(
        ProgrammableSequencerPolicies.lockedContract(submission1Phase3Done)
      )

      // Submit assignments
      val assignmentCompletion1F = Future {
        failingUnassignment(
          cid = cid,
          source = daId,
          target = acmeId,
          submittingParty = signatory.toLf,
          participantOverrideO = Some(participant1),
        )
      }

      val assignmentCompletion2F = submission1Phase3Done.future.map { _ =>
        failingUnassignment(
          cid = cid,
          source = daId,
          target = acmeId,
          submittingParty = observer.toLf,
          participantOverrideO = Some(participant2),
        )
      }

      // Checks
      val status1 = assignmentCompletion1F.futureValue.status.value
      val status2 = assignmentCompletion2F.futureValue.status.value

      status1 shouldBe Status() // Success for P1

      val expectedRejection =
        UnassignmentRejects.ActivenessCheckFailed.Reject("")
      status2.code should not be 0
      status2.message should include(expectedRejection._causePrefix)
    }

    /*
      Both P1 and P2 will concurrently submit the assignment.
      We want the following situation, so that P1 has a command completed and P2 a command rejected
      (because the assignment was completed in the meantime).

           submit        3           7
      P1 ----|---------|---|-------|---|-------------------------------
      P2 --------|----------------------------|---|---------|-----|----
               submit                           3              7
     */
    "emit command rejected for concurrent assignment (contract already active on target)" in {
      implicit env =>
        import env.*

        val signatory = party1aId
        val observer = party2Id

        // Create contract
        val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
        val cid = contract.id.toLf

        // unassignment contract
        val unassignId =
          participant1.ledger_api.commands
            .submit_unassign(signatory, Seq(cid), daId, acmeId)
            .unassignId

        eventually() {
          // make sure the unassignment is completed on P2
          participant2.ledger_api.state.acs
            .incomplete_unassigned_of_party(signatory)
            .find(_.contractId == cid.coid) should not be empty
        }

        // Start of policy
        val submission1Done = Promise[Unit]() // Completed when phase 1 for P1 submission is done
        val submission1Phase7Done =
          Promise[Unit]() // Completed when verdict for P1 submission is sent
        val confirmationRequestsCount = new AtomicLong(0)

        val policy = SendPolicy.processTimeProofs_ { submissionRequest =>
          submissionRequest.sender match {
            case _: ParticipantId if submissionRequest.isConfirmationRequest =>
              val newConfirmationRequestsCount = confirmationRequestsCount.incrementAndGet()

              if (newConfirmationRequestsCount == 1) { // P1 submission
                submission1Done.trySuccess(())
                SendDecision.Process
              } else { // P2 submission should be delayed
                SendDecision.HoldBack(submission1Phase7Done.future)
              }

            case _ => SendDecision.Process
          }
        }

        programmableSequencers(acmeName).setPolicy_(
          "concurrent assignment (contract already active on target)"
        )(policy)

        // Submit assignments
        val assignmentCompletion1F = Future {
          failingAssignment(
            unassignId = unassignId,
            source = daId,
            target = acmeId,
            submittingParty = signatory.toLf,
            participantOverrideO = Some(participant1),
          )
        }

        // P2 submission should not be sent before P1's
        val assignmentCompletion2F = submission1Done.future.map { _ =>
          failingAssignment(
            unassignId = unassignId,
            source = daId,
            target = acmeId,
            submittingParty = observer.toLf,
            participantOverrideO = Some(participant2),
          )
        }

        // Ensure that second confirmation request is not sent before processing of first submission is completely finished
        val status1 = assignmentCompletion1F.futureValue.status.value
        submission1Phase7Done.trySuccess(())

        // Checks
        val status2 = assignmentCompletion2F.futureValue.status.value

        status1 shouldBe Status() // Success for P1

        val expectedRejection =
          LocalRejectError.AssignmentRejects.AlreadyCompleted.Reject("")
        status2.code should not be 0
        status2.message should include(expectedRejection._causePrefix)

        // Cleaning
        IouSyntax.archive(participant1)(contract, signatory)
    }
  }
}

//class ReassignmentServiceConcurrentReassignmentsIntegrationTestDefault
//    extends ReassignmentServiceConcurrentReassignmentsIntegrationTest {
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = Seq(Set("sequencer1"), Set("sequencer2"))
//        .map(_.map(InstanceName.tryCreate)),
//    )
//  )
//  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}

class ReassignmentServiceConcurrentReassignmentsIntegrationTestPostgres
    extends ReassignmentServiceConcurrentReassignmentsIntegrationTest {
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
