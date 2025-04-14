// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.GetCash
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
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  ReassignmentEntry,
  UnknownReassignmentId,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.{BaseTest, config}
import org.scalatest.Assertion

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*

/** This test ensures that signatory assigning participant don't send confirmation responses if the
  * data in the reassignment store is missing. We test that by:
  *   - Deleting the entry after the unassignment
  *   - Counting the sequenced confirmation responses
  *   - Checking that assignment times out
  */
sealed trait ReassignmentNoReassignmentDataIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
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

        alice = participant1.parties.enable("alice")
        bob = participant2.parties.enable("bob")

        participantReactionTimeout = sequencer2.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(acmeId)
          .confirmationResponseTimeout
          .toInternal
          .duration

        programmableSequencer = getProgrammableSequencer(sequencer2.name)
      }

  private var alice: PartyId = _
  private var bob: PartyId = _
  private var participantReactionTimeout: Duration = _
  private val totalConfirmationsResponsesAcme: TrieMap[ParticipantId, Int] = new TrieMap()
  private var programmableSequencer: ProgrammableSequencer = _

  private lazy val reassignmentFailureLogAssertions
      : Seq[(LogEntryOptionality, LogEntry => Assertion)] =
    Seq(
      (
        LogEntryOptionality.OptionalMany,
        _.warningMessage should include regex "Response message for request .* timed out",
      )
    )

  // Create a GetCash contract, which has alice and bob as signatories
  private def createGetCash(
      amount: Double
  )(implicit env: TestConsoleEnvironment): LfContractId = {
    import env.*

    val iou = IouSyntax.createIou(participant1, Some(daId))(alice, bob, amount)
    participant2.ledger_api.javaapi.commands
      .submit(Seq(bob), iou.id.exerciseCall().commands().asScala.toSeq)

    val res = participant2.ledger_api.javaapi.state.acs
      .filter(GetCash.COMPANION)(bob, _.data.amount.value.toString.toDouble == amount)

    LfContractId.assertFromString(res.head.id.contractId)
  }

  /** Target topology:
    *   - da: Alice hosted on P1 and P3 with submission rights, threshold=1
    *   - acme: Alice hosted on P1 and P3 with submission rights,
    *     threshold=`confirmationThresholdAcme`
    */
  private def changeAliceHosting(
      confirmationThresholdAcme: PositiveInt
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    def targetTopology(
        threshold: PositiveInt
    ): (PositiveInt, Set[(ParticipantId, ParticipantPermission)]) =
      (
        threshold,
        Set(
          (participant1.id, ParticipantPermission.Submission),
          (participant3.id, ParticipantPermission.Submission),
        ),
      )

    PartyToParticipantDeclarative.apply(
      Set(participant1, participant2, participant3),
      Set(daId, acmeId),
    )(
      Map(alice -> participant1),
      Map(
        alice -> Map(
          daId -> targetTopology(PositiveInt.one),
          acmeId -> targetTopology(confirmationThresholdAcme),
        )
      ),
    )
  }

  /** Runs the following steps:
    *   - unassign contract `cid`
    *   - if `deletedReassignmentEntry` is true, delete the reassignment entry in P1 store
    *   - submits the assignment
    *   - returns once the expected number of confirmations have been sequenced
    */
  // TODO(#24532) Test that p1 sends an abstain
  private def runScenario(cid: LfContractId, deleteReassignmentEntry: Boolean)(implicit
      env: TestConsoleEnvironment
  ): Future[Completion] = {
    import env.*

    val expectedConfirmationResponses =
      if (deleteReassignmentEntry)
        2 // p2, p3
      else
        3 // p1, p2, p3

    val unassignId =
      participant2.ledger_api.commands
        .submit_unassign(
          bob,
          Seq(cid),
          daId,
          acmeId,
          waitForParticipants = Map(participant1 -> alice),
        )
        .unassignId

    val reassignmentStore = participant1.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore

    val reassignmendId =
      ReassignmentId(Source(daId), CantonTimestamp.assertFromLong(unassignId.toLong))

    reassignmentStore
      .findReassignmentEntry(reassignmendId)
      .futureValueUS
      .value shouldBe a[ReassignmentEntry]

    if (deleteReassignmentEntry) {
      reassignmentStore.deleteReassignment(reassignmendId).futureValueUS

      reassignmentStore
        .findReassignmentEntry(reassignmendId)
        .futureValueUS
        .left
        .value shouldBe a[UnknownReassignmentId]
    }

    val confirmationResponsesCount = new AtomicLong(0)

    val allExpectedConfirmationResponsesReceived = Promise[Unit]()

    programmableSequencer.setPolicy("count confirmation responses") {
      SendPolicy.processTimeProofs { _ => submissionRequest =>
        val isConfirmationResponse = ProgrammableSequencerPolicies
          .isConfirmationResponse(submissionRequest)

        if (isConfirmationResponse) {
          val participantId =
            ParticipantId.tryFromProtoPrimitive(submissionRequest.sender.toProtoPrimitive)
          val newTotalConfirmationsResponses =
            totalConfirmationsResponsesAcme.getOrElse(participantId, 0) + 1
          totalConfirmationsResponsesAcme.put(participantId, newTotalConfirmationsResponses)

          if (confirmationResponsesCount.incrementAndGet() >= expectedConfirmationResponses)
            allExpectedConfirmationResponsesReceived.success(())
        }

        SendDecision.Process
      }
    }

    val assignmentCompletion1F = Future {
      failingAssignment(
        unassignId = unassignId,
        source = daId,
        target = acmeId,
        submittingParty = bob.toLf,
        participantOverrideO = Some(participant2),
      )
    }

    allExpectedConfirmationResponsesReceived.future.futureValue
    assignmentCompletion1F
  }

  "signatory assigning participants without data in the reassignment store" should {
    "not confirm assignments" in { implicit env =>
      import env.*

      loggerFactory.assertLogsUnorderedOptional(
        {

          // success: threshold is one (p3 confirmation is sufficient for Alice)
          {
            changeAliceHosting(confirmationThresholdAcme = PositiveInt.one)
            val result = runScenario(createGetCash(1.0), deleteReassignmentEntry = true)
            result.futureValue.status.value.code shouldBe 0
          }

          // failure: threshold is two and p1 does not confirm (entry deleted)
          {
            changeAliceHosting(confirmationThresholdAcme = PositiveInt.two)
            val result = runScenario(createGetCash(2.0), deleteReassignmentEntry = true)
            environment.simClock.value.advance(participantReactionTimeout.plusMillis(1))
            mediator2.testing.fetch_synchronizer_time()

            val status = result.futureValue.status.value

            status.code should not be 0
            status.message should include(
              "Rejected transaction as the mediator did not receive sufficient confirmations within the expected timeframe."
            )
          }

          // success: entry is not deleted
          {
            changeAliceHosting(confirmationThresholdAcme = PositiveInt.one)
            val result = runScenario(createGetCash(3.0), deleteReassignmentEntry = false)
            result.futureValue.status.value.code shouldBe 0
          }

          // success: entry is not deleted
          {
            changeAliceHosting(confirmationThresholdAcme = PositiveInt.two)
            val result = runScenario(createGetCash(4.0), deleteReassignmentEntry = false)
            result.futureValue.status.value.code shouldBe 0
          }

          // p1 only confirms two assignments (when entry is not deleted)
          totalConfirmationsResponsesAcme(participant1.id) shouldBe 2

          // p2, p4 confirms the four assignments
          totalConfirmationsResponsesAcme(participant2.id) shouldBe 4
          totalConfirmationsResponsesAcme(participant3.id) shouldBe 4
        },
        reassignmentFailureLogAssertions*
      )
    }
  }
}

class ReassignmentNoReassignmentDataIntegrationTestPostgres
    extends ReassignmentNoReassignmentDataIntegrationTest {
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
