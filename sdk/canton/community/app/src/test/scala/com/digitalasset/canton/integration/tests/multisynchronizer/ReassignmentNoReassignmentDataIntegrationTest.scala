// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.examples.java.iou.GetCash
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, PartyToParticipantDeclarative}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  ReassignmentEntry,
  UnknownReassignmentId,
}
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponses,
  LocalVerdict,
  SignedProtocolMessage,
  TypedSignedProtocolMessageContent,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.{BaseTest, config}

import java.util.UUID
import scala.collection.concurrent.TrieMap
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
    with HasProgrammableSequencer {

  private val acmeConfirmationResponses = new TrieMap[ParticipantId, SubmissionRequest]()

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

        alice = participant1.parties.enable("alice", synchronizer = daName)
        participant1.parties.enable("alice", synchronizer = acmeName)

        bob = participant2.parties.enable("bob", synchronizer = daName)
        participant2.parties.enable("bob", synchronizer = acmeName)

      }

  private var alice: PartyId = _
  private var bob: PartyId = _

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
    *   - returns the completion of the assignment
    */
  private def runScenario(cid: LfContractId, deleteReassignmentEntry: Boolean)(implicit
      env: TestConsoleEnvironment
  ): Completion = {
    import env.*

    // clear acmeConfirmationResponses for each run
    acmeConfirmationResponses.clear()

    getProgrammableSequencer(sequencer2.name).setPolicy_("confirmations count")(
      interceptConfirmationResponsesPolicy(acmeConfirmationResponses)
    )

    val reassignmentId =
      participant2.ledger_api.commands
        .submit_unassign(
          bob,
          Seq(cid),
          daId,
          acmeId,
        )
        .reassignmentId

    val reassignmentStore = participant1.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore

    val reassignmendId = ReassignmentId.tryCreate(reassignmentId)

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

    val ledgerEndBefore = participant2.ledger_api.state.end()
    val commandId = UUID.randomUUID().toString

    participant2.ledger_api.commands.submit_assign_async(
      bob,
      reassignmentId,
      daId,
      acmeId,
      commandId = commandId,
    )
    val completion = participant2.ledger_api.completions
      .list(
        partyId = bob,
        atLeastNumCompletions = 1,
        beginOffsetExclusive = ledgerEndBefore,
        filter = _.commandId == commandId,
      )
      .loneElement

    completion
  }

  "signatory assigning participants" should {
    "send an abstain verdict for assignments if data in the reassignment store" in { implicit env =>
      import env.*

      clue("success: threshold is one (p3 confirmation is sufficient for Alice)") {
        changeAliceHosting(confirmationThresholdAcme = PositiveInt.one)
        val result = runScenario(createGetCash(1.0), deleteReassignmentEntry = true)
        // all participants should send a confirmation response
        Seq(participant1, participant2, participant3).foreach(
          acmeConfirmationResponses.get(_) should not be empty
        )

        localVerdictIs(acmeConfirmationResponses(participant1), _.isAbstain) shouldBe true

        result.status.value.code shouldBe 0
      }

      clue("failure: threshold is two and p1 does not confirm (entry deleted)") {
        changeAliceHosting(confirmationThresholdAcme = PositiveInt.two)
        val result = runScenario(createGetCash(2.0), deleteReassignmentEntry = true)

        val status = result.status.value

        Seq(participant1, participant2, participant3).foreach(
          acmeConfirmationResponses.get(_) should not be empty
        )
        status.code should not be 0
        localVerdictIs(acmeConfirmationResponses(participant1), _.isAbstain) shouldBe true
        status.message should include(
          s"Cannot perform all validations: Unassignment data not found when processing assignment"
        )
      }
    }

    "send an approve verdict for assignments if data in the reassignment store" in { implicit env =>
      import env.*

      def assertResultIsOK(result: Completion) = {
        result.status.value.code shouldBe 0
        localVerdictIs(acmeConfirmationResponses(participant1), _.isApprove) shouldBe true
        Seq(participant1, participant2, participant3).foreach(
          acmeConfirmationResponses.get(_) should not be empty
        )
      }

      clue("success: entry is not deleted") {
        changeAliceHosting(confirmationThresholdAcme = PositiveInt.one)
        val result = runScenario(createGetCash(4.0), deleteReassignmentEntry = false)
        assertResultIsOK(result)
      }

      clue("success: entry is not deleted") {
        changeAliceHosting(confirmationThresholdAcme = PositiveInt.two)
        val result = runScenario(createGetCash(5.0), deleteReassignmentEntry = false)
        assertResultIsOK(result)
      }
    }
  }

  private def interceptConfirmationResponsesPolicy(
      confirmations: TrieMap[ParticipantId, SubmissionRequest]
  ): SendPolicyWithoutTraceContext = submissionRequest =>
    submissionRequest.sender match {
      case pid: ParticipantId
          if ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest) =>
        confirmations.put(pid, submissionRequest)

        SendDecision.Process

      case _ => SendDecision.Process
    }

  private def localVerdictIs(
      submissionRequest: SubmissionRequest,
      condition: LocalVerdict => Boolean,
  )(implicit env: TestConsoleEnvironment): Boolean = {
    import env.*
    submissionRequest.batch.envelopes.exists(
      _.closeEnvelope
        .openEnvelope(participant1.crypto.pureCrypto, testedProtocolVersion)
        .value
        .protocolMessage match {
        case SignedProtocolMessage(
              TypedSignedProtocolMessageContent(confirmations: ConfirmationResponses),
              _,
            ) =>
          confirmations.responses.forall(r => condition(r.localVerdict))
        case _ => false
      }
    )
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
