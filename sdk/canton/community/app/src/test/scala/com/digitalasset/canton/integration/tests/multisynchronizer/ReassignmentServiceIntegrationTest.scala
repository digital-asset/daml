// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.daml.ledger.api.v2 as proto
import com.daml.ledger.api.v2.completion.Completion
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.ReassignmentRef
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.GrpcAdminCommandSupport.*
import com.digitalasset.canton.integration.util.GrpcServices.ReassignmentsService
import com.digitalasset.canton.integration.util.HasCommandRunnersHelpers.{
  commandId as defaultCommandId,
  submissionId as defaultSubmissionId,
  userId as defaultUserId,
  workflowId as defaultWorkflowId,
}
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.participant.state.ReassignmentCommandsBatch.{
  DifferingSynchronizers,
  MixedAssignWithOtherCommands,
  NoCommands,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  TargetSynchronizerIsSourceSynchronizer,
  UnknownContract,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.google.rpc.status.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Tag

import scala.concurrent.duration.DurationInt
import scala.language.implicitConversions

abstract class ReassignmentServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  protected def plugin: EnvironmentSetupPlugin

  registerPlugin(plugin)
  registerPlugin(new UsePostgres(loggerFactory))

  // Workaround to avoid false errors reported by IDEA.
  implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  private val party1aName = "party1"
  private val party1bName = "party1b"
  private val party2Name = "party2"

  private var party1a: PartyId = _
  private var party1b: PartyId = _
  private var party2: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer1, alias = daName)
        participant3.synchronizers.connect_local(sequencer1, alias = daName)

        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant3.synchronizers.connect_local(sequencer2, alias = acmeName)

        participants.all.dars.upload(CantonExamplesPath)

        // Allocate parties
        party1a = participant1.parties.enable(
          party1aName,
          synchronizeParticipants = Seq(participant2),
          synchronizer = daName,
        )
        participant1.parties.enable(
          party1aName,
          synchronizeParticipants = Seq(participant2),
          synchronizer = acmeName,
        )

        party1b = participant1.parties.enable(
          party1bName,
          synchronizeParticipants = Seq(participant2),
          synchronizer = daName,
        )
        participant1.parties.enable(
          party1bName,
          synchronizeParticipants = Seq(participant2),
          synchronizer = acmeName,
        )

        party2 = participant2.parties.enable(
          party2Name,
          synchronizeParticipants = Seq(participant1),
          synchronizer = daName,
        )
        participant2.parties.enable(
          party2Name,
          synchronizeParticipants = Seq(participant1),
          synchronizer = acmeName,
        )

        synchronizerOwners2.foreach(
          _.topology.synchronizer_parameters.propose_update(
            synchronizerId = acmeId,
            _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
          )
        )
      }

  "ReassignmentService" should {
    /*
       Create a contract on da and reassign to acme
       Check that events and completions are published
     */
    "allow to unassign and assign" in { implicit env =>
      val signatory = party1a
      val observer = party2

      createAndReassignAContract(signatory, observer)(env)
    }

    "allow to submit unassignment and assignment as an observer" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      createAndReassignAContract(
        signatory,
        observer,
        submittingPartyOverride = Some(observer),
        submittingParticipantOverride = Some(participant2),
      )
    }

    "completion events are only emitted on the submitting participant" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val initialLedgerEnd1 = participant1.ledger_api.state.end()
      val initialLedgerEnd2 = participant2.ledger_api.state.end()

      // add the signatory party to participant2
      for {
        participant <- Seq(participant1, participant2)
        synchronizerId <- Seq(daId, acmeId)
      } {
        participant.topology.party_to_participant_mappings.propose_delta(
          party = signatory,
          adds = List((participant2.id -> ParticipantPermission.Submission)),
          store = synchronizerId,
        )
      }

      val signatoryOnParticipant2 = party1a

      val ledgerEndP1 = participant1.ledger_api.state.end()
      val ledgerEndP2 = participant2.ledger_api.state.end()

      createAndReassignAContract(signatory, observer)

      val finalLedgerEnd1 = participant1.ledger_api.state.end()
      val finalLedgerEnd2 = participant2.ledger_api.state.end()

      val updatesP1 = participant1.ledger_api.updates.flat(
        partyIds = Set(signatory),
        completeAfter = Int.MaxValue,
        beginOffsetExclusive = initialLedgerEnd1,
        endOffsetInclusive = Some(finalLedgerEnd1),
      )

      val updatesP2 = participant2.ledger_api.updates.flat(
        partyIds = Set(signatory),
        completeAfter = Int.MaxValue,
        beginOffsetExclusive = initialLedgerEnd2,
        endOffsetInclusive = Some(finalLedgerEnd2),
      )

      // reduce the ledger command timeout from 1 minute which is the default value
      // to avoid waiting 1 minute per call
      // and to avoid failing with GrpcClientGaveUp: DEADLINE_EXCEEDED
      participant2.consoleEnvironment.setLedgerCommandTimeout(3.seconds)
      participant1.consoleEnvironment.setLedgerCommandTimeout(3.seconds)

      // a submitting party on a non-submitting participant can't see the completion
      getCompletions(
        participant2,
        signatoryOnParticipant2.toLf,
        ledgerEndP2,
        filterBy = _.synchronizerTime.value.synchronizerId == daId.toProtoPrimitive,
      ) should be(empty)

      // but can see the updates
      updatesP2 should have size 4 // create, unassignment, assign, archive

      // a non submitting party (stakeholder) on the submitting participant can't see the completion
      getCompletions(
        participant1,
        observer,
        ledgerEndP1,
        filterBy = _.synchronizerTime.value.synchronizerId == daId.toProtoPrimitive,
      ) should be(empty)

      // a submitting party on a submitting participant should see the completion
      getCompletions(
        participant1,
        signatory,
        ledgerEndP1,
        filterBy = _.synchronizerTime.value.synchronizerId == daId.toProtoPrimitive,
      ) should not be empty

      // but and the updates
      updatesP1 should have size 4 // create, unassign, assign, archive
    }

    "updates are only visible to stakeholders and only them" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2
      val notAStakeholder = party1b

      val startOffsetP1 = participant1.ledger_api.state.end()
      val startOffsetP2 = participant2.ledger_api.state.end()
      createAndReassignAContract(signatory, observer)

      val endOffsetP1 =
        participant1.ledger_api.state.end()
      val endOffsetP2 =
        participant2.ledger_api.state.end()
      val updates = eventually() {

        participant1.ledger_api.updates.flat(
          partyIds = Set(notAStakeholder),
          completeAfter = Int.MaxValue,
          beginOffsetExclusive = startOffsetP1,
          endOffsetInclusive = Some(endOffsetP1),
          synchronizerFilter = Some(daId),
        )
      }
      updates shouldBe empty

      val updatesForObserver = eventually() {
        participant2.ledger_api.updates.flat(
          partyIds = Set(observer),
          completeAfter = Int.MaxValue,
          beginOffsetExclusive = startOffsetP2,
          endOffsetInclusive = Some(endOffsetP2),
        )
      }

      updatesForObserver should not be empty
    }

    "return an error when trying to submit assignment twice" taggedAs SecurityTest(
      SecurityTest.Property.Integrity,
      "virtual shared ledger",
      Attack(
        "a malicious participant",
        "submits the same assignment twice",
        "reject the second assignment",
      ),
    ) in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      // Create contract
      val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val cid = contract.id.toLf

      // unassign contract
      val (unassignedEvent, _) =
        unassign(cid = cid, source = daId, target = acmeId, signatory.toLf)

      // assign contract
      val assignmentCmd = getReassignmentCommand(
        getAssignmentCmd(
          source = daId,
          target = acmeId,
          unassignmentId = unassignedEvent.unassignId,
        ),
        submittingParty = signatory.toLf,
      )

      submitReassignment(assignmentCmd).tryResult.discard

      assertInLedgerAcsSync(
        participantRefs = List(participant1),
        partyId = signatory,
        synchronizerId = acmeId,
        cid = cid,
      )

      // Second assignment should fail
      inside(submitReassignment(assignmentCmd)) { case error: GenericCommandError =>
        error.cause should include("reassignment already completed")
      }

      // Cleaning
      IouSyntax.archive(participant1, Some(acmeId))(contract, signatory)
    }

    "return an error when wrong participant tries to initiate assignment" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2
      val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      // unassignment contract
      val (unassignedEvent, _) =
        unassign(
          cid = contract.id.toLf,
          source = daId,
          target = acmeId,
          submittingParty = signatory.toLf,
        )

      // Assign contract
      val reassignmentId = getReassignmentId(unassignedEvent)
      val assignmentCmd = getReassignmentCommand(
        getAssignmentCmd(
          source = daId,
          target = acmeId,
          unassignmentId = unassignedEvent.unassignId,
        ),
        submittingParty = signatory.toLf,
      )

      val res = participant3.runLapiAdminCommand( // Wrong participant
        ReassignmentsService.submit(
          proto.command_submission_service.SubmitReassignmentRequest(Some(assignmentCmd))
        )
      )

      inside(res) { case error: GenericCommandError =>
        error.cause should include(reassignmentId.toString)
        error.cause should include("unknown reassignment id")
      }

      res shouldBe a[GenericCommandError]
    }

    "fail when the contract is not found" in { implicit env =>
      import env.*

      val cid = LfContractId.assertFromString(
        "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
      )

      val unassignmentCmd = getReassignmentCommand(
        getUnassignmentCmd(cid = cid, source = acmeId, target = daId),
        submittingParty = participant1.id.adminParty.toLf,
      )

      inside(submitReassignment(unassignmentCmd)) { case error: GenericCommandError =>
        error.cause should include(UnknownContract(cid).message)
      }
    }

    "return an error if source and target synchronizers are equal" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val cid = contract.id.toLf

      // unassignment contract
      val unassignmentCmd =
        getReassignmentCommand(
          getUnassignmentCmd(cid = cid, source = daId, target = daId),
          submittingParty = signatory.toLf,
        )

      inside(submitReassignment(unassignmentCmd)) { case error: GenericCommandError =>
        error.cause should include(
          TargetSynchronizerIsSourceSynchronizer(daId, Seq(cid)).message
        )
      }

      // Cleaning
      IouSyntax.archive(participant1, Some(daId))(contract, signatory)
    }

    "fail when submitting party is not a stakeholder" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2
      val otherParty = party1b // not a signatory nor an observer

      // Create contract
      val contract = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val cid = contract.id.toLf

      // unassignment contract
      val unassignmentCmd = getReassignmentCommand(
        getUnassignmentCmd(cid = cid, source = daId, target = acmeId),
        submittingParty = otherParty.toLf, // wrong party (not a stakeholder)
      )

      inside(submitReassignment(unassignmentCmd)) { case error: GenericCommandError =>
        error.cause should include(
          ReassignmentValidationError
            .SubmitterMustBeStakeholder(
              ReassignmentRef(cid),
              submittingParty = otherParty.toLf,
              stakeholders = Set(signatory.toLf),
            )
            .message
        )
      }

      // Cleaning
      IouSyntax.archive(participant1)(contract, signatory)
    }

    "fail when no commands are provided" in { implicit env =>
      import env.*

      val signatory = party1a

      val noCommands = getReassignmentCommands(Seq.empty, submittingParty = signatory.toLf)

      inside(submitReassignment(noCommands)) { case error: GenericCommandError =>
        error.cause should include(NoCommands.error)
      }
    }

    "fail when assignment and unassignment commands are mixed" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val contract1 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val cid1 = contract1.id.toLf

      val (unassignedEvent, _) =
        unassign(cid = cid1, source = daId, target = acmeId, submittingParty = signatory.toLf)

      val contract2 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      // In the same request, assign contract 1 and unassign contract 2
      val mixedReassignmentCmds = getReassignmentCommands(
        Seq(
          getAssignmentCmd(
            source = daId,
            target = acmeId,
            unassignmentId = unassignedEvent.unassignId,
          ),
          getUnassignmentCmd(cid = contract2.id.toLf, source = daId, target = acmeId),
        ),
        submittingParty = signatory.toLf,
      )

      inside(submitReassignment(mixedReassignmentCmds)) { case error: GenericCommandError =>
        error.cause should include(MixedAssignWithOtherCommands.error)
      }

      // Cleaning
      assign(unassignedEvent.unassignId, daId, acmeId, signatory.toLf)
      IouSyntax.archive(participant1)(contract1, signatory)
      IouSyntax.archive(participant1)(contract2, signatory)
    }

    "fail when there are multiple assignment commands" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val contract1 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val contract2 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      val (unassignedEvent1, _) = unassign(
        cid = contract1.id.toLf,
        source = daId,
        target = acmeId,
        submittingParty = signatory.toLf,
      )
      val (unassignedEvent2, _) = unassign(
        cid = contract2.id.toLf,
        source = daId,
        target = acmeId,
        submittingParty = signatory.toLf,
      )

      // In the same request, assign contract 1 and contract 2
      val multipleAssignmentCmds = getReassignmentCommands(
        Seq(
          getAssignmentCmd(
            source = daId,
            target = acmeId,
            unassignmentId = unassignedEvent1.unassignId,
          ),
          getAssignmentCmd(
            source = daId,
            target = acmeId,
            unassignmentId = unassignedEvent2.unassignId,
          ),
        ),
        submittingParty = signatory.toLf,
      )

      inside(submitReassignment(multipleAssignmentCmds)) { case error: GenericCommandError =>
        error.cause should include(MixedAssignWithOtherCommands.error)
      }

      // Cleaning
      assign(unassignedEvent1.unassignId, daId, acmeId, signatory.toLf)
      assign(unassignedEvent2.unassignId, daId, acmeId, signatory.toLf)
      IouSyntax.archive(participant1)(contract1, signatory)
      IouSyntax.archive(participant1)(contract2, signatory)
    }

    "fail when unassignment commands have different targets" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val contract1 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val contract2 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)

      // In the same request, unassign contracts 1 and 2, but to different targets.
      val commandsWithDifferentTargets = getReassignmentCommands(
        Seq(
          getUnassignmentCmd(cid = contract1.id.toLf, source = daId, target = acmeId),
          getUnassignmentCmd(cid = contract2.id.toLf, source = daId, target = synchronizer3Id),
        ),
        submittingParty = signatory.toLf,
      )

      inside(submitReassignment(commandsWithDifferentTargets)) { case error: GenericCommandError =>
        error.cause should include(DifferingSynchronizers.error)
      }

      // Cleaning
      IouSyntax.archive(participant1)(contract1, signatory)
      IouSyntax.archive(participant1)(contract2, signatory)
    }

    "fail when unassignment commands have different sources" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val contract1 = IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
      val contract2 = IouSyntax.createIou(participant1, Some(acmeId))(signatory, observer)

      // In the same request, unassign contracts 1 and 2, but to different targets.
      val commandsWithDifferentTargets = getReassignmentCommands(
        Seq(
          getUnassignmentCmd(cid = contract1.id.toLf, source = daId, target = synchronizer3Id),
          getUnassignmentCmd(cid = contract2.id.toLf, source = acmeId, target = synchronizer3Id),
        ),
        submittingParty = signatory.toLf,
      )

      inside(submitReassignment(commandsWithDifferentTargets)) { case error: GenericCommandError =>
        error.cause should include(DifferingSynchronizers.error)
      }

      // Cleaning
      IouSyntax.archive(participant1)(contract1, signatory)
      IouSyntax.archive(participant1)(contract2, signatory)
    }
  }

  private def createAndReassignAContract(
      signatory: PartyId,
      observer: PartyId,
      submittingPartyOverride: Option[PartyId] = None,
      submittingParticipantOverride: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val submittingParty = submittingPartyOverride.getOrElse(signatory)
    val ledgerEndSubmitterUnassignment =
      submittingParticipantOverride.getOrElse(participant1).ledger_api.state.end()

    // Create contract
    val (contract, createUpdateEvent, createCompletion) =
      IouSyntax.createIouComplete(participant1, Some(daId))(signatory, observer)
    createCompletion.status.value shouldBe Status()

    val createdEvent = createUpdateEvent.eventsById.headOption
      .map { case (_, event) => event }
      .value
      .getCreated

    /*
      During the unassignment below, we wait for the update to be published and expect a unassigned event.
      If the unassignment is submitted before the created event (from the create above) is published, then we will
      see the created event instead of the unassigned and the assertion will fail.
      The solution here is to wait for the created event to be published before proceeding further.
     */
    submittingParticipantOverride.foreach { submittingParticipantOverride =>
      eventually() {
        val endOffset = submittingParticipantOverride.ledger_api.state.end()

        val updates = submittingParticipantOverride.ledger_api.updates.flat(
          partyIds = Set(submittingParty.toLf),
          completeAfter = Int.MaxValue,
          beginOffsetExclusive = ledgerEndSubmitterUnassignment,
          endOffsetInclusive = Some(endOffset),
          synchronizerFilter = Some(daId),
        )

        updates.size shouldBe 1
      }
    }

    val cid = contract.id.toLf

    // unassignment contract
    val (unassignedEvent, unassignmentCompletion) = unassign(
      cid = cid,
      source = daId,
      target = acmeId,
      submittingParty = submittingParty.toLf,
      participantOverride = submittingParticipantOverride,
    )

    val templateId = Iou.TEMPLATE_ID_WITH_PACKAGE_ID
    val expectedTemplateId = Some(
      com.daml.ledger.api.v2.value.Identifier(
        packageId = templateId.getPackageId,
        moduleName = templateId.getModuleName,
        entityName = templateId.getEntityName,
      )
    )

    val expectedUnassignedEvent = proto.reassignment.UnassignedEvent(
      offset = 0L,
      unassignId = unassignedEvent.unassignId, // We don't know this value
      contractId = cid.coid,
      templateId = expectedTemplateId,
      source = daId.toProtoPrimitive,
      target = acmeId.toProtoPrimitive,
      submitter = submittingParty.toLf,
      reassignmentCounter = 1,
      assignmentExclusivity =
        unassignedEvent.events.loneElement.assignmentExclusivity, // We don't know this value
      witnessParties = Seq(submittingParty.toProtoPrimitive),
      packageName = "CantonExamples",
      nodeId = 0,
    )

    comparableUnassignedEvent(
      unassignedEvent.events.loneElement
    ) shouldBe expectedUnassignedEvent
    unassignedEvent.reassignment.workflowId shouldBe defaultWorkflowId
    unassignedEvent.reassignment.commandId shouldBe defaultCommandId

    val ledgerEndAfterUnassignment =
      submittingParticipantOverride.getOrElse(participant1).ledger_api.state.end()

    val expectedUnassignmentCompletion = Completion(
      commandId = defaultCommandId,
      status = Some(Status()),
      updateId = unassignedEvent.reassignment.updateId,
      userId = defaultUserId,
      actAs = Seq(submittingParty.toLf),
      submissionId = defaultSubmissionId.getOrElse(""),
      deduplicationPeriod = Completion.DeduplicationPeriod.Empty,
      traceContext = unassignmentCompletion.traceContext,
      offset = 0L,
      synchronizerTime = None,
    )

    unassignmentCompletion.copy(
      synchronizerTime = None,
      offset = 0L,
    ) shouldBe expectedUnassignmentCompletion
    unassignmentCompletion.offset should be <= ledgerEndAfterUnassignment

    // Check unassignment
    assertNotInLedgerAcsSync(
      participantRefs = List(participant1),
      partyId = signatory,
      synchronizerId = daId,
      cid = cid,
    )

    val (assignedEvent, assignmentCompletion) = assign(
      unassignId = unassignedEvent.unassignId,
      source = daId,
      target = acmeId,
      submittingParty = submittingParty.toLf,
      participantOverride = submittingParticipantOverride,
    )

    val ledgerEndAfterAssignment =
      submittingParticipantOverride.getOrElse(participant1).ledger_api.state.end()

    val expectedCreatedEvent =
      createdEvent
        .focus(_.witnessParties)
        .replace(Seq(submittingParty.toLf))
        .focus(_.nodeId)
        .replace(0)

    val expectedAssignedEvent = proto.reassignment.AssignedEvent(
      source = daId.toProtoPrimitive,
      target = acmeId.toProtoPrimitive,
      unassignId = expectedUnassignedEvent.unassignId,
      submitter = submittingParty.toLf,
      reassignmentCounter = 1,
      createdEvent = Some(expectedCreatedEvent),
    )

    comparableAssignedEvent(assignedEvent.events.loneElement) shouldBe
      comparableAssignedEvent(expectedAssignedEvent)

    assignedEvent.reassignment.workflowId shouldBe defaultWorkflowId
    unassignedEvent.reassignment.commandId shouldBe defaultCommandId

    val expectedAssignmentCompletion = Completion(
      commandId = defaultCommandId,
      status = Some(Status()),
      updateId = assignedEvent.reassignment.updateId,
      userId = defaultUserId,
      actAs = Seq(submittingParty.toLf),
      submissionId = defaultSubmissionId.getOrElse(""),
      deduplicationPeriod = Completion.DeduplicationPeriod.Empty,
      traceContext = assignmentCompletion.traceContext,
      offset = 0L,
      synchronizerTime = None,
    )

    assignmentCompletion.copy(
      synchronizerTime = None,
      offset = 0L,
    ) shouldBe expectedAssignmentCompletion
    assignmentCompletion.offset should be <= ledgerEndAfterAssignment

    assertInAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = acmeName,
      contractId = cid,
    )

    // Cleaning
    IouSyntax.archive(participant1, Some(acmeId))(contract, signatory)
  }
}

class ReferenceReassignmentServiceIntegrationTest extends ReassignmentServiceIntegrationTest {
  override protected lazy val plugin =
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
          Set(InstanceName.tryCreate("sequencer3")),
        )
      ),
    )
}
