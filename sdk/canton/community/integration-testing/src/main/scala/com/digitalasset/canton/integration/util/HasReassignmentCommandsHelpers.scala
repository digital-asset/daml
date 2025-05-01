// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.api.v2 as proto
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.console.{ConsoleCommandResult, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.util.GrpcAdminCommandSupport.ParticipantReferenceOps
import com.digitalasset.canton.integration.util.GrpcServices.ReassignmentsService
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.{
  BaseTest,
  LedgerCommandId,
  LedgerSubmissionId,
  LedgerUserId,
  LfPartyId,
  LfWorkflowId,
}

trait HasReassignmentCommandsHelpers {
  this: BaseTest & HasCommandRunnersHelpers =>

  import HasCommandRunnersHelpers.*

  protected def unassign(
      cid: LfContractId,
      source: SynchronizerId,
      target: SynchronizerId,
      submittingParty: LfPartyId,
      participantOverride: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): (UpdateService.UnassignedWrapper, Completion) = {
    import env.*

    val participant = participantOverride.getOrElse(participant1)
    val ledgerEnd = participant.ledger_api.state.end()

    logger.debug(
      s"Submitting unassignment of $cid on behalf of $submittingParty (source=$source, target=$target)"
    )
    participant.ledger_api.commands.submit_unassign_async(
      submitter = submittingParty,
      contractIds = Seq(cid),
      source = source,
      target = target,
      commandId = commandId,
      userId = userId,
      workflowId = workflowId,
      submissionId = LedgerSubmissionId.assertFromString("some-submission-id"),
    )

    logger.debug(s"Listening for completion and update starting at $ledgerEnd")
    val completions = participant.ledger_api.completions.list(
      partyId = submittingParty,
      beginOffsetExclusive = ledgerEnd,
      atLeastNumCompletions = 1,
      userId = userId,
    )

    val updates = participant.ledger_api.updates.flat(
      partyIds = Set(submittingParty),
      completeAfter = 1,
      beginOffsetExclusive = ledgerEnd,
    )

    val unassignmentCompletion = completions.headOption.value

    updates.headOption.value match {
      case w: UpdateService.UnassignedWrapper => (w, unassignmentCompletion)
      case other => throw new RuntimeException(s"Expected a reassignment event but got $other")
    }
  }

  protected def failingUnassignment(
      cid: LfContractId,
      source: SynchronizerId,
      target: SynchronizerId,
      submittingParty: PartyId,
      participantOverrideO: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Completion = {
    val unassignmentCmd = getReassignmentCommand(
      cmd = getUnassignmentCmd(cid = cid, source = source, target = target),
      userId = userId,
      workflowId = Some(workflowId),
      submissionId = submissionId,
      commandId = commandId,
      submittingParty = submittingParty,
    )

    val unassignmentCompletion =
      runFailingCommand(
        submitReassignment(unassignmentCmd, participantOverrideO),
        source,
        userId,
        submittingParty,
        participantOverrideO,
      )

    unassignmentCompletion
  }

  protected def assign(
      unassignId: String,
      source: SynchronizerId,
      target: SynchronizerId,
      submittingParty: LfPartyId,
      participantOverride: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): (UpdateService.AssignedWrapper, Completion) = {
    import env.*

    val participant = participantOverride.getOrElse(participant1)
    val ledgerEnd = participant.ledger_api.state.end()

    logger.debug(
      s"Submitting assignment of $unassignId on behalf of $submittingParty (source=$source, target=$target)"
    )
    participant.ledger_api.commands.submit_assign_async(
      submitter = submittingParty,
      unassignId = unassignId,
      source = source,
      target = target,
      commandId = commandId,
      userId = userId,
      workflowId = workflowId,
      submissionId = LedgerSubmissionId.assertFromString("some-submission-id"),
    )

    logger.debug(s"Listening for completion and update starting at $ledgerEnd")
    val completions = participant.ledger_api.completions.list(
      partyId = submittingParty,
      beginOffsetExclusive = ledgerEnd,
      atLeastNumCompletions = 1,
      userId = userId,
    )

    val updates = participant.ledger_api.updates.flat(
      partyIds = Set(submittingParty),
      completeAfter = 1,
      beginOffsetExclusive = ledgerEnd,
    )

    val assignmentCompletion = completions.headOption.value
    updates.headOption.value match {
      case w: UpdateService.AssignedWrapper => (w, assignmentCompletion)
      case other =>
        throw new RuntimeException(s"Expected an assignment event but got $other")
    }
  }

  protected def failingAssignment(
      unassignId: String,
      source: SynchronizerId,
      target: SynchronizerId,
      submittingParty: PartyId,
      participantOverrideO: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Completion = {
    val assignmentCmd = getReassignmentCommand(
      cmd = getAssignmentCmd(
        source = source,
        target = target,
        unassignmentId = unassignId,
      ),
      userId = userId,
      workflowId = Some(workflowId),
      submissionId = submissionId,
      commandId = commandId,
      submittingParty = submittingParty,
    )

    val assignmentCompletion =
      runFailingCommand(
        submitReassignment(assignmentCmd, participantOverrideO),
        target,
        userId,
        submittingParty,
        participantOverrideO,
      )

    assignmentCompletion
  }

  protected def submitReassignment(
      cmd: proto.reassignment_commands.ReassignmentCommands,
      participantOverride: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): ConsoleCommandResult[proto.command_submission_service.SubmitReassignmentResponse] =
    participantOverride
      .getOrElse(env.participant1)
      .runLapiAdminCommand(
        ReassignmentsService.submit(
          proto.command_submission_service.SubmitReassignmentRequest(Some(cmd))
        )
      )

  protected def getUnassignmentCmd(
      cid: LfContractId,
      source: SynchronizerId,
      target: SynchronizerId,
  ): proto.reassignment_commands.ReassignmentCommand.Command.UnassignCommand =
    proto.reassignment_commands.ReassignmentCommand.Command.UnassignCommand(
      proto.reassignment_commands.UnassignCommand(
        contractId = cid.coid,
        source = source.toProtoPrimitive,
        target = target.toProtoPrimitive,
      )
    )

  protected def getAssignmentCmd(
      source: SynchronizerId,
      target: SynchronizerId,
      unassignmentId: String,
  ): proto.reassignment_commands.ReassignmentCommand.Command.AssignCommand =
    proto.reassignment_commands.ReassignmentCommand.Command.AssignCommand(
      proto.reassignment_commands.AssignCommand(
        unassignId = unassignmentId,
        source = source.toProtoPrimitive,
        target = target.toProtoPrimitive,
      )
    )

  protected def getReassignmentCommands(
      cmds: Seq[proto.reassignment_commands.ReassignmentCommand.Command],
      userId: LedgerUserId = HasCommandRunnersHelpers.userId,
      submissionId: Option[LedgerSubmissionId] = HasCommandRunnersHelpers.submissionId,
      commandId: LedgerCommandId = HasCommandRunnersHelpers.commandId,
      workflowId: Option[LfWorkflowId] = None,
      submittingParty: PartyId,
  ): proto.reassignment_commands.ReassignmentCommands =
    proto.reassignment_commands.ReassignmentCommands(
      workflowId = workflowId.getOrElse(""), // this field won't affect the reassignment command
      userId = userId,
      commandId = commandId,
      submitter = submittingParty.toLf,
      commands = cmds.map(proto.reassignment_commands.ReassignmentCommand(_)),
      submissionId = submissionId.getOrElse(""),
    )

  protected def getReassignmentCommand(
      cmd: proto.reassignment_commands.ReassignmentCommand.Command,
      userId: LedgerUserId = HasCommandRunnersHelpers.userId,
      submissionId: Option[LedgerSubmissionId] = HasCommandRunnersHelpers.submissionId,
      commandId: LedgerCommandId = HasCommandRunnersHelpers.commandId,
      workflowId: Option[LfWorkflowId] = None,
      submittingParty: PartyId,
  ): proto.reassignment_commands.ReassignmentCommands = getReassignmentCommands(
    Seq(cmd),
    userId,
    submissionId,
    commandId,
    workflowId,
    submittingParty,
  )

  protected def getReassignmentId(out: UpdateService.UnassignedWrapper): ReassignmentId =
    ReassignmentId(
      sourceSynchronizer = Source(SynchronizerId.tryFromString(out.source)),
      unassignmentTs = CantonTimestamp.assertFromLong(out.unassignId.toLong),
    )
}
