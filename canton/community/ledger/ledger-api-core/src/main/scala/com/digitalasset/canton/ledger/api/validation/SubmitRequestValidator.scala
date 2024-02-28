// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.command_submission_service.{SubmitReassignmentRequest, SubmitRequest}
import com.daml.ledger.api.v2.reassignment_command.ReassignmentCommand
import com.daml.lf.data.Time
import com.digitalasset.canton.ledger.api.messages.command.submission
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}
import io.grpc.StatusRuntimeException

import java.time.{Duration, Instant}
import scala.util.Try

class SubmitRequestValidator(
    commandsValidator: CommandsValidator
) {
  import FieldValidator.*
  def validate(
      req: SubmitRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Option[Duration],
      domainIdString: Option[String],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
      domainId <- validateOptional(domainIdString)(
        requireDomainId(_, "domain_id")
      )
    } yield submission.SubmitRequest(validatedCommands.copy(domainId = domainId))

  def validateReassignment(
      req: SubmitReassignmentRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, submission.SubmitReassignmentRequest] =
    for {
      reassignmentCommand <- requirePresence(req.reassignmentCommand, "reassignment_command")
      submitter <- requirePartyField(reassignmentCommand.submitter, "submitter")
      applicationId <- requireApplicationId(reassignmentCommand.applicationId, "application_id")
      commandId <- requireCommandId(reassignmentCommand.commandId, "command_id")
      submissionId <- requireSubmissionId(reassignmentCommand.submissionId, "submission_id")
      workflowId <- validateOptional(Some(reassignmentCommand.workflowId).filter(_.nonEmpty))(
        requireWorkflowId(_, "workflow_id")
      )
      reassignmentCommand <- reassignmentCommand.command match {
        case ReassignmentCommand.Command.Empty =>
          Left(ValidationErrors.missingField("command"))
        case assignCommand: ReassignmentCommand.Command.AssignCommand =>
          for {
            sourceDomainId <- requireDomainId(assignCommand.value.source, "source")
            targetDomainId <- requireDomainId(assignCommand.value.target, "target")
            longUnassignId <- Try(assignCommand.value.unassignId.toLong).toEither.left.map(_ =>
              ValidationErrors.invalidField("unassign_id", "Invalid unassign ID")
            )
            timestampUnassignId <- Time.Timestamp
              .fromLong(longUnassignId)
              .left
              .map(_ => ValidationErrors.invalidField("unassign_id", "Invalid unassign ID"))
          } yield Left(
            submission.AssignCommand(
              sourceDomainId = SourceDomainId(sourceDomainId),
              targetDomainId = TargetDomainId(targetDomainId),
              unassignId = timestampUnassignId,
            )
          )
        case unassignCommand: ReassignmentCommand.Command.UnassignCommand =>
          for {
            sourceDomainId <- requireDomainId(unassignCommand.value.source, "source")
            targetDomainId <- requireDomainId(unassignCommand.value.target, "target")
            cid <- requireContractId(unassignCommand.value.contractId, "contract_id")
          } yield Right(
            submission.UnassignCommand(
              sourceDomainId = SourceDomainId(sourceDomainId),
              targetDomainId = TargetDomainId(targetDomainId),
              contractId = cid,
            )
          )
      }
    } yield submission.SubmitReassignmentRequest(
      submitter = submitter,
      applicationId = applicationId,
      commandId = commandId,
      submissionId = submissionId,
      workflowId = workflowId,
      reassignmentCommand = reassignmentCommand,
    )
}
