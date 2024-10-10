// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.either.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.command_submission_service.{SubmitReassignmentRequest, SubmitRequest}
import com.daml.ledger.api.v2.interactive_submission_service.{
  ExecuteSubmissionRequest,
  PrepareSubmissionRequest,
}
import com.daml.ledger.api.v2.reassignment_command.ReassignmentCommand
import com.digitalasset.canton.crypto.LedgerApiCryptoConversions.*
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.messages.command.submission
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.protocol.TransactionAuthorizationPartySignatures as ProtocolSignatures
import com.digitalasset.canton.protocol.v30.TransactionAuthorizationPartySignatures as ProtoSignatures
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.Time
import io.grpc.StatusRuntimeException
import io.scalaland.chimney.dsl.*
import scalaz.syntax.tag.*

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
      maxDeduplicationDuration: Duration,
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

  def validatePrepare(
      req: PrepareSubmissionRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
      domainIdString: Option[String],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      validatedCommands <- commandsValidator.validatePrepareRequest(
        req,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
      domainId <- validateOptional(domainIdString)(
        requireDomainId(_, "domain_id")
      )
    } yield submission.SubmitRequest(validatedCommands.copy(domainId = domainId))

  def validateExecute(
      req: ExecuteSubmissionRequest,
      submissionIdGenerator: SubmissionIdGenerator,
      maxDeduplicationDuration: Duration,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ExecuteRequest] =
    for {
      submissionId <- validateSubmissionId(req.submissionId)
        .map(_.map(_.unwrap))
        .map(
          _.getOrElse(submissionIdGenerator.generate())
        )
      workflowId <- validateWorkflowId(req.workflowId).map(_.map(_.unwrap))
      deduplicationPeriod <- commandsValidator.validateExecuteDeduplicationPeriod(
        req.deduplicationPeriod,
        maxDeduplicationDuration,
      )
      partySignaturesNonEmpty <- req.partiesSignatures
        .toRight(RequestValidationErrors.MissingField.Reject("parties_signatures").asGrpcError)
      partySignatures <-
        ProtocolSignatures
          .fromProtoV30(
            // Convert the LAPI proto to the Canton protocol proto
            // before parsing it to the internal data type CantonPartySignatures
            partySignaturesNonEmpty.transformInto[ProtoSignatures]
          )
          .leftMap(err =>
            RequestValidationErrors.InvalidArgument
              .Reject(s"Invalid signature argument: $err")
              .asGrpcError
          )
      preparedTransaction <- req.preparedTransaction.toRight(
        RequestValidationErrors.MissingField
          .Reject("prepared_transaction")
          .asGrpcError
      )
    } yield {
      ExecuteRequest(
        submissionId,
        workflowId,
        deduplicationPeriod,
        partySignatures,
        preparedTransaction,
      )
    }

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
              sourceDomainId = Source(sourceDomainId),
              targetDomainId = Target(targetDomainId),
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
              sourceDomainId = Source(sourceDomainId),
              targetDomainId = Target(targetDomainId),
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
