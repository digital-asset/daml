// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.traverse.*
import com.daml.ledger.api.v2.commands.Command.Command.{
  Create as ProtoCreate,
  CreateAndExercise as ProtoCreateAndExercise,
  Empty as ProtoEmpty,
  Exercise as ProtoExercise,
  ExerciseByKey as ProtoExerciseByKey,
}
import com.daml.ledger.api.v2.commands.{Command, Commands as ProtoCommands, PrefetchContractKey}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionRequest,
  PrepareSubmissionRequest,
}
import com.daml.ledger.api.v2.reassignment_commands.{ReassignmentCommand, ReassignmentCommands}
import com.digitalasset.canton.data.{DeduplicationPeriod, Offset}
import com.digitalasset.canton.ledger.api.messages.command.submission
import com.digitalasset.canton.ledger.api.util.{DurationConversion, TimestampConversion}
import com.digitalasset.canton.ledger.api.validation.CommandsValidator.{
  Submitters,
  effectiveSubmitters,
}
import com.digitalasset.canton.ledger.api.{CommandId, Commands}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.command.*
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.value.Value as Lf
import com.google.protobuf.duration.Duration as DurationP
import com.google.protobuf.timestamp.Timestamp
import io.grpc.StatusRuntimeException
import io.scalaland.chimney.dsl.*
import scalaz.syntax.tag.*

import java.time.{Duration, Instant}
import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable
import scala.util.Try

final class CommandsValidator(
    validateDisclosedContracts: ValidateDisclosedContracts,
    validateUpgradingPackageResolutions: ValidateUpgradingPackageResolutions,
    topologyAwarePackageSelectionEnabled: Boolean = false,
) {

  import FieldValidator.*
  import ValidationErrors.*
  import ValueValidator.*

  def validatePrepareRequest(
      prepareRequest: PrepareSubmissionRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Commands] =
    for {
      userId <- requireUserId(prepareRequest.userId, "user_id")
      commandId <- requireLedgerString(prepareRequest.commandId, "command_id").map(
        CommandId(_)
      )
      submitters <- validateSubmitters(effectiveSubmitters(prepareRequest))
      synchronizerIdO <- optionalSynchronizerId(prepareRequest.synchronizerId, "synchronizer_id")
      commandz <- requireNonEmpty(prepareRequest.commands, "commands")
      validatedCommands <- validateInnerCommands(commandz)
      ledgerEffectiveTimestamp <- validateLedgerTime(
        currentLedgerTime,
        prepareRequest.minLedgerTime.flatMap(_.time.minLedgerTimeAbs),
        prepareRequest.minLedgerTime.flatMap(_.time.minLedgerTimeRel),
      )
      validatedDisclosedContracts <- validateDisclosedContracts.fromDisclosedContracts(
        prepareRequest.disclosedContracts
      )
      packageResolutions <- validateUpgradingPackageResolutions(
        prepareRequest.packageIdSelectionPreference
      )
      prefetchKeys <- validatePrefetchContractKeys(prepareRequest.prefetchContractKeys)
    } yield Commands(
      // Not used for external submissions
      workflowId = None,
      userId = userId,
      commandId = commandId,
      // Will be provided in "execute"
      submissionId = None,
      actAs = submitters.actAs,
      readAs = submitters.readAs,
      submittedAt = Time.Timestamp.assertFromInstant(currentUtcTime),
      // Unused for transaction preparation
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration),
      commands = ApiCommands(
        commands = validatedCommands.to(ImmArray),
        ledgerEffectiveTime = ledgerEffectiveTimestamp,
        commandsReference = "",
      ),
      disclosedContracts = validatedDisclosedContracts,
      synchronizerId = synchronizerIdO,
      packageMap = packageResolutions.packageMap,
      packagePreferenceSet =
        if (topologyAwarePackageSelectionEnabled) {
          // TODO(#25385): move the decision point into the TopologyAwareCommandExecutor
          prepareRequest.packageIdSelectionPreference.map(Ref.PackageId.assertFromString).toSet
        } else
          packageResolutions.packagePreferenceSet,
      prefetchKeys = prefetchKeys,
    )

  def validateCommands(
      commands: ProtoCommands,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Commands] =
    for {
      workflowId <- validateWorkflowId(commands.workflowId)
      userId <- requireUserId(commands.userId, "user_id")
      commandId <- requireLedgerString(commands.commandId, "command_id").map(CommandId(_))
      submissionId <- validateSubmissionId(commands.submissionId)
      submitters <- validateSubmitters(effectiveSubmitters(commands))
      synchronizerId <- validateOptional(OptionUtil.emptyStringAsNone(commands.synchronizerId))(
        requireSynchronizerId(_, "synchronizer_id")
      )
      commandz <- requireNonEmpty(commands.commands, "commands")
      validatedCommands <- validateInnerCommands(commandz)
      ledgerEffectiveTimestamp <- validateLedgerTime(
        currentLedgerTime,
        commands.minLedgerTimeAbs,
        commands.minLedgerTimeRel,
      )
      deduplicationPeriod <- validateDeduplicationPeriod(
        commands.deduplicationPeriod,
        maxDeduplicationDuration,
      )
      validatedDisclosedContracts <- validateDisclosedContracts(commands)
      packageResolutions <- validateUpgradingPackageResolutions(
        commands.packageIdSelectionPreference
      )
      prefetchKeys <- validatePrefetchContractKeys(commands.prefetchContractKeys)
    } yield Commands(
      workflowId = workflowId,
      userId = userId,
      commandId = commandId,
      submissionId = submissionId,
      actAs = submitters.actAs,
      readAs = submitters.readAs,
      submittedAt = Time.Timestamp.assertFromInstant(currentUtcTime),
      deduplicationPeriod = deduplicationPeriod,
      commands = ApiCommands(
        commands = validatedCommands.to(ImmArray),
        ledgerEffectiveTime = ledgerEffectiveTimestamp,
        commandsReference = workflowId.fold("")(_.unwrap),
      ),
      disclosedContracts = validatedDisclosedContracts,
      synchronizerId = synchronizerId,
      packageMap = packageResolutions.packageMap,
      packagePreferenceSet =
        if (topologyAwarePackageSelectionEnabled) {
          // TODO(#25385): move the decision point into the TopologyAwareCommandExecutor
          commands.packageIdSelectionPreference.map(Ref.PackageId.assertFromString).toSet
        } else
          packageResolutions.packagePreferenceSet,
      prefetchKeys = prefetchKeys,
    )

  def validateReassignmentCommands(
      reassignmentCommands: ReassignmentCommands
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, submission.SubmitReassignmentRequest] =
    for {
      submitter <- requirePartyField(reassignmentCommands.submitter, "submitter")
      userId <- requireUserId(reassignmentCommands.userId, "user_id")
      commandId <- requireCommandId(reassignmentCommands.commandId, "command_id")
      submissionId <- requireSubmissionId(reassignmentCommands.submissionId, "submission_id")
      workflowId <- validateOptional(Some(reassignmentCommands.workflowId).filter(_.nonEmpty))(
        requireWorkflowId(_, "workflow_id")
      )
      reassignmentCommands <- reassignmentCommands.commands.traverse {
        _.command match {
          case ReassignmentCommand.Command.Empty =>
            Left(ValidationErrors.missingField("command"))
          case assignCommand: ReassignmentCommand.Command.AssignCommand =>
            for {
              sourceSynchronizerId <- requireSynchronizerId(assignCommand.value.source, "source")
              targetSynchronizerId <- requireSynchronizerId(assignCommand.value.target, "target")
              longUnassignId <- Try(assignCommand.value.unassignId.toLong).toEither.left.map(_ =>
                ValidationErrors.invalidField("unassign_id", "Invalid unassign ID")
              )
              timestampUnassignId <- Time.Timestamp
                .fromLong(longUnassignId)
                .left
                .map(_ => ValidationErrors.invalidField("unassign_id", "Invalid unassign ID"))
            } yield Left(
              submission.AssignCommand(
                sourceSynchronizerId = Source(sourceSynchronizerId),
                targetSynchronizerId = Target(targetSynchronizerId),
                unassignId = timestampUnassignId,
              )
            )
          case unassignCommand: ReassignmentCommand.Command.UnassignCommand =>
            for {
              sourceSynchronizerId <- requireSynchronizerId(unassignCommand.value.source, "source")
              targetSynchronizerId <- requireSynchronizerId(unassignCommand.value.target, "target")
              cid <- requireContractId(unassignCommand.value.contractId, "contract_id")
            } yield Right(
              submission.UnassignCommand(
                sourceSynchronizerId = Source(sourceSynchronizerId),
                targetSynchronizerId = Target(targetSynchronizerId),
                contractId = cid,
              )
            )
        }
      }
    } yield submission.SubmitReassignmentRequest(
      submitter = submitter,
      userId = userId,
      commandId = commandId,
      submissionId = submissionId,
      workflowId = workflowId,
      reassignmentCommands = reassignmentCommands,
    )

  def validateLedgerTime(
      currentTime: Instant,
      minLedgerTimeAbs: Option[Timestamp],
      minLedgerTimeRel: Option[DurationP],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Time.Timestamp] =
    for {
      ledgerEffectiveTime <- (minLedgerTimeAbs, minLedgerTimeRel) match {
        case (None, None) => Right(currentTime)
        case (Some(minAbs), None) =>
          Right(currentTime.max(TimestampConversion.toInstant(minAbs)))
        case (None, Some(minRel)) => Right(currentTime.plus(DurationConversion.fromProto(minRel)))
        case (Some(_), Some(_)) =>
          Left(
            invalidArgument(
              "min_ledger_time_abs cannot be specified at the same time as min_ledger_time_rel"
            )
          )
      }
      ledgerEffectiveTimestamp <- Time.Timestamp
        .fromInstant(ledgerEffectiveTime)
        .left
        .map(_ =>
          invalidArgument(
            s"Can not represent command ledger time $ledgerEffectiveTime as a Daml timestamp"
          )
        )

    } yield ledgerEffectiveTimestamp

  // Public because it is used by Canton.
  def validateInnerCommands(
      commands: Seq[Command]
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, immutable.Seq[ApiCommand]] =
    commands.traverse(command => validateInnerCommand(command.command))

  // Public so that clients have an easy way to convert ProtoCommand.Command to ApiCommand.
  def validateInnerCommand(
      command: Command.Command
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ApiCommand] =
    command match {
      case c: ProtoCreate =>
        for {
          templateId <- requirePresence(c.value.templateId, "template_id")
          typeConRef <- validateTypeConRef(templateId)
          createArguments <- requirePresence(c.value.createArguments, "create_arguments")
          recordId <- createArguments.recordId.traverse(validateIdentifier)
          validatedRecordField <- validateRecordFields(createArguments.fields)
        } yield ApiCommand.Create(
          templateRef = typeConRef,
          argument = Lf.ValueRecord(recordId, validatedRecordField),
        )

      case e: ProtoExercise =>
        for {
          templateId <- requirePresence(e.value.templateId, "template_id")
          templateRef <- validateTypeConRef(templateId)
          contractId <- requireContractId(e.value.contractId, "contract_id")
          choice <- requireName(e.value.choice, "choice")
          value <- requirePresence(e.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield ApiCommand.Exercise(
          typeRef = templateRef,
          contractId = contractId,
          choiceId = choice,
          argument = validatedValue,
        )

      case ek: ProtoExerciseByKey =>
        for {
          templateId <- requirePresence(ek.value.templateId, "template_id")
          templateRef <- validateTypeConRef(templateId)
          contractKey <- requirePresence(ek.value.contractKey, "contract_key")
          validatedContractKey <- validateValue(contractKey)
          choice <- requireName(ek.value.choice, "choice")
          value <- requirePresence(ek.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield ApiCommand.ExerciseByKey(
          templateRef = templateRef,
          contractKey = validatedContractKey,
          choiceId = choice,
          argument = validatedValue,
        )

      case ce: ProtoCreateAndExercise =>
        for {
          templateId <- requirePresence(ce.value.templateId, "template_id")
          templateRef <- validateTypeConRef(templateId)
          createArguments <- requirePresence(ce.value.createArguments, "create_arguments")
          recordId <- createArguments.recordId.traverse(validateIdentifier)
          validatedRecordField <- validateRecordFields(createArguments.fields)
          choice <- requireName(ce.value.choice, "choice")
          value <- requirePresence(ce.value.choiceArgument, "value")
          validatedChoiceArgument <- validateValue(value)
        } yield ApiCommand.CreateAndExercise(
          templateRef = templateRef,
          createArgument = Lf.ValueRecord(recordId, validatedRecordField),
          choiceId = choice,
          choiceArgument = validatedChoiceArgument,
        )
      case ProtoEmpty =>
        Left(missingField("command"))
    }

  private def validateSubmitters(
      submitters: Submitters[String]
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Submitters[Ref.Party]] = {
    def actAsMustNotBeEmpty(effectiveActAs: Set[Ref.Party]) =
      Either.cond(
        effectiveActAs.nonEmpty,
        (),
        missingField("party or act_as"),
      )

    for {
      actAs <- requireParties(submitters.actAs)
      readAs <- requireParties(submitters.readAs)
      _ <- actAsMustNotBeEmpty(actAs)
    } yield Submitters(actAs, readAs)
  }

  /** Same as [[validateDeduplicationPeriod]] but for the "ExecuteSubmissionRequest" RPC of the
    * interactive submission service.
    */
  def validateExecuteDeduplicationPeriod(
      deduplicationPeriod: ExecuteSubmissionRequest.DeduplicationPeriod,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, DeduplicationPeriod] =
    validateDeduplicationPeriod(
      deduplicationPeriod.transformInto[ProtoCommands.DeduplicationPeriod],
      maxDeduplicationDuration,
    )

  /** We validate only using current time because we set the currentTime as submitTime so no need to
    * check both
    */
  def validateDeduplicationPeriod(
      deduplicationPeriod: ProtoCommands.DeduplicationPeriod,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, DeduplicationPeriod] =
    deduplicationPeriod match {
      case ProtoCommands.DeduplicationPeriod.Empty =>
        Right(DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration))
      case ProtoCommands.DeduplicationPeriod.DeduplicationDuration(duration) =>
        val deduplicationDuration = DurationConversion.fromProto(duration)
        DeduplicationPeriodValidator
          .validateNonNegativeDuration(deduplicationDuration)
          .map(DeduplicationPeriod.DeduplicationDuration.apply)
      case ProtoCommands.DeduplicationPeriod.DeduplicationOffset(offset) =>
        if (offset < 0L)
          Left(
            RequestValidationErrors.NegativeOffset
              .Error(
                fieldName = "deduplication_period",
                offsetValue = offset,
                message =
                  s"the deduplication offset has to be a non-negative integer and not $offset",
              )
              .asGrpcError
          )
        else
          Right(
            DeduplicationPeriod.DeduplicationOffset(
              Offset.tryOffsetOrParticipantBegin(offset)
            )
          )
    }

  private def validatePrefetchContractKeys(
      keys: Seq[PrefetchContractKey]
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Seq[ApiContractKey]] =
    keys.traverse(validatePrefetchContractKey)

  private def validatePrefetchContractKey(
      key: PrefetchContractKey
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ApiContractKey] = {
    val PrefetchContractKey(templateIdO, contractKeyO) = key
    for {
      templateId <- requirePresence(templateIdO, "template_id")
      templateRef <- validateTypeConRef(templateId)
      contractKey <- requirePresence(contractKeyO, "contract_key")
      validatedKey <- validateValue(contractKey)
    } yield ApiContractKey(templateRef, validatedKey)
  }

}

object CommandsValidator {

  /** Effective submitters of a command
    * @param actAs
    *   Guaranteed to be non-empty. Will contain exactly one element in most cases.
    * @param readAs
    *   May be empty.
    */
  final case class Submitters[T](actAs: Set[T], readAs: Set[T])

  def effectiveSubmitters(commands: Option[ProtoCommands]): Submitters[String] =
    commands.fold(noSubmitters)(effectiveSubmitters)

  def effectiveSubmitters(prepareRequest: PrepareSubmissionRequest): Submitters[String] = {
    val actAs = prepareRequest.actAs.toSet
    val readAs = prepareRequest.readAs.toSet -- actAs
    Submitters(actAs, readAs)
  }

  def effectiveSubmitters(commands: ProtoCommands): Submitters[String] = {
    val actAs = commands.actAs.toSet
    val readAs = commands.readAs.toSet -- actAs
    Submitters(actAs, readAs)
  }

  val noSubmitters: Submitters[String] = Submitters(Set.empty, Set.empty)

}
