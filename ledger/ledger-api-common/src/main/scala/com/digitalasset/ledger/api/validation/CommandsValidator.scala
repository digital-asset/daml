// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import java.time.{Duration, Instant}

import com.daml.api.util.{DurationConversion, TimestampConversion}
import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.daml.ledger.api.v1.commands
import com.daml.ledger.api.v1.commands.Command.Command.{
  Create => ProtoCreate,
  CreateAndExercise => ProtoCreateAndExercise,
  Empty => ProtoEmpty,
  Exercise => ProtoExercise,
  ExerciseByKey => ProtoExerciseByKey,
}
import com.daml.ledger.api.v1.commands.{Command => ProtoCommand, Commands => ProtoCommands}
import com.daml.ledger.api.validation.CommandsValidator.{Submitters, effectiveSubmitters}
import com.daml.ledger.api.{DeduplicationPeriod, domain}
import com.daml.ledger.offset.Offset
import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.value.{Value => Lf}
import com.daml.platform.server.api.validation.{DeduplicationPeriodValidator, FieldValidations}
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag._

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable
import scala.annotation.nowarn

final class CommandsValidator(
    ledgerId: LedgerId,
    validateDisclosedContracts: ValidateDisclosedContracts = new ValidateDisclosedContracts(false),
) {

  import ValidationErrors._
  import FieldValidations._
  import ValueValidator._

  def validateCommands(
      commands: ProtoCommands,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Option[Duration],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Commands] =
    for {
      ledgerId <- matchLedgerId(ledgerId)(optionalLedgerId(commands.ledgerId))
      workflowId <-
        if (commands.workflowId.isEmpty) Right(None)
        else requireLedgerString(commands.workflowId).map(x => Some(domain.WorkflowId(x)))
      appId <- requireApplicationId(commands.applicationId, "application_id")
      commandId <- requireLedgerString(commands.commandId, "command_id").map(domain.CommandId(_))
      submissionId <- validateSubmissionId(commands.submissionId)
      submitters <- validateSubmitters(commands)
      commandz <- requireNonEmpty(commands.commands, "commands")
      validatedCommands <- validateInnerCommands(commandz)
      ledgerEffectiveTime <- validateLedgerTime(currentLedgerTime, commands)
      ledgerEffectiveTimestamp <- Time.Timestamp
        .fromInstant(ledgerEffectiveTime)
        .left
        .map(_ =>
          invalidArgument(
            s"Can not represent command ledger time $ledgerEffectiveTime as a Daml timestamp"
          )
        )
      deduplicationPeriod <- validateDeduplicationPeriod(
        commands.deduplicationPeriod,
        maxDeduplicationDuration,
      )
      validatedDisclosedContracts <- validateDisclosedContracts(commands)
    } yield domain.Commands(
      ledgerId = ledgerId,
      workflowId = workflowId,
      applicationId = appId,
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
    )

  private def validateLedgerTime(
      currentTime: Instant,
      commands: ProtoCommands,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Instant] = {
    val minLedgerTimeAbs = commands.minLedgerTimeAbs
    val minLedgerTimeRel = commands.minLedgerTimeRel

    (minLedgerTimeAbs, minLedgerTimeRel) match {
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
  }

  // Public because it is used by Canton.
  def validateInnerCommands(
      commands: Seq[ProtoCommand]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, immutable.Seq[ApiCommand]] =
    commands.foldLeft[Either[StatusRuntimeException, Vector[ApiCommand]]](
      Right(Vector.empty[ApiCommand])
    )((commandz, command) => {
      for {
        validatedInnerCommands <- commandz
        validatedInnerCommand <- validateInnerCommand(command.command)
      } yield validatedInnerCommands :+ validatedInnerCommand
    })

  // Public so that clients have an easy way to convert ProtoCommand.Command to ApiCommand.
  def validateInnerCommand(
      command: ProtoCommand.Command
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ApiCommand] =
    command match {
      case c: ProtoCreate =>
        for {
          templateId <- requirePresence(c.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          createArguments <- requirePresence(c.value.createArguments, "create_arguments")
          recordId <- validateOptionalIdentifier(createArguments.recordId)
          validatedRecordField <- validateRecordFields(createArguments.fields)
        } yield ApiCommand.Create(
          templateId = validatedTemplateId,
          argument = Lf.ValueRecord(recordId, validatedRecordField),
        )

      case e: ProtoExercise =>
        for {
          templateId <- requirePresence(e.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          contractId <- requireContractId(e.value.contractId, "contract_id")
          choice <- requireName(e.value.choice, "choice")
          value <- requirePresence(e.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield ApiCommand.Exercise(
          typeId = validatedTemplateId,
          contractId = contractId,
          choiceId = choice,
          argument = validatedValue,
        )

      case ek: ProtoExerciseByKey =>
        for {
          templateId <- requirePresence(ek.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          contractKey <- requirePresence(ek.value.contractKey, "contract_key")
          validatedContractKey <- validateValue(contractKey)
          choice <- requireName(ek.value.choice, "choice")
          value <- requirePresence(ek.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield ApiCommand.ExerciseByKey(
          templateId = validatedTemplateId,
          contractKey = validatedContractKey,
          choiceId = choice,
          argument = validatedValue,
        )

      case ce: ProtoCreateAndExercise =>
        for {
          templateId <- requirePresence(ce.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          createArguments <- requirePresence(ce.value.createArguments, "create_arguments")
          recordId <- validateOptionalIdentifier(createArguments.recordId)
          validatedRecordField <- validateRecordFields(createArguments.fields)
          choice <- requireName(ce.value.choice, "choice")
          value <- requirePresence(ce.value.choiceArgument, "value")
          validatedChoiceArgument <- validateValue(value)
        } yield ApiCommand.CreateAndExercise(
          templateId = validatedTemplateId,
          createArgument = Lf.ValueRecord(recordId, validatedRecordField),
          choiceId = choice,
          choiceArgument = validatedChoiceArgument,
        )
      case ProtoEmpty =>
        Left(missingField("command"))
    }

  private def validateSubmitters(
      commands: ProtoCommands
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Submitters[Ref.Party]] = {
    def actAsMustNotBeEmpty(effectiveActAs: Set[Ref.Party]) =
      Either.cond(
        effectiveActAs.nonEmpty,
        (),
        missingField("party or act_as"),
      )

    val submitters = effectiveSubmitters(commands)
    for {
      actAs <- requireParties(submitters.actAs)
      readAs <- requireParties(submitters.readAs)
      _ <- actAsMustNotBeEmpty(actAs)
    } yield Submitters(actAs, readAs)
  }

  // TODO: Address usage of deprecated class DeduplicationTime

  /** We validate only using current time because we set the currentTime as submitTime so no need to check both
    */
  @nowarn(
    "msg=class DeduplicationTime in object DeduplicationPeriod is deprecated"
  )
  def validateDeduplicationPeriod(
      deduplicationPeriod: commands.Commands.DeduplicationPeriod,
      optMaxDeduplicationDuration: Option[Duration],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DeduplicationPeriod] =
    optMaxDeduplicationDuration.fold[Either[StatusRuntimeException, DeduplicationPeriod]](
      Left(
        LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration
          .Reject()
          .asGrpcError
      )
    ) { maxDeduplicationDuration =>
      deduplicationPeriod match {
        case commands.Commands.DeduplicationPeriod.Empty =>
          Right(DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration))
        case commands.Commands.DeduplicationPeriod.DeduplicationTime(duration) =>
          val deduplicationDuration = DurationConversion.fromProto(duration)
          DeduplicationPeriodValidator
            .validateNonNegativeDuration(deduplicationDuration)
            .map(DeduplicationPeriod.DeduplicationDuration)
        case commands.Commands.DeduplicationPeriod.DeduplicationDuration(duration) =>
          val deduplicationDuration = DurationConversion.fromProto(duration)
          DeduplicationPeriodValidator
            .validateNonNegativeDuration(deduplicationDuration)
            .map(DeduplicationPeriod.DeduplicationDuration)
        case commands.Commands.DeduplicationPeriod.DeduplicationOffset(offset) =>
          Ref.HexString
            .fromString(offset)
            .fold(
              _ =>
                Left(
                  LedgerApiErrors.RequestValidation.NonHexOffset
                    .Error(
                      fieldName = "deduplication_period",
                      offsetValue = offset,
                      message =
                        s"the deduplication offset has to be a hexadecimal string and not $offset",
                    )
                    .asGrpcError
                ),
              hexOffset =>
                Right(DeduplicationPeriod.DeduplicationOffset(Offset.fromHexString(hexOffset))),
            )
      }
    }
}

object CommandsValidator {
  def apply(ledgerId: LedgerId, explicitDisclosureUnsafeEnabled: Boolean) = new CommandsValidator(
    ledgerId = ledgerId,
    validateDisclosedContracts = new ValidateDisclosedContracts(explicitDisclosureUnsafeEnabled),
  )

  /** Effective submitters of a command
    * @param actAs Guaranteed to be non-empty. Will contain exactly one element in most cases.
    * @param readAs May be empty.
    */
  case class Submitters[T](actAs: Set[T], readAs: Set[T])

  def effectiveSubmitters(commands: Option[ProtoCommands]): Submitters[String] = {
    commands.fold(noSubmitters)(effectiveSubmitters)
  }

  def effectiveSubmitters(commands: ProtoCommands): Submitters[String] = {
    val actAs = effectiveActAs(commands)
    val readAs = commands.readAs.toSet -- actAs
    Submitters(actAs, readAs)
  }

  def effectiveActAs(commands: ProtoCommands): Set[String] =
    if (commands.party.isEmpty)
      commands.actAs.toSet
    else
      commands.actAs.toSet + commands.party

  val noSubmitters: Submitters[String] = Submitters(Set.empty, Set.empty)
}
