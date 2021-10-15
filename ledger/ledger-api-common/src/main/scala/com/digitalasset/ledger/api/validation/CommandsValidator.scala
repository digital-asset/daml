// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.api.util.{DurationConversion, TimestampConversion}
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.commands.Command.Command.{
  Create => ProtoCreate,
  CreateAndExercise => ProtoCreateAndExercise,
  Empty => ProtoEmpty,
  Exercise => ProtoExercise,
  ExerciseByKey => ProtoExerciseByKey,
}
import com.daml.ledger.api.v1.commands.{Command => ProtoCommand, Commands => ProtoCommands}
import com.daml.ledger.api.validation.CommandsValidator.{Submitters, effectiveSubmitters}
import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.value.{Value => Lf}
import com.daml.platform.server.api.validation.ErrorFactories._
import com.daml.platform.server.api.validation.FieldValidations.{requirePresence, _}
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag._

import java.time.{Duration, Instant}
import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable

final class CommandsValidator(ledgerId: LedgerId) {

  import ValueValidator._

  def validateCommands(
      commands: ProtoCommands,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationTime: Option[Duration],
  )(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Commands] =
    for {
      cmdLegerId <- requireLedgerString(commands.ledgerId, "ledger_id")
      ledgerId <- matchLedgerId(ledgerId)(LedgerId(cmdLegerId))
      workflowId <-
        if (commands.workflowId.isEmpty) Right(None)
        else requireLedgerString(commands.workflowId).map(x => Some(domain.WorkflowId(x)))
      appId <- requireLedgerString(commands.applicationId, "application_id")
        .map(domain.ApplicationId(_))
      commandId <- requireLedgerString(commands.commandId, "command_id").map(domain.CommandId(_))
      submissionId <- requireSubmissionId(commands.submissionId)
      submitters <- validateSubmitters(commands)
      commandz <- requireNonEmpty(commands.commands, "commands")
      validatedCommands <- validateInnerCommands(commandz)
      ledgerEffectiveTime <- validateLedgerTime(currentLedgerTime, commands)
      ledgerEffectiveTimestamp <- Time.Timestamp
        .fromInstant(ledgerEffectiveTime)
        .left
        .map(_ =>
          invalidArgument(definiteAnswer = Some(false))(
            s"Can not represent command ledger time $ledgerEffectiveTime as a Daml timestamp"
          )
        )
      deduplicationPeriod <- validateDeduplicationPeriod(
        commands.deduplicationPeriod,
        maxDeduplicationTime,
        "deduplication_period",
      )
    } yield domain.Commands(
      ledgerId = ledgerId,
      workflowId = workflowId,
      applicationId = appId,
      commandId = commandId,
      submissionId = submissionId,
      actAs = submitters.actAs,
      readAs = submitters.readAs,
      submittedAt = currentUtcTime,
      deduplicationPeriod = deduplicationPeriod,
      commands = Commands(
        commands = validatedCommands.to(ImmArray),
        ledgerEffectiveTime = ledgerEffectiveTimestamp,
        commandsReference = workflowId.fold("")(_.unwrap),
      ),
    )

  private def validateLedgerTime(
      currentTime: Instant,
      commands: ProtoCommands,
  )(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
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
          invalidArgument(definiteAnswer = Some(false))(
            "min_ledger_time_abs cannot be specified at the same time as min_ledger_time_rel"
          )
        )
    }
  }

  private def validateInnerCommands(
      commands: Seq[ProtoCommand]
  )(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, immutable.Seq[ApiCommand]] =
    commands.foldLeft[Either[StatusRuntimeException, Vector[ApiCommand]]](
      Right(Vector.empty[ApiCommand])
    )((commandz, command) => {
      for {
        validatedInnerCommands <- commandz
        validatedInnerCommand <- validateInnerCommand(command.command)
      } yield validatedInnerCommands :+ validatedInnerCommand
    })

  private def validateInnerCommand(
      command: ProtoCommand.Command
  )(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ApiCommand] =
    command match {
      case c: ProtoCreate =>
        for {
          templateId <- requirePresence(c.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          createArguments <- requirePresence(c.value.createArguments, "create_arguments")
          recordId <- validateOptionalIdentifier(createArguments.recordId)
          validatedRecordField <- validateRecordFields(createArguments.fields)
        } yield CreateCommand(
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
        } yield ExerciseCommand(
          templateId = validatedTemplateId,
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
        } yield ExerciseByKeyCommand(
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
        } yield CreateAndExerciseCommand(
          templateId = validatedTemplateId,
          createArgument = Lf.ValueRecord(recordId, validatedRecordField),
          choiceId = choice,
          choiceArgument = validatedChoiceArgument,
        )
      case ProtoEmpty =>
        Left(missingField("command", definiteAnswer = Some(false)))
    }

  private def validateSubmitters(
      commands: ProtoCommands
  )(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Submitters[Ref.Party]] = {
    def actAsMustNotBeEmpty(effectiveActAs: Set[Ref.Party]) =
      Either.cond(
        effectiveActAs.nonEmpty,
        (),
        missingField("party or act_as", definiteAnswer = Some(false)),
      )

    val submitters = effectiveSubmitters(commands)
    for {
      actAs <- requireParties(submitters.actAs)
      readAs <- requireParties(submitters.readAs)
      _ <- actAsMustNotBeEmpty(actAs)
    } yield Submitters(actAs, readAs)
  }
}

object CommandsValidator {

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
