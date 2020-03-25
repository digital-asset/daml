// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import java.time.{Duration, Instant}

import com.digitalasset.api.util.{DurationConversion, TimestampConversion}
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data._
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.commands.Command.Command.{
  Create => ProtoCreate,
  CreateAndExercise => ProtoCreateAndExercise,
  Empty => ProtoEmpty,
  Exercise => ProtoExercise,
  ExerciseByKey => ProtoExerciseByKey
}
import com.digitalasset.ledger.api.v1.commands.{Command => ProtoCommand, Commands => ProtoCommands}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.server.api.validation.ErrorFactories._
import com.digitalasset.platform.server.api.validation.FieldValidations.{requirePresence, _}
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.Ordering.Implicits._

final class CommandsValidator(ledgerId: LedgerId) {

  import ValueValidator._

  def validateCommands(
      commands: ProtoCommands,
      currentLedgerTime: Instant,
      currentUTCTime: Instant,
      maxDeduplicationTime: Duration): Either[StatusRuntimeException, domain.Commands] =
    for {
      cmdLegerId <- requireLedgerString(commands.ledgerId, "ledger_id")
      ledgerId <- matchLedgerId(ledgerId)(LedgerId(cmdLegerId))
      workflowId <- if (commands.workflowId.isEmpty) Right(None)
      else requireLedgerString(commands.workflowId).map(x => Some(domain.WorkflowId(x)))
      appId <- requireLedgerString(commands.applicationId, "application_id")
        .map(domain.ApplicationId(_))
      commandId <- requireLedgerString(commands.commandId, "command_id").map(domain.CommandId(_))
      submitter <- requireParty(commands.party, "party")
      commandz <- requireNonEmpty(commands.commands, "commands")
      validatedCommands <- validateInnerCommands(commandz, submitter)
      ledgerEffectiveTime <- validateLedgerTime(currentLedgerTime, commands)
      ledgerEffectiveTimestamp <- Time.Timestamp
        .fromInstant(ledgerEffectiveTime)
        .left
        .map(_ =>
          invalidArgument(
            s"Can not represent command ledger time $ledgerEffectiveTime as a DAML timestamp"))
      deduplicationTime <- validateDeduplicationTime(
        commands.deduplicationTime,
        maxDeduplicationTime,
        "deduplication_time")
    } yield
      domain.Commands(
        ledgerId = ledgerId,
        workflowId = workflowId,
        applicationId = appId,
        commandId = commandId,
        submitter = submitter,
        ledgerEffectiveTime = ledgerEffectiveTime,
        submittedAt = currentUTCTime,
        deduplicateUntil = currentUTCTime.plus(deduplicationTime),
        commands = Commands(
          submitter = submitter,
          commands = ImmArray(validatedCommands),
          ledgerEffectiveTime = ledgerEffectiveTimestamp,
          commandsReference = workflowId.fold("")(_.unwrap)
        ),
      )

  private def validateLedgerTime(
      currentTime: Instant,
      commands: ProtoCommands,
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
            "min_ledger_time_abs can not be specified at the same time as min_ledger_time_rel"))
    }
  }

  private def validateInnerCommands(
      commands: Seq[ProtoCommand],
      submitter: Ref.Party
  ): Either[StatusRuntimeException, immutable.Seq[Command]] =
    commands.foldLeft[Either[StatusRuntimeException, Vector[Command]]](
      Right(Vector.empty[Command]))((commandz, command) => {
      for {
        validatedInnerCommands <- commandz
        validatedInnerCommand <- validateInnerCommand(command.command)
      } yield validatedInnerCommands :+ validatedInnerCommand
    })

  private def validateInnerCommand(
      command: ProtoCommand.Command): Either[StatusRuntimeException, Command] =
    command match {
      case c: ProtoCreate =>
        for {
          templateId <- requirePresence(c.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          createArguments <- requirePresence(c.value.createArguments, "create_arguments")
          recordId <- validateOptionalIdentifier(createArguments.recordId)
          validatedRecordField <- validateRecordFields(createArguments.fields)
        } yield
          CreateCommand(
            templateId = validatedTemplateId,
            argument = Lf.ValueRecord(recordId, validatedRecordField))

      case e: ProtoExercise =>
        for {
          templateId <- requirePresence(e.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          contractId <- requireLedgerString(e.value.contractId, "contract_id")
          choice <- requireName(e.value.choice, "choice")
          value <- requirePresence(e.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield
          ExerciseCommand(
            templateId = validatedTemplateId,
            contractId = contractId,
            choiceId = choice,
            argument = validatedValue)

      case ek: ProtoExerciseByKey =>
        for {
          templateId <- requirePresence(ek.value.templateId, "template_id")
          validatedTemplateId <- validateIdentifier(templateId)
          contractKey <- requirePresence(ek.value.contractKey, "contract_key")
          validatedContractKey <- validateValue(contractKey)
          choice <- requireName(ek.value.choice, "choice")
          value <- requirePresence(ek.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield
          ExerciseByKeyCommand(
            templateId = validatedTemplateId,
            contractKey = validatedContractKey,
            choiceId = choice,
            argument = validatedValue
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
        } yield
          CreateAndExerciseCommand(
            templateId = validatedTemplateId,
            createArgument = Lf.ValueRecord(recordId, validatedRecordField),
            choiceId = choice,
            choiceArgument = validatedChoiceArgument
          )
      case ProtoEmpty =>
        Left(missingField("command"))
    }
}
