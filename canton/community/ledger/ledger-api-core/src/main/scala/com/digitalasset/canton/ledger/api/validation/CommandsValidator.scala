// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.traverse.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.commands.Command.Command.{
  Create as ProtoCreate,
  CreateAndExercise as ProtoCreateAndExercise,
  Empty as ProtoEmpty,
  Exercise as ProtoExercise,
  ExerciseByKey as ProtoExerciseByKey,
}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1.commands as V1
import com.daml.ledger.api.v2.commands as V2
import com.daml.lf.command.*
import com.daml.lf.data.*
import com.daml.lf.value.Value as Lf
import com.digitalasset.canton.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.digitalasset.canton.ledger.api.util.{DurationConversion, TimestampConversion}
import com.digitalasset.canton.ledger.api.validation.CommandsValidator.{
  Submitters,
  effectiveSubmitters,
}
import com.digitalasset.canton.ledger.api.validation.FieldValidator.ResolveToTemplateId
import com.digitalasset.canton.ledger.api.{DeduplicationPeriod, domain}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.offset.Offset
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag.*

import java.time.{Duration, Instant}
import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.nowarn
import scala.collection.immutable

final class CommandsValidator(
    ledgerId: LedgerId,
    resolveToTemplateId: ResolveToTemplateId,
    upgradingEnabled: Boolean = false,
    validateDisclosedContracts: ValidateDisclosedContracts = new ValidateDisclosedContracts,
) {

  import ValidationErrors.*
  import FieldValidator.*
  import ValueValidator.*

  def validateCommands(
      commands: V1.Commands,
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
      commands: V1.Commands,
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
      commands: Seq[V1.Command]
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
      command: V1.Command.Command
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ApiCommand] =
    command match {
      case c: ProtoCreate =>
        for {
          templateId <- requirePresence(c.value.templateId, "template_id")
          validatedTemplateId <- validateTemplateId(templateId)
          createArguments <- requirePresence(c.value.createArguments, "create_arguments")
          recordId <- createArguments.recordId.traverse(validateTemplateId)
          validatedRecordField <- validateRecordFields(createArguments.fields)
        } yield ApiCommand.Create(
          templateId = validatedTemplateId,
          argument = Lf.ValueRecord(recordId, validatedRecordField),
        )

      case e: ProtoExercise =>
        for {
          templateId <- requirePresence(e.value.templateId, "template_id")
          validatedTemplateId <- validateTemplateId(templateId)
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
          validatedTemplateId <- validateTemplateId(templateId)
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
          validatedTemplateId <- validateTemplateId(templateId)
          createArguments <- requirePresence(ce.value.createArguments, "create_arguments")
          recordId <- createArguments.recordId.traverse(validateTemplateId)
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

  private def validateTemplateId(identifier: Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Identifier] =
    if (upgradingEnabled)
      validateIdentifierWithOptionalPackageId(resolveToTemplateId)(identifier)
    else validateIdentifier(identifier)

  private def validateSubmitters(
      commands: V1.Commands
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

  // TODO(i12279): Address usage of deprecated class DeduplicationTime

  /** We validate only using current time because we set the currentTime as submitTime so no need to check both
    */
  @nowarn(
    "msg=class DeduplicationTime in object DeduplicationPeriod is deprecated"
  )
  def validateDeduplicationPeriod(
      deduplicationPeriod: V1.Commands.DeduplicationPeriod,
      optMaxDeduplicationDuration: Option[Duration],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DeduplicationPeriod] =
    optMaxDeduplicationDuration.fold[Either[StatusRuntimeException, DeduplicationPeriod]](
      Left(
        RequestValidationErrors.NotFound.LedgerConfiguration
          .Reject()
          .asGrpcError
      )
    ) { maxDeduplicationDuration =>
      deduplicationPeriod match {
        case V1.Commands.DeduplicationPeriod.Empty =>
          Right(DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration))
        case V1.Commands.DeduplicationPeriod.DeduplicationTime(duration) =>
          val deduplicationDuration = DurationConversion.fromProto(duration)
          DeduplicationPeriodValidator
            .validateNonNegativeDuration(deduplicationDuration)
            .map(DeduplicationPeriod.DeduplicationDuration)
        case V1.Commands.DeduplicationPeriod.DeduplicationDuration(duration) =>
          val deduplicationDuration = DurationConversion.fromProto(duration)
          DeduplicationPeriodValidator
            .validateNonNegativeDuration(deduplicationDuration)
            .map(DeduplicationPeriod.DeduplicationDuration)
        case V1.Commands.DeduplicationPeriod.DeduplicationOffset(offset) =>
          Ref.HexString
            .fromString(offset)
            .fold(
              _ =>
                Left(
                  RequestValidationErrors.NonHexOffset
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
  def apply(
      ledgerId: LedgerId,
      resolveToTemplateId: ResolveToTemplateId,
      upgradingEnabled: Boolean,
  ) =
    new CommandsValidator(
      ledgerId = ledgerId,
      resolveToTemplateId = resolveToTemplateId,
      upgradingEnabled = upgradingEnabled,
    )

  /** Effective submitters of a command
    * @param actAs Guaranteed to be non-empty. Will contain exactly one element in most cases.
    * @param readAs May be empty.
    */
  final case class Submitters[T](actAs: Set[T], readAs: Set[T])

  def effectiveSubmitters(commands: Option[V1.Commands]): Submitters[String] = {
    commands.fold(noSubmitters)(effectiveSubmitters)
  }

  def effectiveSubmittersV2(commands: Option[V2.Commands]): Submitters[String] = {
    commands.fold(noSubmitters)(effectiveSubmitters)
  }

  def effectiveSubmitters(commands: V1.Commands): Submitters[String] = {
    val actAs = effectiveActAs(commands)
    val readAs = commands.readAs.toSet -- actAs
    Submitters(actAs, readAs)
  }

  def effectiveSubmitters(commands: V2.Commands): Submitters[String] = {
    val actAs = effectiveActAs(commands)
    val readAs = commands.readAs.toSet -- actAs
    Submitters(actAs, readAs)
  }

  def effectiveActAs(commands: V1.Commands): Set[String] =
    if (commands.party.isEmpty)
      commands.actAs.toSet
    else
      commands.actAs.toSet + commands.party

  def effectiveActAs(commands: V2.Commands): Set[String] =
    if (commands.party.isEmpty)
      commands.actAs.toSet
    else
      commands.actAs.toSet + commands.party

  val noSubmitters: Submitters[String] = Submitters(Set.empty, Set.empty)
}
