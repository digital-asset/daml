// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.Value.VariantValue
import com.digitalasset.ledger.api.messages.command.submission
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Empty, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands}
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{
  Identifier,
  RecordField,
  Value,
  List => ApiList,
  Variant => ApiVariant,
  Map => ApiMap,
}
import com.digitalasset.platform.server.api.validation.ErrorFactories._
import com.digitalasset.platform.server.api.validation.FieldValidations.{requirePresence, _}
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.util.context.TraceContextConversions._
import io.grpc.StatusRuntimeException

import scala.collection.immutable

class CommandSubmissionRequestValidator(ledgerId: String, identifierResolver: IdentifierResolver) {

  def validate(req: SubmitRequest): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- validateCommands(commands)
    } yield submission.SubmitRequest(validatedCommands, req.traceContext.map(toBrave))

  def validateCommands(commands: Commands): Either[StatusRuntimeException, domain.Commands] =
    for {
      ledgerId <- matchLedgerId(ledgerId)(commands.ledgerId)
      commandId <- requireNonEmptyString(commands.commandId, "command_id")
      appId <- requireNonEmptyString(commands.applicationId, "application_id")
      submitter <- requireNonEmptyString(commands.party, "party")
      let <- requirePresence(commands.ledgerEffectiveTime, "ledger_effective_time")
      mrt <- requirePresence(commands.maximumRecordTime, "maximum_record_time")
      validatedCommands <- validateInnerCommands(commands.commands)
    } yield
      domain.Commands(
        domain.LedgerId(ledgerId),
        Option(commands.workflowId).filterNot(_.isEmpty).map(domain.WorkflowId(_)),
        domain.ApplicationId(appId),
        domain.CommandId(commandId),
        domain.Party(submitter),
        TimestampConversion.toInstant(let),
        TimestampConversion.toInstant(mrt),
        validatedCommands
      )

  private def validateInnerCommands(
      commands: Seq[Command]): Either[StatusRuntimeException, immutable.Seq[domain.Command]] =
    commands.foldLeft[Either[StatusRuntimeException, Vector[domain.Command]]](
      Right(Vector.empty[domain.Command]))((commandz, command) => {
      for {
        validatedInnerCommands <- commandz
        validatedInnerCommand <- validateInnerCommand(command.command)
      } yield validatedInnerCommands :+ validatedInnerCommand
    })

  private def validateInnerCommand(
      command: Command.Command): Either[StatusRuntimeException, domain.Command] =
    command match {
      case c: Create =>
        for {
          templateId <- requirePresence(c.value.templateId, "template_id")
          validatedTemplateId <- identifierResolver.resolveIdentifier(templateId)
          createArguments <- requirePresence(c.value.createArguments, "create_arguments")
          recordId <- validateOptionalIdentifier(createArguments.recordId)
          validatedRecordField <- validateRecordFields(createArguments.fields)
        } yield
          domain.CreateCommand(
            validatedTemplateId,
            domain.Value.RecordValue(recordId, validatedRecordField))

      case e: Exercise =>
        for {
          templateId <- requirePresence(e.value.templateId, "template_id")
          validatedTemplateId <- identifierResolver.resolveIdentifier(templateId)
          contractId <- requireNonEmptyString(e.value.contractId, "contract_id")
          choice <- requireNonEmptyString(e.value.choice, "choice")
          value <- requirePresence(e.value.choiceArgument, "value")
          validatedValue <- validateValue(value)
        } yield
          domain.ExerciseCommand(
            validatedTemplateId,
            domain.ContractId(contractId),
            domain.Choice(choice),
            validatedValue)
      case Empty => Left(missingField("command"))
    }

  private def validateRecordFields(recordFields: Seq[RecordField])
    : Either[StatusRuntimeException, immutable.Seq[domain.RecordField]] =
    recordFields
      .foldLeft[Either[StatusRuntimeException, Vector[domain.RecordField]]](
        Right(Vector.empty[domain.RecordField]))((acc, rf) => {
        for {
          fields <- acc
          v <- requirePresence(rf.value, "value")
          value <- validateValue(v)
          label = if (rf.label.isEmpty) None else Some(domain.Label(rf.label))
        } yield fields :+ domain.RecordField(label, value)
      })

  def validateMapKey(value: Value): Either[StatusRuntimeException, String] = value.sum match {
    case Sum.Text(text) => Right(text)
    case _ => Left(invalidArgument(s"expected Text, found $value"))
  }

  def validateValue(value: Value): Either[StatusRuntimeException, domain.Value] = value.sum match {
    case Sum.ContractId(cId) => Right(domain.Value.ContractIdValue(domain.ContractId(cId)))
    case Sum.Decimal(value) => Right(domain.Value.DecimalValue(value))
    case Sum.Party(party) => Right(domain.Value.PartyValue(domain.Party(party)))
    case Sum.Bool(b) => Right(domain.Value.BoolValue(b))
    case Sum.Timestamp(micros) => Right(domain.Value.TimeStampValue(micros))
    case Sum.Date(days) => Right(domain.Value.DateValue(days))
    case Sum.Text(text) => Right(domain.Value.TextValue(text))
    case Sum.Int64(value) => Right(domain.Value.Int64Value(value))
    case Sum.Record(rec) =>
      for {
        recId <- validateOptionalIdentifier(rec.recordId)
        fields <- validateRecordFields(rec.fields)
      } yield domain.Value.RecordValue(recId, fields)
    case Sum.Variant(ApiVariant(variantId, constructor, value)) =>
      for {
        validatedConstructor <- requireNonEmptyString(constructor, "constructor")
        v <- requirePresence(value, "value")
        validatedValue <- validateValue(v)
        validatedVariantId <- validateOptionalIdentifier(variantId)
      } yield
        VariantValue(
          validatedVariantId,
          domain.VariantConstructor(validatedConstructor),
          validatedValue)
    case Sum.List(ApiList(elems)) =>
      elems
        .foldLeft[Either[StatusRuntimeException, Vector[domain.Value]]](
          Right(Vector.empty[domain.Value]))((valuesE, v) =>
          for {
            values <- valuesE
            validatedValue <- validateValue(v)
          } yield values :+ validatedValue)
        .map(elements => domain.Value.ListValue(elements))
    case _: Sum.Unit => Right(domain.Value.UnitValue)
    case Sum.Optional(o) =>
      o.value.fold[Either[StatusRuntimeException, domain.Value]](
        Right(domain.Value.OptionalValue.Empty))(validateValue(_).map(v =>
        domain.Value.OptionalValue(Some(v))))
    case Sum.Map(m) =>
      m.entries
        .foldLeft[Either[StatusRuntimeException, Map[String, domain.Value]]](Right(Map.empty)) {
          case (acc, ApiMap.Entry(key0, value0)) =>
            for {
              map <- acc
              key <- requirePresence(key0, "key")
              validatedKey <- validateMapKey(key)
              v <- requirePresence(value0, "value")
              validatedValue <- validateValue(v)
            } yield map + (validatedKey -> validatedValue)
        }
        .map(domain.Value.MapValue)
    case Sum.Empty => Left(missingField("value"))
  }

  private def validateOptionalIdentifier(
      variantIdO: Option[Identifier]): Either[StatusRuntimeException, Option[domain.Identifier]] = {
    variantIdO
      .map { variantId =>
        identifierResolver.resolveIdentifier(variantId).map(Some.apply)
      }
      .getOrElse(Right(None))
  }

}
