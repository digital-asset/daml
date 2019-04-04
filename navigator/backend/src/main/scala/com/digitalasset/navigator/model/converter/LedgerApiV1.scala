// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model.converter

import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.digitalasset.ledger.api.{v1 => V1}
import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.{model => Model}
import com.digitalasset.navigator.model.{IdentifierApiConversions, IdentifierDamlConversions}
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.code.Code
import scalaz.Tag

import scala.util.Try

case object LedgerApiV1 {
  // ------------------------------------------------------------------------------------------------------------------
  // Types
  // ------------------------------------------------------------------------------------------------------------------
  case class Context(party: ApiTypes.Party, templates: Model.PackageRegistry)

  // ------------------------------------------------------------------------------------------------------------------
  // Read methods (V1 -> Model)
  // ------------------------------------------------------------------------------------------------------------------
  /*
  def readTransaction(tx: V1.transaction.Transaction, ctx: Context): Either[ConversionError, Model.Transaction] = {
    for {
      events      <- Converter.sequence(tx.events.map(ev =>
        readEvent(ev, ApiTypes.TransactionId(tx.transactionId), ctx, List.empty, ApiTypes.WorkflowId(tx.workflowId), None)))
      effectiveAt <- Converter.checkExists("Transaction.effectiveAt", tx.effectiveAt)
      offset      <- readLedgerOffset(tx.offset)
    } yield {
      Model.Transaction(
        id          = ApiTypes.TransactionId(tx.transactionId),
        commandId   = if (tx.commandId.isEmpty) None else Some(ApiTypes.CommandId(tx.commandId)),
        effectiveAt = Instant.ofEpochSecond(effectiveAt.seconds, effectiveAt.nanos),
        offset      = offset,
        events      = events
      )
    }
  }

  private def readEvent(
    event: V1.event.Event,
    transactionId: ApiTypes.TransactionId,
    ctx: Context,
    parentWitnessParties: List[ApiTypes.Party],
    workflowId: ApiTypes.WorkflowId,
    parentId: Option[ApiTypes.EventId] = None
  ): Either[ConversionError, Model.Event] = {
    event match {
      case V1.event.Event(V1.event.Event.Event.Created(ev)) =>
        readEventCreated(ev, transactionId, parentWitnessParties, workflowId, parentId, ctx)

      case V1.event.Event(V1.event.Event.Event.Exercised(ev)) =>
        // This case should be removed from the protobuf, Transactions never contain Exercised events
        Left(GenericConversionError("Exercised event found in GetTransactions"))

      case V1.event.Event(V1.event.Event.Event.Archived(ev)) =>
        readEventArchived(ev, transactionId, parentWitnessParties, workflowId, parentId, ctx)

      case V1.event.Event(V1.event.Event.Event.Empty) =>
        Left(RequiredFieldDoesNotExistError("Event.value"))
    }
  }

  private def readEventArchived(
    event: V1.event.ArchivedEvent,
    transactionId: ApiTypes.TransactionId,
    parentWitnessParties: List[ApiTypes.Party],
    workflowId: ApiTypes.WorkflowId,
    parentId: Option[ApiTypes.EventId],
    ctx: Context
  ): Either[ConversionError, Model.Event] = {
    val witnessParties = parentWitnessParties ++ ApiTypes.Party.subst(event.witnessParties)
    Right(
      Model.ContractArchived(
        id             = ApiTypes.EventId(event.eventId),
        parentId       = parentId,
        transactionId  = transactionId,
        witnessParties = witnessParties,
        workflowId     = workflowId,
        contractId     = ApiTypes.ContractId(event.contractId)
      )
    )
  }
   */

  def readTransactionTree(
      tx: V1.transaction.TransactionTree,
      ctx: Context): Either[ConversionError, Model.Transaction] = {
    for {
      events <- Converter
        .sequence(
          tx.rootEventIds
            .map(
              evid =>
                readTreeEvent(
                  tx.eventsById(evid),
                  ApiTypes.TransactionId(tx.transactionId),
                  tx.eventsById,
                  ctx,
                  ApiTypes.WorkflowId(tx.workflowId),
                  None))
        )
        .map(_.flatten)
      effectiveAt <- Converter.checkExists("Transaction.effectiveAt", tx.effectiveAt)
      offset <- readLedgerOffset(tx.offset)
    } yield {
      Model.Transaction(
        id = ApiTypes.TransactionId(tx.transactionId),
        commandId = if (tx.commandId.isEmpty) None else Some(ApiTypes.CommandId(tx.commandId)),
        effectiveAt = Instant.ofEpochSecond(effectiveAt.seconds, effectiveAt.nanos.toLong),
        offset = offset,
        events = events
      )
    }
  }

  private def readTreeEvent(
      event: V1.transaction.TreeEvent,
      transactionId: ApiTypes.TransactionId,
      eventsById: Map[String, V1.transaction.TreeEvent],
      ctx: Context,
      workflowId: ApiTypes.WorkflowId,
      parentId: Option[ApiTypes.EventId] = None
  ): Either[ConversionError, List[Model.Event]] = {
    com.digitalasset.ledger.api.v1.event.Event.Event.Exercised
    event match {
      case V1.transaction.TreeEvent(V1.transaction.TreeEvent.Kind.Created(ev)) =>
        readEventCreated(ev, transactionId, workflowId, parentId, ctx).map(List(_))

      case V1.transaction.TreeEvent(V1.transaction.TreeEvent.Kind.Exercised(ev)) =>
        readEventExercised(ev, transactionId, eventsById, workflowId, parentId, ctx)

      case V1.transaction.TreeEvent(V1.transaction.TreeEvent.Kind.Empty) =>
        Left(RequiredFieldDoesNotExistError("TreeEvent.value"))
    }
  }

  private def getTemplate(
      id: Model.DamlLfIdentifier,
      ctx: Context): Either[ConversionError, Model.Template] =
    ctx.templates
      .template(id)
      .map(Right(_))
      .getOrElse(Left(TypeNotFoundError(id)))

  private def readLedgerOffset(offset: String): Either[ConversionError, String] = {
    // Ledger offset may change to become a number in the future
    // Try(BigInt(offset)).toEither
    //  .left.map(t => GenericConversionError(s"Could not parse ledger offset '$offset'"))
    Right(offset)
  }

  private def readEventCreated(
      event: V1.event.CreatedEvent,
      transactionId: ApiTypes.TransactionId,
      workflowId: ApiTypes.WorkflowId,
      parentId: Option[ApiTypes.EventId],
      ctx: Context
  ): Either[ConversionError, Model.Event] = {
    val witnessParties = ApiTypes.Party.subst(event.witnessParties.toList)
    for {
      templateId <- Converter.checkExists("CreatedEvent.templateId", event.templateId)
      templateIdentifier = templateId.asDaml
      template <- getTemplate(templateIdentifier, ctx)
      arguments <- Converter.checkExists("CreatedEvent.arguments", event.createArguments)
      arg <- readRecordArgument(arguments, templateIdentifier, ctx)
    } yield
      Model.ContractCreated(
        id = ApiTypes.EventId(event.eventId),
        parentId = parentId,
        transactionId = transactionId,
        witnessParties = witnessParties,
        workflowId = workflowId,
        contractId = ApiTypes.ContractId(event.contractId),
        templateId = templateIdentifier,
        argument = arg
      )
  }

  private def readEventExercised(
      event: V1.event.ExercisedEvent,
      transactionId: ApiTypes.TransactionId,
      eventsById: Map[String, V1.transaction.TreeEvent],
      workflowId: ApiTypes.WorkflowId,
      parentId: Option[ApiTypes.EventId],
      ctx: Context
  ): Either[ConversionError, List[Model.Event]] = {
    val witnessParties = ApiTypes.Party.subst(event.witnessParties.toList)
    for {
      templateId <- Converter.checkExists("ExercisedEvent.templateId", event.templateId)
      templateIdentifier = templateId.asDaml
      template <- getTemplate(templateId.asDaml, ctx)
      argument <- Converter.checkExists("ExercisedEvent.arguments", event.choiceArgument)
      choice <- Converter.checkExists(
        template.choices.find(c => ApiTypes.Choice.unwrap(c.name) == event.choice),
        GenericConversionError(s"Choice '${event.choice}' not found"))
      modelArgument <- readArgument(argument, choice.parameter, ctx)
      children <- Converter
        .sequence(
          event.childEventIds
            .map(
              childId =>
                readTreeEvent(
                  eventsById(childId),
                  transactionId,
                  eventsById,
                  ctx,
                  workflowId,
                  Some(ApiTypes.EventId(event.eventId))))
        )
        .map(_.flatten)
    } yield
      Model.ChoiceExercised(
        id = ApiTypes.EventId(event.eventId),
        parentId = parentId,
        transactionId = transactionId,
        witnessParties = witnessParties,
        workflowId = workflowId,
        contractId = ApiTypes.ContractId(event.contractId),
        contractCreateEvent = ApiTypes.EventId(event.contractCreatingEventId),
        templateId = templateIdentifier,
        choice = ApiTypes.Choice(event.choice),
        argument = modelArgument,
        consuming = event.consuming,
        actingParties = event.actingParties.map(ApiTypes.Party(_)).toList
      ) :: children
  }

  private def readRecordArgument(
      value: V1.value.Record,
      typId: Model.DamlLfIdentifier,
      ctx: Context): Either[ConversionError, Model.ApiRecord] =
    readRecordArgument(
      value,
      Model.DamlLfTypeCon(Model.DamlLfTypeConName(typId), Model.DamlLfImmArraySeq()),
      ctx)

  private def readRecordArgument(
      value: V1.value.Record,
      typ: Model.DamlLfType,
      ctx: Context): Either[ConversionError, Model.ApiRecord] = {
    for {
      typeCon <- typ match {
        case t @ Model.DamlLfTypeCon(_, _) => Right(t)
        case _ => Left(GenericConversionError(s"Cannot read $value as $typ"))
      }
      ddt <- ctx.templates
        .damlLfDefDataType(typeCon.name.identifier)
        .toRight(GenericConversionError(s"Unknown type ${typeCon.name.identifier}"))
      dt <- typeCon
        .instantiate(ddt)
        .fold(Right(_), _ => Left(GenericConversionError(s"Variant expected")))
      fields <- Converter.sequence(
        value.fields.toList
          .zip(dt.fields.toList)
          .map(
            p =>
              Converter
                .checkExists("RecordField.value", p._1.value)
                .flatMap(value => readArgument(value, p._2._2, ctx))
                .map(a => Model.ApiRecordField(p._2._1, a))))
    } yield Model.ApiRecord(Some(typeCon.name.identifier), fields)
  }

  private def readListArgument(
      list: V1.value.List,
      typ: Model.DamlLfType,
      ctx: Context): Either[ConversionError, Model.ApiList] = {
    for {
      elementType <- typ match {
        case Model.DamlLfTypePrim(Model.DamlLfPrimType.List, t) =>
          t.headOption.toRight(GenericConversionError("List type parameter missing"))
        case _ => Left(GenericConversionError(s"Cannot read $list as $typ"))
      }
      values <- Converter.sequence(
        list.elements.map(value => readArgument(value, elementType, ctx)))
    } yield {
      Model.ApiList(values)
    }
  }

  private def readOptionalArgument(
      opt: V1.value.Optional,
      typ: Model.DamlLfType,
      ctx: Context): Either[ConversionError, Model.ApiOptional] = {
    for {
      optType <- typ match {
        case Model.DamlLfTypePrim(Model.DamlLfPrimType.Optional, t) =>
          t.headOption.toRight(GenericConversionError("Optional type parameter missing"))
        case _ => Left(GenericConversionError(s"Cannot read $opt as $typ"))
      }
      value <- opt.value match {
        case None => Right(None)
        case Some(o) => readArgument(o, optType, ctx).map(Some(_))
      }
    } yield {
      Model.ApiOptional(value)
    }
  }

  private def readVariantArgument(
      variant: V1.value.Variant,
      typ: Model.DamlLfType,
      ctx: Context): Either[ConversionError, Model.ApiVariant] = {
    for {
      value <- Converter.checkExists("Variant.value", variant.value)
      typeCon <- typ match {
        case t @ Model.DamlLfTypeCon(_, _) => Right(t)
        case _ => Left(GenericConversionError(s"Cannot read $variant as $typ"))
      }
      ddt <- ctx.templates
        .damlLfDefDataType(typeCon.name.identifier)
        .toRight(GenericConversionError(s"Unknown type ${typeCon.name.identifier}"))
      dt <- typeCon
        .instantiate(ddt)
        .fold(_ => Left(GenericConversionError(s"Variant expected")), Right(_))
      choice <- dt.fields
        .find(f => f._1 == variant.constructor)
        .toRight(GenericConversionError(s"Unknown choice ${variant.constructor}"))
      argument <- readArgument(value, choice._2, ctx)
    } yield {
      Model.ApiVariant(Some(typeCon.name.identifier), variant.constructor, argument)
    }
  }

  private def readArgument(
      value: V1.value.Value,
      typ: Model.DamlLfType,
      ctx: Context): Either[ConversionError, Model.ApiValue] = {
    import V1.value.Value.{Sum => VS}
    (value.sum, typ) match {
      case (VS.Int64(v), _) => Right(Model.ApiInt64(v))
      case (VS.Decimal(v), _) => Right(Model.ApiDecimal(v))
      case (VS.Text(v), _) => Right(Model.ApiText(v))
      case (VS.Unit(v), _) => Right(Model.ApiUnit())
      case (VS.Bool(v), _) => Right(Model.ApiBool(v))
      case (VS.Party(v), _) => Right(Model.ApiParty(v))
      case (VS.Timestamp(v), _) => Right(Model.ApiTimestamp(v))
      case (VS.Date(v), _) => Right(Model.ApiDate(v))
      case (VS.ContractId(v), _) => Right(Model.ApiContractId(v))
      case (VS.Optional(v), t) => readOptionalArgument(v, t, ctx)
      case (VS.List(v), t) => readListArgument(v, t, ctx)
      case (VS.Record(v), t) => readRecordArgument(v, t, ctx)
      case (VS.Variant(v), t) => readVariantArgument(v, t, ctx)
      case (VS.Empty, _) => Left(GenericConversionError("Argument value is empty"))
      case (_, _) => Left(GenericConversionError(s"Cannot read argument $value as $typ"))
    }
  }

  def readCompletion(completion: V1.completion.Completion)
    : Either[ConversionError, Option[Model.CommandStatus]] = {
    for {
      status <- Converter.checkExists("Completion.status", completion.status)
    } yield {
      val code = Code.fromValue(status.code)

      if (code == Code.OK)
        // The completion does not contain the new transaction created by this command.
        // Do not report completion, the command result will be updated from the transaction stream.
        None
      else
        Some(Model.CommandStatusError(code.toString(), status.message))
    }
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Write methods (Model -> V1)
  // ------------------------------------------------------------------------------------------------------------------

  def writeArgument(value: Model.ApiValue): Either[ConversionError, V1.value.Value] = {
    import V1.value.Value
    value match {
      case arg: Model.ApiRecord => writeRecordArgument(arg).map(a => Value(Value.Sum.Record(a)))
      case arg: Model.ApiVariant => writeVariantArgument(arg).map(a => Value(Value.Sum.Variant(a)))
      case arg: Model.ApiList => writeListArgument(arg).map(a => Value(Value.Sum.List(a)))
      case Model.ApiBool(v) => Right(Value(Value.Sum.Bool(v)))
      case Model.ApiInt64(v) => Right(Value(Value.Sum.Int64(v)))
      case Model.ApiDecimal(v) => Right(Value(Value.Sum.Decimal(v)))
      case Model.ApiParty(v) => Right(Value(Value.Sum.Party(v)))
      case Model.ApiText(v) => Right(Value(Value.Sum.Text(v)))
      case Model.ApiTimestamp(v) => Right(Value(Value.Sum.Timestamp(v)))
      case Model.ApiDate(v) => Right(Value(Value.Sum.Date(v)))
      case Model.ApiContractId(v) => Right(Value(Value.Sum.ContractId(v)))
      case Model.ApiUnit() => Right(Value(Value.Sum.Unit(com.google.protobuf.empty.Empty())))
      case Model.ApiOptional(None) => Right(Value(Value.Sum.Optional(V1.value.Optional(None))))
      case Model.ApiOptional(Some(v)) =>
        writeArgument(v).map(a => Value(Value.Sum.Optional(V1.value.Optional(Some(a)))))
      case arg: Model.ApiMap =>
        writeMapArgument(arg).map(a => Value(Value.Sum.Map(a)))
    }
  }

  def writeRecordArgument(value: Model.ApiRecord): Either[ConversionError, V1.value.Record] = {
    for {
      fields <- Converter
        .sequence(value.fields.map(f =>
          writeArgument(f.value).map(v => V1.value.RecordField(f.label, Some(v)))))
    } yield {
      V1.value.Record(value.recordId.map(_.asApi), fields)
    }
  }

  def writeVariantArgument(value: Model.ApiVariant): Either[ConversionError, V1.value.Variant] = {
    for {
      arg <- writeArgument(value.value)
    } yield {
      V1.value.Variant(value.variantId.map(_.asApi), value.constructor, Some(arg))
    }
  }

  def writeListArgument(value: Model.ApiList): Either[ConversionError, V1.value.List] = {
    for {
      values <- Converter.sequence(value.elements.map(e => writeArgument(e)))
    } yield {
      V1.value.List(values)
    }
  }


  def writeMapArgument(value: Model.ApiMap): Either[ConversionError, V1.value.Map] = {
    for {
      values <- Converter.sequence(
        value.value.toSeq.map{case (k, v) => writeArgument(v).map(k -> _)}
      )
    } yield {
      V1.value.Map(values.map{case (k, v) =>
        V1.value.Map.Entry(
          Some(V1.value.Value(V1.value.Value.Sum.Text(k))),
          Some(v)
        )})
    }
  }


  /** Write a composite command consisting of just the given command */
  def writeCommands(
      party: Model.PartyState,
      command: Model.Command,
      maxRecordDelay: Long,
      ledgerId: String,
      applicationId: String
  ): Either[ConversionError, V1.commands.Commands] = {
    for {
      ledgerCommand <- writeCommand(party, command)
    } yield {
      val ledgerEffectiveTime =
        new Timestamp(command.platformTime.getEpochSecond, command.platformTime.getNano)
      val maximumRecordTime =
        ledgerEffectiveTime.copy(seconds = ledgerEffectiveTime.seconds + maxRecordDelay)
      V1.commands.Commands(
        ledgerId,
        Tag.unwrap(command.workflowId),
        applicationId,
        Tag.unwrap(command.id),
        Tag.unwrap(party.name),
        Some(ledgerEffectiveTime),
        Some(maximumRecordTime),
        List(ledgerCommand)
      )
    }
  }

  def writeCommand(
      party: Model.PartyState,
      command: Model.Command
  ): Either[ConversionError, V1.commands.Command] = {
    command match {
      case cmd: Model.CreateCommand => writeCreateContract(party, cmd.template, cmd.argument)
      case cmd: Model.ExerciseCommand =>
        writeExerciseChoice(party, cmd.contract, cmd.choice, cmd.argument)
    }
  }

  def writeCreateContract(
      party: Model.PartyState,
      templateId: Model.DamlLfIdentifier,
      value: Model.ApiRecord
  ): Either[ConversionError, V1.commands.Command] = {
    for {
      template <- Converter.checkExists(
        party.packageRegistry.template(templateId),
        GenericConversionError(s"Template '$templateId' not found"))
      argument <- writeRecordArgument(value)
    } yield {
      V1.commands.Command(
        V1.commands.Command.Command.Create(
          V1.commands.CreateCommand(
            Some(template.id.asApi),
            Some(argument)
          )
        )
      )
    }
  }

  def writeExerciseChoice(
      party: Model.PartyState,
      contractId: ApiTypes.ContractId,
      choiceId: ApiTypes.Choice,
      value: Model.ApiValue
  ): Either[ConversionError, V1.commands.Command] = {
    for {
      contract <- Converter.checkExists(
        party.ledger.contract(contractId, party.packageRegistry),
        GenericConversionError(s"Contract '${Tag.unwrap(contractId)}' not found"))
      choice <- Converter.checkExists(
        contract.template.choices.find(c => c.name == choiceId),
        GenericConversionError(s"Choice '${Tag.unwrap(choiceId)}' not found"))
      argument <- writeArgument(value)
    } yield {
      V1.commands.Command(
        V1.commands.Command.Command.Exercise(
          V1.commands.ExerciseCommand(
            Some(contract.template.id.asApi),
            Tag.unwrap(contractId),
            Tag.unwrap(choiceId),
            Some(argument)
          )
        )
      )
    }
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------------------------------------

  private def epochMicrosToString(time: Long): Either[ConversionError, String] = {
    val micro: Long = 1000000
    val seconds: Long = time / micro
    val nanos: Long = (time % micro) * 1000
    (for {
      instant <- Try(Instant.ofEpochSecond(seconds, nanos)).toEither
      result <- Try(DateTimeFormatter.ISO_INSTANT.format(instant)).toEither
    } yield {
      result
    }).left.map(e => GenericConversionError(s"Could not convert timestamp '$time' to a string"))
  }

  private def stringToEpochMicros(time: String): Either[ConversionError, Long] = {
    (for {
      ta <- Try(DateTimeFormatter.ISO_INSTANT.parse(time)).toEither
      instant <- Try(Instant.from(ta)).toEither
    } yield {
      val micro: Long = 1000000
      instant.getEpochSecond * micro + instant.getNano / 1000
    }).left.map(e => GenericConversionError(s"Could not convert string '$time' to a TimeStamp: $e"))
  }

  private def epochDaysToString(time: Int): Either[ConversionError, String] = {
    (for {
      ta <- Try(LocalDate.ofEpochDay(time.toLong)).toEither
      result <- Try(DateTimeFormatter.ISO_LOCAL_DATE.format(ta)).toEither
    } yield {
      result
    }).left.map(e => GenericConversionError(s"Could not convert date '$time' to a Date: $e"))
  }

  private def stringToEpochDays(time: String): Either[ConversionError, Int] = {
    (for {
      ta <- Try(DateTimeFormatter.ISO_INSTANT.parse(time)).toEither
      instant <- Try(Instant.from(ta)).toEither
    } yield {
      val epoch = Instant.EPOCH
      epoch.until(instant, ChronoUnit.DAYS).toInt
    }).left.map(e => GenericConversionError(s"Could not convert string '$time' to a Date: $e"))
  }

}
