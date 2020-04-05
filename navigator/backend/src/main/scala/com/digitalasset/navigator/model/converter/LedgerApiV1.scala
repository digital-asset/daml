// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model.converter

import java.time.Instant

import com.daml.lf.data.Ref
import com.daml.lf.data.LawlessTraversals._
import com.daml.lf.iface
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.lf.value.{Value => V}
import com.daml.ledger.api.{v1 => V1}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.validation.ValueValidator.{validateRecord, validateValue}
import com.daml.navigator.{model => Model}
import com.daml.navigator.model.{IdentifierApiConversions, IdentifierDamlConversions}
import com.daml.platform.participant.util.LfEngineToApi.{lfValueToApiRecord, lfValueToApiValue}

import com.google.rpc.code.Code
import scalaz.Tag
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.std.either._
import scalaz.std.option._

import scala.util.control.NoStackTrace

@SuppressWarnings(Array("org.wartremover.warts.Any"))
case object LedgerApiV1 {
  // ------------------------------------------------------------------------------------------------------------------
  // Types
  // ------------------------------------------------------------------------------------------------------------------
  case class Context(party: ApiTypes.Party, templates: Model.PackageRegistry)

  private type Result[X] = Either[ConversionError, X]

  // ------------------------------------------------------------------------------------------------------------------
  // Read methods (V1 -> Model)
  // ------------------------------------------------------------------------------------------------------------------
  /*
  def readTransaction(tx: V1.transaction.Transaction, ctx: Context): Result[Model.Transaction] = {
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
  ): Result[Model.Event] = {
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
  ): Result[Model.Event] = {
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
      ctx: Context
  ): Result[Model.Transaction] = {
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
  ): Result[List[Model.Event]] = {
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
      ctx: Context
  ): Result[Model.Template] =
    ctx.templates
      .template(id)
      .map(Right(_))
      .getOrElse(Left(TypeNotFoundError(id)))

  private def readLedgerOffset(offset: String): Result[String] = {
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
  ): Result[Model.Event] = {
    val witnessParties = ApiTypes.Party.subst(event.witnessParties.toList)
    val signatories = ApiTypes.Party.subst(event.signatories.toList)
    val observers = ApiTypes.Party.subst(event.observers.toList)
    for {
      templateId <- Converter.checkExists("CreatedEvent.templateId", event.templateId)
      templateIdentifier = templateId.asDaml
      template <- getTemplate(templateIdentifier, ctx)
      arguments <- Converter.checkExists("CreatedEvent.arguments", event.createArguments)
      arg <- readRecordArgument(arguments, templateIdentifier, ctx)
      keyResult = event.contractKey
        .traverseU(k => readArgument(k, template.key.get, ctx))
      key <- keyResult
    } yield
      Model.ContractCreated(
        id = ApiTypes.EventId(event.eventId),
        parentId = parentId,
        transactionId = transactionId,
        witnessParties = witnessParties,
        workflowId = workflowId,
        contractId = ApiTypes.ContractId(event.contractId),
        templateId = templateIdentifier,
        argument = arg,
        agreementText = event.agreementText,
        signatories = signatories,
        observers = observers,
        key = key
      )
  }

  private def readEventExercised(
      event: V1.event.ExercisedEvent,
      transactionId: ApiTypes.TransactionId,
      eventsById: Map[String, V1.transaction.TreeEvent],
      workflowId: ApiTypes.WorkflowId,
      parentId: Option[ApiTypes.EventId],
      ctx: Context
  ): Result[List[Model.Event]] = {
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
      ctx: Context
  ): Result[Model.ApiRecord] =
    for {
      lfr <- validateRecord(value).leftMap(sre => GenericConversionError(sre.getMessage))
      cidMapped <- lfr mapContractId (_.coid) match {
        case r: Model.ApiRecord => Right(r)
        case v => Left(GenericConversionError(s"validating record produced non-record $v"))
      }
      filled <- fillInRecordTI(
        cidMapped,
        Model.DamlLfTypeCon(Model.DamlLfTypeConName(typId), Model.DamlLfImmArraySeq()),
        ctx)
    } yield filled

  private def fillInRecordTI(
      value: Model.ApiRecord,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiRecord] =
    for {
      typeCon <- asTypeCon(typ, value)
      ddt <- ctx.templates
        .damlLfDefDataType(typeCon.name.identifier)
        .toRight(GenericConversionError(s"Unknown type ${typeCon.name.identifier}"))
      dt <- typeCon.instantiate(ddt) match {
        case r @ iface.Record(_) => Right(r)
        case iface.Variant(_) | iface.Enum(_) => Left(GenericConversionError(s"Record expected"))
      }
      fields <- value.fields.toSeq zip dt.fields traverseEitherStrictly {
        case ((von, vv), (tn, fieldType)) =>
          for {
            _ <- von.cata(
              vn =>
                Either.cond(
                  (vn: String) == (tn: String),
                  (),
                  GenericConversionError(s"field order mismatch: expected $tn, got $vn")),
              Right(()))
            newVv <- fillInTypeInfo(vv, fieldType, ctx)
          } yield (Some(tn), newVv)
      }
    } yield V.ValueRecord(Some(typeCon.name.identifier), fields.toImmArray)

  private def fillInListTI(
      list: Model.ApiList,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiList] =
    for {
      elementType <- typ match {
        case Model.DamlLfTypePrim(Model.DamlLfPrimType.List, Seq(t)) =>
          Right(t)
        case _ => Left(GenericConversionError(s"Cannot read $list as $typ"))
      }
      values <- list.values traverseU (fillInTypeInfo(_, elementType, ctx))
    } yield V.ValueList(values)

  private def fillInTextMapTI(
      textMap: Model.ApiMap,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiMap] =
    for {
      elementType <- typ match {
        case Model.DamlLfTypePrim(Model.DamlLfPrimType.TextMap, Seq(t)) =>
          Right(t)
        case _ => Left(GenericConversionError(s"Cannot read $textMap as $typ"))
      }
      values <- textMap.value traverseU (fillInTypeInfo(_, elementType, ctx))
    } yield V.ValueTextMap(values)

  private def fillInGenMapTI(
      genMap: Model.ApiGenMap,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiGenMap] =
    for {
      types <- typ match {
        case Model.DamlLfTypePrim(Model.DamlLfPrimType.GenMap, Seq(kT, vT)) =>
          Right((kT, vT))
        case _ => Left(GenericConversionError(s"Cannot read $genMap as $typ"))
      }
      (keyType, valueType) = types
      values <- genMap.entries.toSeq traverseU {
        case (k, v) =>
          for {
            key <- fillInTypeInfo(k, keyType, ctx)
            value <- fillInTypeInfo(v, valueType, ctx)
          } yield key -> value
      }
    } yield V.ValueGenMap(values.toImmArray)

  private def fillInOptionalTI(
      opt: Model.ApiOptional,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiOptional] =
    for {
      optType <- typ match {
        case Model.DamlLfTypePrim(Model.DamlLfPrimType.Optional, Seq(t)) =>
          Right(t)
        case _ => Left(GenericConversionError(s"Cannot read $opt as $typ"))
      }
      value <- opt.value traverseU (fillInTypeInfo(_, optType, ctx))
    } yield V.ValueOptional(value)

  private def asTypeCon(
      typ: Model.DamlLfType,
      selector: Model.ApiValue): Result[Model.DamlLfTypeCon] =
    typ match {
      case t @ Model.DamlLfTypeCon(_, _) => Right(t)
      case _ => Left(GenericConversionError(s"Cannot read $selector as $typ"))
    }

  private def fillInVariantTI(
      variant: Model.ApiVariant,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiVariant] =
    for {
      typeCon <- asTypeCon(typ, variant)
      ddt <- ctx.templates
        .damlLfDefDataType(typeCon.name.identifier)
        .toRight(GenericConversionError(s"Unknown type ${typeCon.name.identifier}"))
      dt <- typeCon.instantiate(ddt) match {
        case v @ iface.Variant(_) => Right(v)
        case iface.Record(_) | iface.Enum(_) =>
          Left(GenericConversionError(s"Variant expected"))
      }
      constructor = variant.variant
      choice <- dt.fields
        .collectFirst { case (`constructor`, cargTyp) => cargTyp }
        .toRight(GenericConversionError(s"Unknown enum constructor $constructor"))
      value = variant.value
      argument <- fillInTypeInfo(value, choice, ctx)
    } yield variant.copy(tycon = Some(typeCon.name.identifier), value = argument)

  private def fillInEnumTI(
      enum: V.ValueEnum,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[V.ValueEnum] =
    for {
      typeCon <- asTypeCon(typ, enum)
    } yield enum.copy(tycon = Some(typeCon.name.identifier))

  private def readArgument(
      value: V1.value.Value,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiValue] =
    validateValue(value)
      .leftMap(sre => GenericConversionError(sre.getMessage))
      .flatMap(vv => fillInTypeInfo(vv.mapContractId(_.coid), typ, ctx))

  /** Add `tycon`s and record field names where absent. */
  private def fillInTypeInfo(
      value: Model.ApiValue,
      typ: Model.DamlLfType,
      ctx: Context
  ): Result[Model.ApiValue] =
    value match {
      case v: V.ValueEnum => fillInEnumTI(v, typ, ctx)
      case _: V.ValueCidlessLeaf | _: V.ValueContractId[_] => Right(value)
      case v: Model.ApiOptional => fillInOptionalTI(v, typ, ctx)
      case v: Model.ApiMap => fillInTextMapTI(v, typ, ctx)
      case v: Model.ApiGenMap => fillInGenMapTI(v, typ, ctx)
      case v: Model.ApiList => fillInListTI(v, typ, ctx)
      case v: Model.ApiRecord => fillInRecordTI(v, typ, ctx)
      case v: Model.ApiVariant => fillInVariantTI(v, typ, ctx)
      case _: Model.ApiImpossible =>
        Left(GenericConversionError("unserializable Struct appeared of serializable type"))
    }

  def readCompletion(completion: V1.completion.Completion): Result[Option[Model.CommandStatus]] = {
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

  def writeArgument(value: Model.ApiValue): Result[V1.value.Value] =
    wrapAbsContractId(value) flatMap (vac =>
      lfValueToApiValue(verbose = true, vac) leftMap GenericConversionError)

  def writeRecordArgument(value: Model.ApiRecord): Result[V1.value.Record] =
    wrapAbsContractId(value) flatMap (vac =>
      lfValueToApiRecord(verbose = true, vac) leftMap GenericConversionError)

  private[this] def wrapAbsContractId(value: Model.ApiValue): Result[V[V.AbsoluteContractId]] = {
    final class NotACoid(message: String) extends RuntimeException(message) with NoStackTrace
    // this is 100% cheating as Value should have Traverse instead
    try Right(value mapContractId { coid =>
      AbsoluteContractId fromString coid fold (
        e => throw new NotACoid(e),
        identity
      )
    })
    catch { case e: NotACoid => Left(GenericConversionError(e.getMessage)) }
  }

  /** Write a composite command consisting of just the given command */
  def writeCommands(
      party: Model.PartyState,
      command: Model.Command,
      ledgerId: String,
      applicationId: Ref.LedgerString
  ): Result[V1.commands.Commands] = {
    for {
      ledgerCommand <- writeCommand(party, command)
    } yield {
      V1.commands.Commands(
        ledgerId,
        Tag.unwrap(command.workflowId),
        applicationId,
        Tag.unwrap(command.id),
        Tag.unwrap(party.name),
        List(ledgerCommand)
      )
    }
  }

  def writeCommand(
      party: Model.PartyState,
      command: Model.Command
  ): Result[V1.commands.Command] = {
    command match {
      case cmd: Model.CreateCommand =>
        writeCreateContract(party, cmd.template, cmd.argument)
      case cmd: Model.ExerciseCommand =>
        writeExerciseChoice(party, cmd.contract, cmd.choice, cmd.argument)
    }
  }

  def writeCreateContract(
      party: Model.PartyState,
      templateId: Model.DamlLfIdentifier,
      value: Model.ApiRecord
  ): Result[V1.commands.Command] = {
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
  ): Result[V1.commands.Command] = {
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
}
