// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger

import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import com.daml.lf.data.{BackStack, FrontStack, ImmArray, Ref}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.api.v1.commands.{
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
  Command => ApiCommand,
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, InterfaceView}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.lf.language.StablePackagesV1
import com.daml.lf.speedy.Command
import com.daml.lf.value.Value
import com.daml.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier,
}
import com.daml.script.converter.ConverterException
import com.daml.script.converter.Converter._
import com.daml.script.converter.Converter.Implicits._

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MICROSECONDS}

// Convert from a Ledger API transaction to an SValue corresponding to a Message from the Daml.Trigger module
final class Converter(
    compiledPackages: CompiledPackages,
    triggerDef: TriggerDefinition,
) {

  import Converter._

  private[this] val valueTranslator = new preprocessing.ValueTranslator(
    compiledPackages.pkgInterface,
    requireV1ContractIdSuffix = false,
  )

  private[this] def validateRecord(r: value.Record): Either[String, Value.ValueRecord] =
    NoLoggingValueValidator.validateRecord(r).left.map(_.getMessage)

  private[this] def translateValue(ty: Type, value: Value): Either[String, SValue] =
    valueTranslator
      .strictTranslateValue(ty, value)
      .left
      .map(res => s"Failure to translate value: $res")

  private[this] val triggerIds: TriggerIds = triggerDef.triggerIds

  // TODO(#17366): support both LF v1 and v2 in triggers
  private[this] val templateTypeRepTyCon = StablePackagesV1.TemplateTypeRep
  private[this] val anyTemplateTyCon = StablePackagesV1.AnyTemplate
  private[this] val anyViewTyCon = StablePackagesV1.AnyView
  private[this] val activeContractsTy = triggerIds.damlTriggerLowLevel("ActiveContracts")
  private[this] val triggerConfigTy = triggerIds.damlTriggerLowLevel("TriggerConfig")
  private[this] val triggerSetupArgumentsTy =
    triggerIds.damlTriggerLowLevel("TriggerSetupArguments")
  private[this] val triggerStateTy = triggerIds.damlTriggerInternal("TriggerState")
  private[this] val anyContractIdTy = triggerIds.damlTriggerLowLevel("AnyContractId")
  private[this] val archivedTy = triggerIds.damlTriggerLowLevel("Archived")
  private[this] val commandIdTy = triggerIds.damlTriggerLowLevel("CommandId")
  private[this] val completionTy = triggerIds.damlTriggerLowLevel("Completion")
  private[this] val createdTy = triggerIds.damlTriggerLowLevel("Created")
  private[this] val eventIdTy = triggerIds.damlTriggerLowLevel("EventId")
  private[this] val eventTy = triggerIds.damlTriggerLowLevel("Event")
  private[this] val failedTy = triggerIds.damlTriggerLowLevel("CompletionStatus.Failed")
  private[this] val messageTy = triggerIds.damlTriggerLowLevel("Message")
  private[this] val succeedTy = triggerIds.damlTriggerLowLevel("CompletionStatus.Succeeded")
  private[this] val transactionIdTy = triggerIds.damlTriggerLowLevel("TransactionId")
  private[this] val transactionTy = triggerIds.damlTriggerLowLevel("Transaction")
  private[this] val createCommandTy = triggerIds.damlTriggerLowLevel("CreateCommand")
  private[this] val exerciseCommandTy = triggerIds.damlTriggerLowLevel("ExerciseCommand")
  private[this] val createAndExerciseCommandTy =
    triggerIds.damlTriggerLowLevel("CreateAndExerciseCommand")
  private[this] val exerciseByKeyCommandTy = triggerIds.damlTriggerLowLevel("ExerciseByKeyCommand")
  private[this] val acsTy = triggerIds.damlTriggerInternal("ACS")

  private[this] def fromTemplateTypeRep(tyCon: value.Identifier): SValue =
    record(
      templateTypeRepTyCon,
      "getTemplateTypeRep" -> STypeRep(
        TTyCon(
          TypeConName(
            PackageId.assertFromString(tyCon.packageId),
            QualifiedName(
              DottedName.assertFromString(tyCon.moduleName),
              DottedName.assertFromString(tyCon.entityName),
            ),
          )
        )
      ),
    )

  private[this] def fromTransactionId(transactionId: String): SValue =
    record(transactionIdTy, "unpack" -> SText(transactionId))

  private[this] def fromEventId(eventId: String): SValue =
    record(eventIdTy, "unpack" -> SText(eventId))

  private[trigger] def fromCommandId(commandId: String): SValue =
    record(commandIdTy, "unpack" -> SText(commandId))

  private[this] def fromOptionalCommandId(commandId: String): SValue =
    if (commandId.isEmpty)
      SOptional(None)
    else
      SOptional(Some(fromCommandId(commandId)))

  private[this] def fromAnyContractId(
      templateId: value.Identifier,
      contractId: String,
  ): SValue =
    record(
      anyContractIdTy,
      "templateId" -> fromTemplateTypeRep(templateId),
      "contractId" -> SContractId(ContractId.assertFromString(contractId)),
    )

  private def fromArchivedEvent(archived: ArchivedEvent): SValue =
    record(
      archivedTy,
      "eventId" -> fromEventId(archived.eventId),
      "contractId" -> fromAnyContractId(archived.getTemplateId, archived.contractId),
    )

  private[this] def fromRecord(typ: Type, record: value.Record): Either[String, SValue] =
    for {
      record <- validateRecord(record)
      tmplPayload <- translateValue(typ, record)
    } yield tmplPayload

  private[this] def fromAnyTemplate(typ: Type, value: SValue) =
    record(anyTemplateTyCon, "getAnyTemplate" -> SAny(typ, value))

  private[this] def fromV20CreatedEvent(
      created: CreatedEvent
  ): Either[String, SValue] =
    for {
      tmplId <- fromIdentifier(created.getTemplateId)
      tmplType = TTyCon(tmplId)
      tmplPayload <- fromRecord(tmplType, created.getCreateArguments)
    } yield {
      record(
        createdTy,
        "eventId" -> fromEventId(created.eventId),
        "contractId" -> fromAnyContractId(created.getTemplateId, created.contractId),
        "argument" -> fromAnyTemplate(tmplType, tmplPayload),
      )
    }

  private[this] def fromAnyView(typ: Type, value: SValue) =
    record(anyViewTyCon, "getAnyView" -> SAny(typ, value))

  private[this] def fromInterfaceView(view: InterfaceView): Either[String, SOptional] =
    for {
      ifaceId <- fromIdentifier(view.getInterfaceId)
      iface <- compiledPackages.pkgInterface.lookupInterface(ifaceId).left.map(_.pretty)
      viewType = iface.view
      viewValue <- view.viewValue.traverseU(fromRecord(viewType, _))
    } yield SOptional(viewValue.map(fromAnyView(viewType, _)))

  private[this] def fromV250CreatedEvent(
      created: CreatedEvent
  ): Either[String, SValue] =
    for {
      tmplId <- fromIdentifier(created.getTemplateId)
      tmplType = TTyCon(tmplId)
      tmplPayload <- created.createArguments.traverseU(fromRecord(tmplType, _))
      views <- created.interfaceViews.toList.traverseU(fromInterfaceView)
    } yield {
      record(
        createdTy,
        "eventId" -> fromEventId(created.eventId),
        "contractId" -> fromAnyContractId(created.getTemplateId, created.contractId),
        "argument" -> SOptional(tmplPayload.map(fromAnyTemplate(tmplType, _))),
        "views" -> SList(views.to(FrontStack)),
      )
    }

  private[this] val fromCreatedEvent: CreatedEvent => Either[String, SValue] =
    if (triggerDef.version < Trigger.Version.`2.5.0`) {
      fromV20CreatedEvent
    } else {
      fromV250CreatedEvent
    }

  private def fromEvent(ev: Event): Either[String, SValue] =
    ev.event match {
      case Event.Event.Archived(archivedEvent) =>
        Right(
          SVariant(
            id = eventTy,
            variant = EventVariant.ArchiveEventConstructor,
            constructorRank = EventVariant.ArchiveEventConstructorRank,
            value = fromArchivedEvent(archivedEvent),
          )
        )
      case Event.Event.Created(createdEvent) =>
        for {
          event <- fromCreatedEvent(createdEvent)
        } yield SVariant(
          id = eventTy,
          variant = EventVariant.CreatedEventConstructor,
          constructorRank = EventVariant.CreatedEventConstructorRank,
          value = event,
        )
      case _ => Left(s"Expected Archived or Created but got ${ev.event}")
    }

  def fromTransaction(t: Transaction): Either[String, SValue] =
    for {
      events <- t.events.to(ImmArray).traverse(fromEvent).map(xs => SList(FrontStack.from(xs)))
      transactionId = fromTransactionId(t.transactionId)
      commandId = fromOptionalCommandId(t.commandId)
    } yield SVariant(
      id = messageTy,
      variant = MessageVariant.MTransactionVariant,
      constructorRank = MessageVariant.MTransactionVariantRank,
      value = record(
        transactionTy,
        "transactionId" -> transactionId,
        "commandId" -> commandId,
        "events" -> events,
      ),
    )

  def fromCompletion(c: Completion): Either[String, SValue] = {
    val status: SValue =
      if (c.getStatus.code == 0)
        SVariant(
          triggerIds.damlTriggerLowLevel("CompletionStatus"),
          CompletionStatusVariant.SucceedVariantConstructor,
          CompletionStatusVariant.SucceedVariantConstructorRank,
          record(
            succeedTy,
            "transactionId" -> fromTransactionId(c.transactionId),
          ),
        )
      else
        SVariant(
          triggerIds.damlTriggerLowLevel("CompletionStatus"),
          CompletionStatusVariant.FailVariantConstructor,
          CompletionStatusVariant.FailVariantConstructorRank,
          record(
            failedTy,
            "status" -> SInt64(c.getStatus.code.asInstanceOf[Long]),
            "message" -> SText(c.getStatus.message),
          ),
        )
    Right(
      SVariant(
        messageTy,
        MessageVariant.MCompletionConstructor,
        MessageVariant.MCompletionConstructorRank,
        record(
          completionTy,
          "commandId" -> fromCommandId(c.commandId),
          "status" -> status,
        ),
      )
    )
  }

  def fromHeartbeat: SValue =
    SVariant(
      messageTy,
      MessageVariant.MHeartbeatConstructor,
      MessageVariant.MHeartbeatConstructorRank,
      SUnit,
    )

  def fromActiveContracts(createdEvents: Seq[CreatedEvent]): Either[String, SValue] =
    for {
      events <- createdEvents
        .to(ImmArray)
        .traverse(fromCreatedEvent)
        .map(xs => SList(FrontStack.from(xs)))
    } yield record(activeContractsTy, "activeContracts" -> events)

  private[this] def fromTriggerConfig(triggerConfig: TriggerRunnerConfig): SValue =
    record(
      triggerConfigTy,
      "maxInFlightCommands" -> SValue.SInt64(triggerConfig.inFlightCommandBackPressureCount),
      "maxActiveContracts" -> SValue.SInt64(triggerConfig.hardLimit.maximumActiveContracts),
    )

  def fromTriggerSetupArguments(
      parties: TriggerParties,
      createdEvents: Seq[CreatedEvent],
      triggerConfig: TriggerRunnerConfig,
  ): Either[String, SValue] =
    for {
      acs <- fromActiveContracts(createdEvents)
      actAs = SParty(Ref.Party.assertFromString(parties.actAs.unwrap))
      readAs = SList(
        parties.readAs.map(p => SParty(Ref.Party.assertFromString(p.unwrap))).to(FrontStack)
      )
      config = fromTriggerConfig(triggerConfig)
    } yield record(
      triggerSetupArgumentsTy,
      "actAs" -> actAs,
      "readAs" -> readAs,
      "acs" -> acs,
      "config" -> config,
    )

  def fromCommand(command: Command): SValue = command match {
    case Command.Create(templateId, argument) =>
      record(
        createCommandTy,
        "templateArg" -> fromAnyTemplate(
          TTyCon(templateId),
          argument,
        ),
      )

    case Command.ExerciseTemplate(_, contractId, _, choiceArg) =>
      record(
        exerciseCommandTy,
        "contractId" -> contractId,
        "choiceArg" -> choiceArg,
      )

    case Command.ExerciseInterface(_, contractId, _, choiceArg) =>
      record(
        exerciseCommandTy,
        "contractId" -> contractId,
        "choiceArg" -> choiceArg,
      )

    case Command.CreateAndExercise(templateId, createArg, _, choiceArg) =>
      record(
        createAndExerciseCommandTy,
        "templateArg" -> fromAnyTemplate(TTyCon(templateId), createArg),
        "choiceArg" -> choiceArg,
      )

    case Command.ExerciseByKey(templateId, contractKey, _, choiceArg) =>
      record(
        exerciseByKeyCommandTy,
        "tplTypeRep" -> SValue.STypeRep(TTyCon(templateId)),
        "contractKey" -> contractKey,
        "choiceArg" -> choiceArg,
      )

    case _ =>
      throw new ConverterException(
        s"${command.getClass.getSimpleName} is an unexpected command type"
      )
  }

  def fromCommands(commands: Seq[Command]): SValue = {
    SList(commands.map(fromCommand).to(FrontStack))
  }

  def fromACS(activeContracts: Seq[CreatedEvent]): SValue = {
    val createMapByTemplateId = mutable.HashMap.empty[value.Identifier, BackStack[CreatedEvent]]
    for (create <- activeContracts) {
      createMapByTemplateId += (create.getTemplateId -> (createMapByTemplateId.getOrElse(
        create.getTemplateId,
        BackStack.empty,
      ) :+ create))
    }

    record(
      acsTy,
      "activeContracts" -> SMap(
        isTextMap = false,
        createMapByTemplateId.iterator.map { case (templateId, creates) =>
          fromTemplateTypeRep(templateId) -> SMap(
            isTextMap = false,
            creates.reverseIterator.map { event =>
              val templateType = TTyCon(fromIdentifier(templateId).orConverterException)
              val template = fromAnyTemplate(
                templateType,
                fromRecord(templateType, event.getCreateArguments).orConverterException,
              )

              fromAnyContractId(templateId, event.contractId) -> template
            },
          )
        },
      ),
      "pendingContracts" -> SMap(isTextMap = false),
    )
  }

  def fromTriggerUpdateState(
      createdEvents: Seq[CreatedEvent],
      userState: SValue,
      commandsInFlight: Map[String, Seq[Command]] = Map.empty,
      parties: TriggerParties,
      triggerConfig: TriggerRunnerConfig,
  ): SValue = {
    val acs = fromACS(createdEvents)
    val actAs = SParty(Ref.Party.assertFromString(parties.actAs.unwrap))
    val readAs = SList(
      parties.readAs.map(p => SParty(Ref.Party.assertFromString(p.unwrap))).to(FrontStack)
    )
    val config = fromTriggerConfig(triggerConfig)

    record(
      triggerStateTy,
      "acs" -> acs,
      "actAs" -> actAs,
      "readAs" -> readAs,
      "userState" -> userState,
      "commandsInFlight" -> SMap(
        isTextMap = false,
        commandsInFlight.iterator.map { case (cmdId, cmds) =>
          (fromCommandId(cmdId), fromCommands(cmds))
        },
      ),
      "config" -> config,
    )
  }
}

object Converter {

  final case class AnyContractId(templateId: Identifier, contractId: ContractId)
  final case class AnyTemplate(ty: Identifier, arg: SValue)

  private final case class AnyChoice(name: ChoiceName, arg: SValue)
  private final case class AnyContractKey(key: SValue)

  object EventVariant {
    // Those values should be kept consistent with type `Event` defined in
    // triggers/daml/Daml/Trigger/LowLevel.daml
    val CreatedEventConstructor = Name.assertFromString("CreatedEvent")
    val CreatedEventConstructorRank = 0
    val ArchiveEventConstructor = Name.assertFromString("ArchivedEvent")
    val ArchiveEventConstructorRank = 1
  }

  object MessageVariant {
    // Those values should be kept consistent with type `Message` defined in
    // triggers/daml/Daml/Trigger/LowLevel.daml
    val MTransactionVariant = Name.assertFromString("MTransaction")
    val MTransactionVariantRank = 0
    val MCompletionConstructor = Name.assertFromString("MCompletion")
    val MCompletionConstructorRank = 1
    val MHeartbeatConstructor = Name.assertFromString("MHeartbeat")
    val MHeartbeatConstructorRank = 2
  }

  object CompletionStatusVariant {
    // Those values should be kept consistent `CompletionStatus` defined in
    // triggers/daml/Daml/Trigger/LowLevel.daml
    val FailVariantConstructor = Name.assertFromString("Failed")
    val FailVariantConstructorRank = 0
    val SucceedVariantConstructor = Name.assertFromString("Succeeded")
    val SucceedVariantConstructorRank = 1
  }

  def fromIdentifier(identifier: value.Identifier): Either[String, Identifier] =
    for {
      pkgId <- PackageId.fromString(identifier.packageId)
      mod <- DottedName.fromString(identifier.moduleName)
      name <- DottedName.fromString(identifier.entityName)
    } yield Identifier(pkgId, QualifiedName(mod, name))

  private def toLedgerRecord(v: SValue): Either[String, value.Record] =
    lfValueToApiRecord(verbose = true, v.toUnnormalizedValue)

  private def toLedgerValue(v: SValue): Either[String, value.Value] =
    lfValueToApiValue(verbose = true, v.toUnnormalizedValue)

  private def toIdentifier(v: SValue): Either[String, Identifier] =
    v.expect(
      "STypeRep",
      { case STypeRep(TTyCon(id)) =>
        id
      },
    )

  def toAnyContractId(v: SValue): Either[String, AnyContractId] =
    v.expectE(
      "AnyContractId",
      { case SRecord(_, _, ArrayList(stid, scid)) =>
        for {
          templateId <- toTemplateTypeRep(stid)
          contractId <- toContractId(scid)
        } yield AnyContractId(templateId, contractId)
      },
    )

  private def toTemplateTypeRep(v: SValue): Either[String, Identifier] =
    v.expectE(
      "TemplateTypeRep",
      { case SRecord(_, _, ArrayList(id)) =>
        toIdentifier(id)
      },
    )

  def toFiniteDuration(value: SValue): Either[String, FiniteDuration] =
    value.expect(
      "RelTime",
      { case SRecord(_, _, ArrayList(SInt64(microseconds))) =>
        FiniteDuration(microseconds, MICROSECONDS)
      },
    )

  private def toRegisteredTemplate(v: SValue): Either[String, Identifier] =
    v.expectE(
      "RegisteredTemplate",
      { case SRecord(_, _, ArrayList(sttr)) =>
        toTemplateTypeRep(sttr)
      },
    )

  def toRegisteredTemplates(v: SValue): Either[String, Seq[Identifier]] =
    v.expectE(
      "list of RegisteredTemplate",
      { case SList(tpls) =>
        tpls.traverse(toRegisteredTemplate).map(_.toImmArray.toSeq)
      },
    )

  def toAnyTemplate(v: SValue): Either[String, AnyTemplate] =
    v match {
      case SRecord(_, _, ArrayList(SAny(TTyCon(tmplId), value))) =>
        Right(AnyTemplate(tmplId, value))
      case _ => Left(s"Expected AnyTemplate but got $v")
    }

  private def choiceArgTypeToChoiceName(choiceCons: TypeConName) = {
    // This exploits the fact that in Daml, choice argument type names
    // and choice names match up.
    assert(choiceCons.qualifiedName.name.segments.length == 1)
    choiceCons.qualifiedName.name.segments.head
  }

  private def toAnyChoice(v: SValue): Either[String, AnyChoice] =
    v match {
      case SRecord(_, _, ArrayList(SAny(TTyCon(choiceCons), choiceVal), _)) =>
        Right(AnyChoice(choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case _ =>
        Left(s"Expected AnyChoice but got $v")
    }

  private def toAnyContractKey(v: SValue): Either[String, AnyContractKey] =
    v.expect(
      "AnyContractKey",
      { case SRecord(_, _, ArrayList(SAny(_, v), _)) =>
        AnyContractKey(v)
      },
    )

  private def toCreate(v: SValue): Either[String, CreateCommand] =
    v.expectE(
      "CreateCommand",
      { case SRecord(_, _, ArrayList(sTpl)) =>
        for {
          anyTmpl <- toAnyTemplate(sTpl)
          templateArg <- toLedgerRecord(anyTmpl.arg)
        } yield CreateCommand(Some(toApiIdentifier(anyTmpl.ty)), Some(templateArg))
      },
    )

  private def toExercise(v: SValue): Either[String, ExerciseCommand] =
    v.expectE(
      "ExerciseCommand",
      { case SRecord(_, _, ArrayList(sAnyContractId, sChoiceVal)) =>
        for {
          anyContractId <- toAnyContractId(sAnyContractId)
          anyChoice <- toAnyChoice(sChoiceVal)
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield ExerciseCommand(
          Some(toApiIdentifier(anyContractId.templateId)),
          anyContractId.contractId.coid,
          anyChoice.name,
          Some(choiceArg),
        )
      },
    )

  private def toExerciseByKey(v: SValue): Either[String, ExerciseByKeyCommand] =
    v.expectE(
      "ExerciseByKeyCommand",
      { case SRecord(_, _, ArrayList(stplId, skeyVal, sChoiceVal)) =>
        for {
          tplId <- toTemplateTypeRep(stplId)
          keyVal <- toAnyContractKey(skeyVal)
          keyArg <- toLedgerValue(keyVal.key)
          anyChoice <- toAnyChoice(sChoiceVal)
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield ExerciseByKeyCommand(
          Some(toApiIdentifier(tplId)),
          Some(keyArg),
          anyChoice.name,
          Some(choiceArg),
        )
      },
    )

  private def toCreateAndExercise(v: SValue): Either[String, CreateAndExerciseCommand] =
    v.expectE(
      "CreateAndExerciseCommand",
      { case SRecord(_, _, ArrayList(sTpl, sChoiceVal)) =>
        for {
          anyTmpl <- toAnyTemplate(sTpl)
          templateArg <- toLedgerRecord(anyTmpl.arg)
          anyChoice <- toAnyChoice(sChoiceVal)
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield CreateAndExerciseCommand(
          Some(toApiIdentifier(anyTmpl.ty)),
          Some(templateArg),
          anyChoice.name,
          Some(choiceArg),
        )
      },
    )

  private def toCommand(v: SValue): Either[String, ApiCommand] = {
    v match {
      case SVariant(_, "CreateCommand", _, createVal) =>
        for {
          create <- toCreate(createVal)
        } yield ApiCommand().withCreate(create)
      case SVariant(_, "ExerciseCommand", _, exerciseVal) =>
        for {
          exercise <- toExercise(exerciseVal)
        } yield ApiCommand().withExercise(exercise)
      case SVariant(_, "ExerciseByKeyCommand", _, exerciseByKeyVal) =>
        for {
          exerciseByKey <- toExerciseByKey(exerciseByKeyVal)
        } yield ApiCommand().withExerciseByKey(exerciseByKey)
      case SVariant(_, "CreateAndExerciseCommand", _, createAndExerciseVal) =>
        for {
          createAndExercise <- toCreateAndExercise(createAndExerciseVal)
        } yield ApiCommand().withCreateAndExercise(createAndExercise)
      case _ => Left(s"Expected a Command but got $v")
    }
  }

  def toCommands(v: SValue): Either[String, Seq[ApiCommand]] =
    for {
      cmdValues <- v.expect(
        "[Command]",
        { case SList(cmdValues) =>
          cmdValues
        },
      )
      commands <- cmdValues.traverse(toCommand)
    } yield commands.toImmArray.toSeq
}

// Helper to create identifiers pointing to the Daml.Trigger module
final case class TriggerIds(triggerPackageId: PackageId) {
  def damlTrigger(s: String): Identifier =
    Identifier(
      triggerPackageId,
      QualifiedName(ModuleName.assertFromString("Daml.Trigger"), DottedName.assertFromString(s)),
    )

  def damlTriggerLowLevel(s: String): Identifier =
    Identifier(
      triggerPackageId,
      QualifiedName(
        ModuleName.assertFromString("Daml.Trigger.LowLevel"),
        DottedName.assertFromString(s),
      ),
    )

  def damlTriggerInternal(s: String): Identifier =
    Identifier(
      triggerPackageId,
      QualifiedName(
        ModuleName.assertFromString("Daml.Trigger.Internal"),
        DottedName.assertFromString(s),
      ),
    )
}
