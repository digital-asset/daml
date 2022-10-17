// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger

import scalaz.std.either._
import scalaz.syntax.traverse._
import com.daml.lf.data.{FrontStack, ImmArray, Struct}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.api.v1.commands.{
  Command,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.lf.language.StablePackage.DA
import com.daml.lf.value.Value
import com.daml.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier,
}

import scala.concurrent.duration.{FiniteDuration, MICROSECONDS}

// Convert from a Ledger API transaction to an SValue corresponding to a Message from the Daml.Trigger module
final class Converter(compiledPackages: CompiledPackages, triggerIds: TriggerIds) {

  import Converter._
  import com.daml.script.converter.Converter._, Implicits._

  private[this] val valueTranslator = new preprocessing.ValueTranslator(
    compiledPackages.pkgInterface,
    requireV1ContractIdSuffix = false,
  )

  private[this] def validateRecord(r: value.Record): Either[String, Value.ValueRecord] =
    NoLoggingValueValidator.validateRecord(r).left.map(_.getMessage)

  private[this] def translateValue(ty: Type, value: Value): Either[String, SValue] =
    valueTranslator.translateValue(ty, value).left.map(res => s"Failure to translate value: $res")

  private[this] val anyTemplateTyCon = DA.Internal.Any.assertIdentifier("AnyTemplate")
  private[this] val templateTypeRepTyCon = DA.Internal.Any.assertIdentifier("TemplateTypeRep")

  private[this] val activeContractsTy = triggerIds.damlTriggerLowLevel("ActiveContracts")
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

  private[this] def fromIdentifier(identifier: value.Identifier) =
    for {
      pkgId <- PackageId.fromString(identifier.packageId)
      mod <- DottedName.fromString(identifier.moduleName)
      name <- DottedName.fromString(identifier.entityName)
    } yield Identifier(pkgId, QualifiedName(mod, name))

  private[this] def toLedgerRecord(v: SValue): Either[String, value.Record] =
    lfValueToApiRecord(true, v.toUnnormalizedValue)

  private[this] def toLedgerValue(v: SValue): Either[String, value.Value] =
    lfValueToApiValue(true, v.toUnnormalizedValue)

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

  private[this] def fromCommandId(commandId: String): SValue =
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

  private[this] def fromCreatedEvent(
      created: CreatedEvent
  ): Either[String, SValue] =
    for {
      tmplId <- fromIdentifier(created.getTemplateId)
      createArguments <- validateRecord(created.getCreateArguments)
      tmplPayload <- translateValue(TTyCon(tmplId), createArguments)
    } yield {
      record(
        createdTy,
        "eventId" -> fromEventId(created.eventId),
        "contractId" -> fromAnyContractId(created.getTemplateId, created.contractId),
        "argument" -> record(
          anyTemplateTyCon,
          "getAnyTemplate" -> SAny(TTyCon(tmplId), tmplPayload),
        ),
      )
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
          CompletionStatusVariant.SucceedVariantConstrcutor,
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

  def fromACS(createdEvents: Seq[CreatedEvent]): Either[String, SValue] =
    for {
      events <- createdEvents
        .to(ImmArray)
        .traverse(fromCreatedEvent)
        .map(xs => SList(FrontStack.from(xs)))
    } yield record(activeContractsTy, "activeContracts" -> events)

  def toFiniteDuration(value: SValue): Either[String, FiniteDuration] =
    value.expect(
      "RelTime",
      { case SRecord(_, _, ArrayList(SInt64(microseconds))) =>
        FiniteDuration(microseconds, MICROSECONDS)
      },
    )

  private[this] def toIdentifier(v: SValue): Either[String, Identifier] =
    v.expect(
      "STypeRep",
      { case STypeRep(TTyCon(id)) =>
        id
      },
    )

  private[this] def toTemplateTypeRep(v: SValue): Either[String, Identifier] =
    v.expectE(
      "TemplateTypeRep",
      { case SRecord(_, _, ArrayList(id)) =>
        toIdentifier(id)
      },
    )

  private[this] def toRegisteredTemplate(v: SValue): Either[String, Identifier] =
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

  private[this] def toAnyContractId(v: SValue): Either[String, AnyContractId] =
    v.expectE(
      "AnyContractId",
      { case SRecord(_, _, ArrayList(stid, scid)) =>
        for {
          templateId <- toTemplateTypeRep(stid)
          contractId <- toContractId(scid)
        } yield AnyContractId(templateId, contractId)
      },
    )

  private[this] def toAnyTemplate(v: SValue): Either[String, AnyTemplate] =
    v match {
      case SRecord(_, _, ArrayList(SAny(TTyCon(tmplId), value))) =>
        Right(AnyTemplate(tmplId, value))
      case _ => Left(s"Expected AnyTemplate but got $v")
    }

  private[this] def choiceArgTypeToChoiceName(choiceCons: TypeConName) = {
    // This exploits the fact that in Daml, choice argument type names
    // and choice names match up.
    assert(choiceCons.qualifiedName.name.segments.length == 1)
    choiceCons.qualifiedName.name.segments.head
  }

  private[this] def toAnyChoice(v: SValue): Either[String, AnyChoice] =
    v match {
      case SRecord(_, _, ArrayList(SAny(TTyCon(choiceCons), choiceVal), _)) =>
        Right(AnyChoice.Template(choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case SRecord(
            _,
            _,
            ArrayList(
              SAny(
                TStruct(Struct((_, TTyCon(choiceCons)), _)),
                SStruct(_, ArrayList(choiceVal, STypeRep(TTyCon(ifaceId)))),
              ),
              _,
            ),
          ) =>
        Right(AnyChoice.Interface(ifaceId, choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case _ =>
        Left(s"Expected AnyChoice but got $v")
    }

  private[this] def toAnyContractKey(v: SValue): Either[String, AnyContractKey] =
    v.expect(
      "AnyContractKey",
      { case SRecord(_, _, ArrayList(SAny(_, v), _)) =>
        AnyContractKey(v)
      },
    )

  private[this] def toCreate(v: SValue): Either[String, CreateCommand] =
    v.expectE(
      "CreateCommand",
      { case SRecord(_, _, ArrayList(sTpl)) =>
        for {
          anyTmpl <- toAnyTemplate(sTpl)
          templateArg <- toLedgerRecord(anyTmpl.arg)
        } yield CreateCommand(Some(toApiIdentifier(anyTmpl.ty)), Some(templateArg))
      },
    )

  private[this] def toExercise(v: SValue): Either[String, ExerciseCommand] =
    v.expectE(
      "ExerciseCommand",
      { case SRecord(_, _, ArrayList(sAnyContractId, sChoiceVal)) =>
        for {
          anyContractId <- toAnyContractId(sAnyContractId)
          anyChoice <- toAnyChoice(sChoiceVal)
          choiceTypeId = anyChoice match {
            case _: AnyChoice.Template => anyContractId.templateId
            case AnyChoice.Interface(ifaceId, _, _) => ifaceId
          }
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield ExerciseCommand(
          Some(toApiIdentifier(choiceTypeId)),
          anyContractId.contractId.coid,
          anyChoice.name,
          Some(choiceArg),
        )
      },
    )

  private[this] def toExerciseByKey(v: SValue): Either[String, ExerciseByKeyCommand] =
    v.expectE(
      "ExerciseByKeyCommand",
      { case SRecord(_, _, ArrayList(stplId, skeyVal, sChoiceVal)) =>
        for {
          tplId <- toTemplateTypeRep(stplId)
          keyVal <- toAnyContractKey(skeyVal)
          keyArg <- toLedgerValue(keyVal.key)
          anyChoice <- toAnyChoice(sChoiceVal)
          _ <- anyChoice match {
            case _: AnyChoice.Template => Right(())
            case _: AnyChoice.Interface =>
              Left("Cannot run a ExerciseByKey over a interface choice")
          }
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield ExerciseByKeyCommand(
          Some(toApiIdentifier(tplId)),
          Some(keyArg),
          anyChoice.name,
          Some(choiceArg),
        )
      },
    )

  private[this] def toCreateAndExercise(v: SValue): Either[String, CreateAndExerciseCommand] =
    v.expectE(
      "CreateAndExerciseCommand",
      { case SRecord(_, _, ArrayList(sTpl, sChoiceVal)) =>
        for {
          anyTmpl <- toAnyTemplate(sTpl)
          templateArg <- toLedgerRecord(anyTmpl.arg)
          anyChoice <- toAnyChoice(sChoiceVal)
          _ <- anyChoice match {
            case _: AnyChoice.Template => Right(())
            case _: AnyChoice.Interface =>
              Left("Cannot run a CreateAndExercise over a interface choice")
          }
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield CreateAndExerciseCommand(
          Some(toApiIdentifier(anyTmpl.ty)),
          Some(templateArg),
          anyChoice.name,
          Some(choiceArg),
        )
      },
    )

  private[this] def toCommand(v: SValue): Either[String, Command] = {
    v match {
      case SVariant(_, "CreateCommand", _, createVal) =>
        for {
          create <- toCreate(createVal)
        } yield Command().withCreate(create)
      case SVariant(_, "ExerciseCommand", _, exerciseVal) =>
        for {
          exercise <- toExercise(exerciseVal)
        } yield Command().withExercise(exercise)
      case SVariant(_, "ExerciseByKeyCommand", _, exerciseByKeyVal) =>
        for {
          exerciseByKey <- toExerciseByKey(exerciseByKeyVal)
        } yield Command().withExerciseByKey(exerciseByKey)
      case SVariant(_, "CreateAndExerciseCommand", _, createAndExerciseVal) =>
        for {
          createAndExercise <- toCreateAndExercise(createAndExerciseVal)
        } yield Command().withCreateAndExercise(createAndExercise)
      case _ => Left(s"Expected a Command but got $v")
    }
  }

  def toCommands(v: SValue): Either[String, Seq[Command]] =
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

object Converter {

  private case class AnyContractId(templateId: Identifier, contractId: ContractId)
  private case class AnyTemplate(ty: Identifier, arg: SValue)
  sealed abstract class AnyChoice extends Product with Serializable {
    def name: ChoiceName
    def arg: SValue
  }
  object AnyChoice {
    final case class Template(name: ChoiceName, arg: SValue) extends AnyChoice
    final case class Interface(ifaceId: Identifier, name: ChoiceName, arg: SValue) extends AnyChoice
  }
  private case class AnyContractKey(key: SValue)

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
    val SucceedVariantConstrcutor = 1
  }
}

// Helper to create identifiers pointing to the Daml.Trigger module
case class TriggerIds(val triggerPackageId: PackageId) {
  def damlTrigger(s: String) =
    Identifier(
      triggerPackageId,
      QualifiedName(ModuleName.assertFromString("Daml.Trigger"), DottedName.assertFromString(s)),
    )
  def damlTriggerLowLevel(s: String) =
    Identifier(
      triggerPackageId,
      QualifiedName(
        ModuleName.assertFromString("Daml.Trigger.LowLevel"),
        DottedName.assertFromString(s),
      ),
    )
  def damlTriggerInternal(s: String) =
    Identifier(
      triggerPackageId,
      QualifiedName(
        ModuleName.assertFromString("Daml.Trigger.Internal"),
        DottedName.assertFromString(s),
      ),
    )
}
