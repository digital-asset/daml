// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger

import scalaz.std.either._
import scalaz.syntax.traverse._
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.api.v1.commands.{
  Command,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
  CreateAndExerciseCommand
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.validation.ValueValidator
import com.daml.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier
}

import scala.concurrent.duration.{FiniteDuration, MICROSECONDS}

// Convert from a Ledger API transaction to an SValue corresponding to a Message from the Daml.Trigger module
case class Converter(
    fromTransaction: Transaction => Either[String, SValue],
    fromCompletion: Completion => Either[String, SValue],
    fromHeartbeat: SValue,
    fromACS: Seq[CreatedEvent] => Either[String, SValue],
    toFiniteDuration: SValue => Either[String, FiniteDuration],
    toCommands: SValue => Either[String, Seq[Command]],
    toRegisteredTemplates: SValue => Either[String, Seq[Identifier]],
)

// Helper to create identifiers pointing to the DAML.Trigger module
case class TriggerIds(val triggerPackageId: PackageId) {
  def damlTrigger(s: String) =
    Identifier(
      triggerPackageId,
      QualifiedName(ModuleName.assertFromString("Daml.Trigger"), DottedName.assertFromString(s)))
  def damlTriggerLowLevel(s: String) =
    Identifier(
      triggerPackageId,
      QualifiedName(
        ModuleName.assertFromString("Daml.Trigger.LowLevel"),
        DottedName.assertFromString(s)))
  def damlTriggerInternal(s: String) =
    Identifier(
      triggerPackageId,
      QualifiedName(
        ModuleName.assertFromString("Daml.Trigger.Internal"),
        DottedName.assertFromString(s)))
}

case class AnyContractId(templateId: Identifier, contractId: ContractId)

object Converter {
  import com.daml.script.converter.Converter._, Implicits._

  private def toLedgerRecord(v: SValue): Either[String, value.Record] =
    lfValueToApiRecord(true, v.toValue)

  private def toLedgerValue(v: SValue): Either[String, value.Value] =
    lfValueToApiValue(true, v.toValue)

  private def fromIdentifier(id: value.Identifier): SValue = {
    STypeRep(
      TTyCon(
        TypeConName(
          PackageId.assertFromString(id.packageId),
          QualifiedName(
            DottedName.assertFromString(id.moduleName),
            DottedName.assertFromString(id.entityName)))))
  }

  private def fromTransactionId(triggerIds: TriggerIds, transactionId: String): SValue = {
    val transactionIdTy = triggerIds.damlTriggerLowLevel("TransactionId")
    record(transactionIdTy, ("unpack", SText(transactionId)))
  }

  private def fromEventId(triggerIds: TriggerIds, eventId: String): SValue = {
    val eventIdTy = triggerIds.damlTriggerLowLevel("EventId")
    record(eventIdTy, ("unpack", SText(eventId)))
  }

  private def fromCommandId(triggerIds: TriggerIds, commandId: String): SValue = {
    val commandIdTy = triggerIds.damlTriggerLowLevel("CommandId")
    record(commandIdTy, ("unpack", SText(commandId)))
  }

  private def fromOptionalCommandId(triggerIds: TriggerIds, commandId: String): SValue = {
    if (commandId.isEmpty) {
      SOptional(None)
    } else {
      SOptional(Some(fromCommandId(triggerIds, commandId)))
    }
  }

  private def fromTemplateTypeRep(templateId: value.Identifier): SValue = {
    val templateTypeRepTy = daInternalAny("TemplateTypeRep")
    record(templateTypeRepTy, ("getTemplateTypeRep", fromIdentifier(templateId)))
  }

  private def fromAnyContractId(
      triggerIds: TriggerIds,
      templateId: value.Identifier,
      contractId: String): SValue = {
    val contractIdTy = triggerIds.damlTriggerLowLevel("AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromTemplateTypeRep(templateId)),
      ("contractId", SContractId(ContractId.assertFromString(contractId)))
    )
  }

  private def fromArchivedEvent(triggerIds: TriggerIds, archived: ArchivedEvent): SValue = {
    val archivedTy = triggerIds.damlTriggerLowLevel("Archived")
    record(
      archivedTy,
      ("eventId", fromEventId(triggerIds, archived.eventId)),
      ("contractId", fromAnyContractId(triggerIds, archived.getTemplateId, archived.contractId))
    )
  }

  private def fromCreatedEvent(
      valueTranslator: preprocessing.ValueTranslator,
      triggerIds: TriggerIds,
      created: CreatedEvent): Either[String, SValue] = {
    val createdTy = triggerIds.damlTriggerLowLevel("Created")
    val anyTemplateTyCon = daInternalAny("AnyTemplate")
    val templateTy = Identifier(
      PackageId.assertFromString(created.getTemplateId.packageId),
      QualifiedName(
        DottedName.assertFromString(created.getTemplateId.moduleName),
        DottedName.assertFromString(created.getTemplateId.entityName))
    )
    for {
      createArguments <- ValueValidator
        .validateRecord(created.getCreateArguments)
        .left
        .map(_.getMessage)
      anyTemplate <- valueTranslator.translateValue(TTyCon(templateTy), createArguments) match {
        case Right(r @ SRecord(tyCon, _, _)) =>
          Right(record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), r))))
        case Right(v) =>
          Left(s"Expected record but got $v")
        case Left(res) =>
          Left(s"Failure to translate value in create: $res")
      }
    } yield
      record(
        createdTy,
        ("eventId", fromEventId(triggerIds, created.eventId)),
        ("contractId", fromAnyContractId(triggerIds, created.getTemplateId, created.contractId)),
        ("argument", anyTemplate)
      )
  }

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

  private def fromEvent(
      valueTranslator: preprocessing.ValueTranslator,
      triggerIds: TriggerIds,
      ev: Event): Either[String, SValue] = {
    val eventTy = triggerIds.damlTriggerLowLevel("Event")
    ev.event match {
      case Event.Event.Archived(archivedEvent) =>
        Right(
          SVariant(
            id = eventTy,
            variant = EventVariant.ArchiveEventConstructor,
            constructorRank = EventVariant.ArchiveEventConstructorRank,
            value = fromArchivedEvent(triggerIds, archivedEvent)
          )
        )
      case Event.Event.Created(createdEvent) =>
        for {
          event <- fromCreatedEvent(valueTranslator, triggerIds, createdEvent)
        } yield
          SVariant(
            id = eventTy,
            variant = EventVariant.CreatedEventConstructor,
            constructorRank = EventVariant.CreatedEventConstructorRank,
            value = event)
      case _ => Left(s"Expected Archived or Created but got ${ev.event}")
    }
  }

  private def fromTransaction(
      valueTranslator: preprocessing.ValueTranslator,
      triggerIds: TriggerIds,
      t: Transaction): Either[String, SValue] = {
    val messageTy = triggerIds.damlTriggerLowLevel("Message")
    val transactionTy = triggerIds.damlTriggerLowLevel("Transaction")
    for {
      events <- FrontStack(t.events).traverse(fromEvent(valueTranslator, triggerIds, _)).map(SList)
      transactionId = fromTransactionId(triggerIds, t.transactionId)
      commandId = fromOptionalCommandId(triggerIds, t.commandId)
    } yield
      SVariant(
        id = messageTy,
        variant = MessageVariant.MTransactionVariant,
        constructorRank = MessageVariant.MTransactionVariantRank,
        value = record(
          transactionTy,
          ("transactionId", transactionId),
          ("commandId", commandId),
          ("events", events)
        )
      )
  }

  private def fromCompletion(triggerIds: TriggerIds, c: Completion): Either[String, SValue] = {
    val messageTy = triggerIds.damlTriggerLowLevel("Message")
    val completionTy = triggerIds.damlTriggerLowLevel("Completion")
    val status: SValue = if (c.getStatus.code == 0) {
      SVariant(
        triggerIds.damlTriggerLowLevel("CompletionStatus"),
        CompletionStatusVariant.SucceedVariantConstructor,
        CompletionStatusVariant.SucceedVariantConstrcutor,
        record(
          triggerIds.damlTriggerLowLevel("CompletionStatus.Succeeded"),
          ("transactionId", fromTransactionId(triggerIds, c.transactionId)))
      )
    } else {
      SVariant(
        triggerIds.damlTriggerLowLevel("CompletionStatus"),
        CompletionStatusVariant.FailVariantConstructor,
        CompletionStatusVariant.FailVariantConstructorRank,
        record(
          triggerIds.damlTriggerLowLevel("CompletionStatus.Failed"),
          ("status", SInt64(c.getStatus.code.asInstanceOf[Long])),
          ("message", SText(c.getStatus.message))
        )
      )
    }
    Right(
      SVariant(
        messageTy,
        MessageVariant.MCompletionConstructor,
        MessageVariant.MCompletionConstructorRank,
        record(
          completionTy,
          ("commandId", fromCommandId(triggerIds, c.commandId)),
          ("status", status)
        )
      ))
  }

  private def fromHeartbeat(ids: TriggerIds): SValue = {
    val messageTy = ids.damlTriggerLowLevel("Message")
    SVariant(
      messageTy,
      MessageVariant.MHeartbeatConstructor,
      MessageVariant.MHeartbeatConstructorRank,
      SUnit
    )
  }

  private def toFiniteDuration(value: SValue): Either[String, FiniteDuration] =
    value expect ("RelTime", {
      case SRecord(_, _, JavaList(SInt64(microseconds))) =>
        FiniteDuration(microseconds, MICROSECONDS)
    })

  private def toIdentifier(v: SValue): Either[String, Identifier] =
    v expect ("STypeRep", {
      case STypeRep(TTyCon(id)) => id
    })

  private def extractTemplateId(v: SValue): Either[String, Identifier] =
    v expect ("contract value", {
      case SRecord(templateId, _, _) => templateId
    })

  private def toTemplateTypeRep(v: SValue): Either[String, Identifier] =
    v expectE ("TemplateTypeRep", {
      case SRecord(_, _, JavaList(id)) =>
        toIdentifier(id)
    })

  private def toRegisteredTemplate(v: SValue): Either[String, Identifier] =
    v expectE ("RegisteredTemplate", {
      case SRecord(_, _, JavaList(sttr)) => toTemplateTypeRep(sttr)
    })

  private def toRegisteredTemplates(v: SValue): Either[String, Seq[Identifier]] =
    v expectE ("list of RegisteredTemplate", {
      case SList(tpls) => tpls.traverse(toRegisteredTemplate).map(_.toImmArray.toSeq)
    })

  private def toAnyContractId(v: SValue): Either[String, AnyContractId] =
    v expectE ("AnyContractId", {
      case SRecord(_, _, JavaList(stid, scid)) =>
        for {
          templateId <- toTemplateTypeRep(stid)
          contractId <- toContractId(scid)
        } yield AnyContractId(templateId, contractId)
    })

  private def toAnyTemplate(v: SValue): Either[String, SValue] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 =>
        vals.get(0) match {
          case SAny(_, v) => Right(v)
          case v => Left(s"Expected Any but got $v")
        }
      case _ => Left(s"Expected AnyTemplate but got $v")
    }
  }

  // toAnyChoice and toAnyContractKey are identical right now
  // but there is no resaon why they have to be, so we
  // use two different methods.
  private def toAnyChoice(v: SValue): Either[String, SValue] =
    v expect ("AnyChoice", {
      case SRecord(_, _, JavaList(SAny(_, v), _)) => v
    })

  private def toAnyContractKey(v: SValue): Either[String, SValue] =
    v expect ("AnyContractKey", {
      case SRecord(_, _, JavaList(SAny(_, v), _)) => v
    })

  private def extractChoiceName(v: SValue): Either[String, String] =
    v expect ("choice value", {
      case SRecord(ty, _, _) =>
        ty.qualifiedName.name.toString
    })

  private def toCreate(v: SValue): Either[String, CreateCommand] =
    v expectE ("CreateCommand", {
      case SRecord(_, _, JavaList(sTpl)) =>
        for {
          tpl <- toAnyTemplate(sTpl)
          templateId <- extractTemplateId(tpl)
          templateArg <- toLedgerRecord(tpl)
        } yield CreateCommand(Some(toApiIdentifier(templateId)), Some(templateArg))
    })

  private def toExercise(v: SValue): Either[String, ExerciseCommand] =
    v expectE ("ExerciseCommand", {
      case SRecord(_, _, JavaList(sAnyContractId, sChoiceVal)) =>
        for {
          anyContractId <- toAnyContractId(sAnyContractId)
          choiceVal <- toAnyChoice(sChoiceVal)
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield
          ExerciseCommand(
            Some(toApiIdentifier(anyContractId.templateId)),
            anyContractId.contractId.coid,
            choiceName,
            Some(choiceArg))
    })

  private def toExerciseByKey(v: SValue): Either[String, ExerciseByKeyCommand] =
    v expectE ("ExerciseByKeyCommand", {
      case SRecord(_, _, JavaList(stplId, skeyVal, schoiceVal)) =>
        for {
          tplId <- toTemplateTypeRep(stplId)
          keyVal <- toAnyContractKey(skeyVal)
          keyArg <- toLedgerValue(keyVal)
          choiceVal <- toAnyChoice(schoiceVal)
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield
          ExerciseByKeyCommand(
            Some(toApiIdentifier(tplId)),
            Some(keyArg),
            choiceName,
            Some(choiceArg)
          )
    })

  private def toCreateAndExercise(v: SValue): Either[String, CreateAndExerciseCommand] =
    v expectE ("CreateAndExerciseCommand", {
      case SRecord(_, _, JavaList(stpl, schoiceVal)) =>
        for {
          tpl <- toAnyTemplate(stpl)
          templateId <- extractTemplateId(tpl)
          templateArg <- toLedgerRecord(tpl)
          choiceVal <- toAnyChoice(schoiceVal)
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield
          CreateAndExerciseCommand(
            Some(toApiIdentifier(templateId)),
            Some(templateArg),
            choiceName,
            Some(choiceArg)
          )
    })

  private def toCommand(v: SValue): Either[String, Command] = {
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

  private def toCommands(v: SValue): Either[String, Seq[Command]] =
    for {
      cmdValues <- v expect ("[Command]", {
        case SList(cmdValues) => cmdValues
      })
      commands <- cmdValues.traverse(toCommand)
    } yield commands.toImmArray.toSeq

  private def fromACS(
      valueTranslator: preprocessing.ValueTranslator,
      triggerIds: TriggerIds,
      createdEvents: Seq[CreatedEvent]): Either[String, SValue] = {
    val activeContractsTy = triggerIds.damlTriggerLowLevel("ActiveContracts")
    for {
      events <- FrontStack(createdEvents)
        .traverse(fromCreatedEvent(valueTranslator, triggerIds, _))
        .map(SList)
    } yield record(activeContractsTy, ("activeContracts", events))
  }

  def apply(compiledPackages: CompiledPackages, triggerIds: TriggerIds): Converter = {
    val valueTranslator = new preprocessing.ValueTranslator(compiledPackages)
    Converter(
      fromTransaction(valueTranslator, triggerIds, _),
      fromCompletion(triggerIds, _),
      fromHeartbeat(triggerIds),
      fromACS(valueTranslator, triggerIds, _),
      toFiniteDuration(_),
      toCommands(_),
      toRegisteredTemplates(_),
    )
  }
}
