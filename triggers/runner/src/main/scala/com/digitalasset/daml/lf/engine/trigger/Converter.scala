// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger

import java.util

import scala.collection.JavaConverters._
import scalaz.std.either._
import scalaz.syntax.traverse._
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.AbsoluteContractId
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
    toCommands: SValue => Either[String, (String, Seq[Command])],
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

case class AnyContractId(templateId: Identifier, contractId: AbsoluteContractId)

class ConverterException(message: String) extends RuntimeException(message)

object Converter {
  private val DA_INTERNAL_ANY_PKGID =
    PackageId.assertFromString("cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7")
  private def daInternalAny(s: String): Identifier =
    Identifier(
      DA_INTERNAL_ANY_PKGID,
      QualifiedName(DottedName.assertFromString("DA.Internal.Any"), DottedName.assertFromString(s)))

  // Helper to make constructing an SRecord more convenient
  private def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = Name.Array(fields.map({ case (n, _) => Name.assertFromString(n) }): _*)
    val args = new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  private def toLedgerRecord(v: SValue): Either[String, value.Record] =
    for {
      value <- v.toValue.ensureNoRelCid.left.map(rcoid => s"Unexpected contract id $rcoid")
      apiRecord <- lfValueToApiRecord(true, value)
    } yield apiRecord

  private def toLedgerValue(v: SValue): Either[String, value.Value] =
    for {
      value <- v.toValue.ensureNoRelCid.left.map(rcoid => s"Unexpected contract id $rcoid")
      apiValue <- lfValueToApiValue(true, value)
    } yield apiValue

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

  private def fromTemplateTypeRep(triggerIds: TriggerIds, templateId: value.Identifier): SValue = {
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
      ("templateId", fromTemplateTypeRep(triggerIds, templateId)),
      ("contractId", SContractId(AbsoluteContractId.assertFromString(contractId)))
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
      events <- FrontStack(t.events).traverseU(fromEvent(valueTranslator, triggerIds, _)).map(SList)
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

  private def toFiniteDuration(ids: TriggerIds, value: SValue): Either[String, FiniteDuration] = {
    value match {
      case SRecord(_, _, values) if values.size() == 1 =>
        values.get(0) match {
          case SInt64(microseconds) =>
            Right(FiniteDuration(microseconds, MICROSECONDS))
          case _ =>
            Left(s"Expected RelTime but got $value.")
        }
      case _ =>
        Left(s"Expected RelTime but got $value.")
    }
  }

  private def toText(v: SValue): Either[String, String] = {
    v match {
      case SText(t) => Right(t)
      case _ => Left(s"Expected Text but got $v")
    }
  }

  private def toCommandId(v: SValue): Either[String, String] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => toText(vals.get(0))
      case _ => Left(s"Expected CommandId but got $v")
    }
  }

  private def toIdentifier(v: SValue): Either[String, Identifier] = {
    v match {
      case STypeRep(TTyCon(id)) => Right(id)
      case _ => Left(s"Expected STypeRep but got $v")
    }
  }

  private def extractTemplateId(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(templateId, _, _) => Right(templateId)
      case _ => Left(s"Expected contract value but got $v")
    }
  }

  private def toTemplateTypeRep(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        toIdentifier(vals.get(0))
      }
      case _ => Left(s"Expected TemplateTypeRep but got $v")
    }
  }

  private def toRegisteredTemplate(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => toTemplateTypeRep(vals.get(0))
      case _ => Left(s"Expected RegisteredTemplate but got $v")
    }
  }

  private def toRegisteredTemplates(v: SValue): Either[String, Seq[Identifier]] = {
    v match {
      case SList(tpls) => tpls.traverseU(toRegisteredTemplate(_)).map(_.toImmArray.toSeq)
      case _ => Left(s"Expected list of RegisteredTemplate but got $v")
    }
  }

  private def toAbsoluteContractId(v: SValue): Either[String, AbsoluteContractId] = {
    v match {
      case SContractId(cid: AbsoluteContractId) => Right(cid)
      case _ => Left(s"Expected AbsoluteContractId but got $v")
    }
  }

  private def toAnyContractId(v: SValue): Either[String, AnyContractId] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          templateId <- toTemplateTypeRep(vals.get(0))
          contractId <- toAbsoluteContractId(vals.get(1))
        } yield AnyContractId(templateId, contractId)
      }
      case _ => Left(s"Expected AnyContractId but got $v")
    }
  }

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
  private def toAnyChoice(v: SValue): Either[String, SValue] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 =>
        vals.get(0) match {
          case SAny(_, v) => Right(v)
          case v => Left(s"Expected Any but got $v")
        }
      case _ => Left(s"Expected AnyChoice but got $v")
    }
  }

  private def toAnyContractKey(v: SValue): Either[String, SValue] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 =>
        vals.get(0) match {
          case SAny(_, v) => Right(v)
          case v => Left(s"Expected Any but got $v")
        }
      case _ => Left(s"Expected AnyContractKey but got $v")
    }
  }

  private def extractChoiceName(v: SValue): Either[String, String] = {
    v match {
      case SRecord(ty, _, _) => {
        Right(ty.qualifiedName.name.toString)
      }
      case _ => Left(s"Expected choice value but got $v")
    }
  }

  private def toCreate(triggerIds: TriggerIds, v: SValue): Either[String, CreateCommand] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        for {
          tpl <- toAnyTemplate(vals.get(0))
          templateId <- extractTemplateId(tpl)
          templateArg <- toLedgerRecord(tpl)
        } yield CreateCommand(Some(toApiIdentifier(templateId)), Some(templateArg))
      }
      case _ => Left(s"Expected CreateCommand but got $v")
    }
  }

  private def toExercise(triggerIds: TriggerIds, v: SValue): Either[String, ExerciseCommand] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          anyContractId <- toAnyContractId(vals.get(0))
          choiceVal <- toAnyChoice(vals.get(1))
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield {
          ExerciseCommand(
            Some(toApiIdentifier(anyContractId.templateId)),
            anyContractId.contractId.coid,
            choiceName,
            Some(choiceArg))
        }
      }
      case _ => Left(s"Expected ExerciseCommand but got $v")
    }
  }

  private def toExerciseByKey(
      triggerIds: TriggerIds,
      v: SValue): Either[String, ExerciseByKeyCommand] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 3 => {
        for {
          tplId <- toTemplateTypeRep(vals.get(0))
          keyVal <- toAnyContractKey(vals.get(1))
          keyArg <- toLedgerValue(keyVal)
          choiceVal <- toAnyChoice(vals.get(2))
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield {
          ExerciseByKeyCommand(
            Some(toApiIdentifier(tplId)),
            Some(keyArg),
            choiceName,
            Some(choiceArg)
          )
        }
      }
    }
  }

  private def toCreateAndExercise(
      triggerIds: TriggerIds,
      v: SValue): Either[String, CreateAndExerciseCommand] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          tpl <- toAnyTemplate(vals.get(0))
          templateId <- extractTemplateId(tpl)
          templateArg <- toLedgerRecord(tpl)
          choiceVal <- toAnyChoice(vals.get(1))
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield {
          CreateAndExerciseCommand(
            Some(toApiIdentifier(templateId)),
            Some(templateArg),
            choiceName,
            Some(choiceArg)
          )
        }
      }
    }
  }

  private def toCommand(triggerIds: TriggerIds, v: SValue): Either[String, Command] = {
    v match {
      case SVariant(_, "CreateCommand", _, createVal) =>
        for {
          create <- toCreate(triggerIds, createVal)
        } yield Command().withCreate(create)
      case SVariant(_, "ExerciseCommand", _, exerciseVal) =>
        for {
          exercise <- toExercise(triggerIds, exerciseVal)
        } yield Command().withExercise(exercise)
      case SVariant(_, "ExerciseByKeyCommand", _, exerciseByKeyVal) =>
        for {
          exerciseByKey <- toExerciseByKey(triggerIds, exerciseByKeyVal)
        } yield Command().withExerciseByKey(exerciseByKey)
      case SVariant(_, "CreateAndExerciseCommand", _, createAndExerciseVal) =>
        for {
          createAndExercise <- toCreateAndExercise(triggerIds, createAndExerciseVal)
        } yield Command().withCreateAndExercise(createAndExercise)
      case _ => Left(s"Expected a Command but got $v")
    }
  }

  private def toCommands(
      triggerIds: TriggerIds,
      v: SValue): Either[String, (String, Seq[Command])] = {
    for {
      values <- v match {
        case SRecord(_, _, values) if values.size == 2 => Right(values)
        case _ => Left(s"Expected Commands but got $v")
      }
      commandId <- toCommandId(values.get(0))
      commands <- values.get(1) match {
        case SList(cmdValues) => cmdValues.traverseU(toCommand(triggerIds, _))
        case _ => Left(s"Expected List but got ${values.get(1)}")
      }
    } yield (commandId, commands.toImmArray.toSeq)
  }

  private def fromACS(
      valueTranslator: preprocessing.ValueTranslator,
      triggerIds: TriggerIds,
      createdEvents: Seq[CreatedEvent]): Either[String, SValue] = {
    val activeContractsTy = triggerIds.damlTriggerLowLevel("ActiveContracts")
    for {
      events <- FrontStack(createdEvents)
        .traverseU(fromCreatedEvent(valueTranslator, triggerIds, _))
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
      toFiniteDuration(triggerIds, _),
      toCommands(triggerIds, _),
      toRegisteredTemplates(_),
    )
  }
}
