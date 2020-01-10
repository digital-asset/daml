// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.trigger

import com.digitalasset.daml.lf.engine.{ResultDone, ValueTranslator}
import java.util

import scala.collection.JavaConverters._
import scalaz.std.either._
import scalaz.syntax.traverse._
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId}
import com.digitalasset.ledger.api.v1.commands.{
  Command,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand
}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.platform.participant.util.LfEngineToApi.{
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
case class TriggerIds(
    triggerPackageId: PackageId,
    triggerModuleName: ModuleName,
    highlevelModuleName: ModuleName,
    stdlibPackageId: PackageId,
    mainPackageId: PackageId) {
  def getId(n: String): Identifier =
    Identifier(triggerPackageId, QualifiedName(triggerModuleName, DottedName.assertFromString(n)))
  def getHighlevelId(n: String): Identifier =
    Identifier(triggerPackageId, QualifiedName(highlevelModuleName, DottedName.assertFromString(n)))
}

object TriggerIds {
  def fromDar(dar: Dar[(PackageId, Package)]): TriggerIds = {
    val triggerModuleName = DottedName.assertFromString("Daml.Trigger.LowLevel")
    val highlevelModuleName = DottedName.assertFromString("Daml.Trigger")
    val triggerPackageId: PackageId = dar.all
      .find {
        case (pkgId, pkg) =>
          pkg.modules.contains(triggerModuleName) &&
            pkg.modules.contains(highlevelModuleName)
      }
      .get
      ._1
    val stdlibPackageId =
      dar.all
        .find {
          case (pkgId, pkg) =>
            pkg.modules.contains(DottedName.assertFromString("DA.Internal.LF"))
        }
        .get
        ._1
    TriggerIds(
      triggerPackageId,
      triggerModuleName,
      highlevelModuleName,
      stdlibPackageId,
      dar.main._1)
  }
}

case class AnyContractId(templateId: Identifier, contractId: AbsoluteContractId)

class ConverterException(message: String) extends RuntimeException(message)

object Converter {
  // Helper to make constructing an SRecord more convenient
  private def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = Name.Array(fields.map({ case (n, _) => Name.assertFromString(n) }): _*)
    val args = new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  private def toLedgerRecord(v: SValue): Either[String, value.Record] = {
    try {
      lfValueToApiRecord(
        true,
        v.toValue.mapContractId {
          case rcoid: RelativeContractId =>
            throw new ConverterException(s"Unexpected contract id $rcoid")
          case acoid: AbsoluteContractId => acoid
        }
      )
    } catch {
      case ex: ConverterException => Left(ex.getMessage())
    }
  }
  private def toLedgerValue(v: SValue) = {
    try {
      lfValueToApiValue(
        true,
        v.toValue.mapContractId {
          case rcoid: RelativeContractId =>
            throw new ConverterException(s"Unexpected contract id $rcoid")
          case acoid: AbsoluteContractId => acoid
        }
      )
    } catch {
      case ex: ConverterException => Left(ex.getMessage())
    }
  }

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
    val transactionIdTy = triggerIds.getId("TransactionId")
    record(transactionIdTy, ("unpack", SText(transactionId)))
  }

  private def fromEventId(triggerIds: TriggerIds, eventId: String): SValue = {
    val eventIdTy = triggerIds.getId("EventId")
    record(eventIdTy, ("unpack", SText(eventId)))
  }

  private def fromCommandId(triggerIds: TriggerIds, commandId: String): SValue = {
    val commandIdTy = triggerIds.getId("CommandId")
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
    val templateTypeRepTy = Identifier(
      triggerIds.stdlibPackageId,
      QualifiedName(
        DottedName.assertFromString("DA.Internal.LF"),
        DottedName.assertFromString("TemplateTypeRep")))
    record(templateTypeRepTy, ("getTemplateTypeRep", fromIdentifier(templateId)))
  }

  private def fromAnyContractId(
      triggerIds: TriggerIds,
      templateId: value.Identifier,
      contractId: String): SValue = {
    val contractIdTy = triggerIds.getId("AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromTemplateTypeRep(triggerIds, templateId)),
      ("contractId", SContractId(AbsoluteContractId(ContractIdString.assertFromString(contractId))))
    )
  }

  private def fromArchivedEvent(triggerIds: TriggerIds, archived: ArchivedEvent): SValue = {
    val archivedTy = triggerIds.getId("Archived")
    record(
      archivedTy,
      ("eventId", fromEventId(triggerIds, archived.eventId)),
      ("contractId", fromAnyContractId(triggerIds, archived.getTemplateId, archived.contractId))
    )
  }

  private def fromCreatedEvent(
      valueTranslator: ValueTranslator,
      triggerIds: TriggerIds,
      created: CreatedEvent): Either[String, SValue] = {
    val createdTy = triggerIds.getId("Created")
    val anyTemplateTyCon =
      Identifier(
        triggerIds.stdlibPackageId,
        QualifiedName(
          DottedName.assertFromString("DA.Internal.LF"),
          DottedName.assertFromString("AnyTemplate")))
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
        case ResultDone(r @ SRecord(tyCon, _, _)) =>
          Right(record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), r))))
        case ResultDone(v) => Left(s"Expected record but got $v")
        case res => Left(s"Failure to translate value in create: $res")
      }
    } yield
      record(
        createdTy,
        ("eventId", fromEventId(triggerIds, created.eventId)),
        ("contractId", fromAnyContractId(triggerIds, created.getTemplateId, created.contractId)),
        ("argument", anyTemplate)
      )
  }

  private def fromEvent(
      valueTranslator: ValueTranslator,
      triggerIds: TriggerIds,
      ev: Event): Either[String, SValue] = {
    val eventTy = triggerIds.getId("Event")
    ev.event match {
      case Event.Event.Archived(archivedEvent) =>
        for {
          variant <- Name.fromString("ArchivedEvent")
          event = fromArchivedEvent(triggerIds, archivedEvent)
        } yield SVariant(eventTy, variant, event)
      case Event.Event.Created(createdEvent) =>
        for {
          variant <- Name.fromString("CreatedEvent")
          event <- fromCreatedEvent(valueTranslator, triggerIds, createdEvent)
        } yield SVariant(eventTy, variant, event)
      case _ => Left(s"Expected Archived or Created but got ${ev.event}")
    }
  }

  private def fromTransaction(
      valueTranslator: ValueTranslator,
      triggerIds: TriggerIds,
      t: Transaction): Either[String, SValue] = {
    val messageTy = triggerIds.getId("Message")
    val transactionTy = triggerIds.getId("Transaction")
    for {
      name <- Name.fromString("MTransaction")
      transactionId = fromTransactionId(triggerIds, t.transactionId)
      commandId = fromOptionalCommandId(triggerIds, t.commandId)
      events <- FrontStack(t.events).traverseU(fromEvent(valueTranslator, triggerIds, _)).map(SList)
    } yield
      SVariant(
        messageTy,
        name,
        record(
          transactionTy,
          ("transactionId", transactionId),
          ("commandId", commandId),
          ("events", events)
        )
      )
  }

  private def fromCompletion(triggerIds: TriggerIds, c: Completion): Either[String, SValue] = {
    val messageTy = triggerIds.getId("Message")
    val completionTy = triggerIds.getId("Completion")
    val status: SValue = if (c.getStatus.code == 0) {
      SVariant(
        triggerIds.getId("CompletionStatus"),
        Name.assertFromString("Succeeded"),
        record(
          triggerIds.getId("CompletionStatus.Succeeded"),
          ("transactionId", fromTransactionId(triggerIds, c.transactionId)))
      )
    } else {
      SVariant(
        triggerIds.getId("CompletionStatus"),
        Name.assertFromString("Failed"),
        record(
          triggerIds.getId("CompletionStatus.Failed"),
          ("status", SInt64(c.getStatus.code.asInstanceOf[Long])),
          ("message", SText(c.getStatus.message)))
      )
    }
    Right(
      SVariant(
        messageTy,
        Name.assertFromString("MCompletion"),
        record(
          completionTy,
          ("commandId", fromCommandId(triggerIds, c.commandId)),
          ("status", status)
        )
      ))
  }

  private def fromHeartbeat(ids: TriggerIds): SValue = {
    val messageTy = ids.getId("Message")
    SVariant(
      messageTy,
      Name.assertFromString("MHeartbeat"),
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
      case SContractId(cid @ AbsoluteContractId(_)) => Right(cid)
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

  private def toCommand(triggerIds: TriggerIds, v: SValue): Either[String, Command] = {
    v match {
      case SVariant(_, "CreateCommand", createVal) =>
        for {
          create <- toCreate(triggerIds, createVal)
        } yield Command().withCreate(create)
      case SVariant(_, "ExerciseCommand", exerciseVal) =>
        for {
          exercise <- toExercise(triggerIds, exerciseVal)
        } yield Command().withExercise(exercise)
      case SVariant(_, "ExerciseByKeyCommand", exerciseByKeyVal) =>
        for {
          exerciseByKey <- toExerciseByKey(triggerIds, exerciseByKeyVal)
        } yield Command().withExerciseByKey(exerciseByKey)
      case _ => Left("Expected CreateCommand or ExerciseCommand but got $v")
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
      valueTranslator: ValueTranslator,
      triggerIds: TriggerIds,
      createdEvents: Seq[CreatedEvent]): Either[String, SValue] = {
    val activeContractsTy = triggerIds.getId("ActiveContracts")
    for {
      events <- FrontStack(createdEvents)
        .traverseU(fromCreatedEvent(valueTranslator, triggerIds, _))
        .map(SList)
    } yield record(activeContractsTy, ("activeContracts", events))
  }

  def fromDar(dar: Dar[(PackageId, Package)], compiledPackages: CompiledPackages): Converter = {
    val triggerIds = TriggerIds.fromDar(dar)
    val valueTranslator = new ValueTranslator(compiledPackages)
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
