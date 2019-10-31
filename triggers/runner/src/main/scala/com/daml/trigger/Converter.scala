// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import java.util
import scala.collection.JavaConverters._
import scalaz.std.either._
import scalaz.syntax.traverse._

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId}

import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  toApiIdentifier,
  lfValueToApiRecord,
  lfValueToApiValue
}

// Convert from a Ledger API transaction to an SValue corresponding to a Message from the Daml.Trigger module
case class Converter(
    fromTransaction: Transaction => Either[String, SValue],
    fromCompletion: Completion => Either[String, SValue],
    fromACS: Seq[CreatedEvent] => Either[String, SValue],
    toCommands: SValue => Either[String, (String, Seq[Command])]
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

case class AnyContractId(templateId: Identifier, contractId: String)

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
      ("contractId", SText(contractId))
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
      triggerIds: TriggerIds,
      created: CreatedEvent): Either[String, SValue] = {
    val createdTy = triggerIds.getId("Created")
    val anyTemplateTyCon =
      Identifier(
        triggerIds.stdlibPackageId,
        QualifiedName(
          DottedName.assertFromString("DA.Internal.LF"),
          DottedName.assertFromString("AnyTemplate")))
    for {
      createArguments <- ValueValidator
        .validateRecord(created.getCreateArguments)
        .left
        .map(_.getMessage)
      anyTemplate <- SValue.fromValue(createArguments) match {
        case r @ SRecord(tyCon, _, _) =>
          Right(record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), r))))
        case v => Left(s"Expected record but got $v")
      }
    } yield
      record(
        createdTy,
        ("eventId", fromEventId(triggerIds, created.eventId)),
        ("contractId", fromAnyContractId(triggerIds, created.getTemplateId, created.contractId)),
        ("argument", anyTemplate)
      )
  }

  private def fromEvent(triggerIds: TriggerIds, ev: Event): Either[String, SValue] = {
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
          event <- fromCreatedEvent(triggerIds, createdEvent)
        } yield SVariant(eventTy, variant, event)
      case _ => Left(s"Expected Archived or Created but got ${ev.event}")
    }
  }

  private def fromTransaction(triggerIds: TriggerIds, t: Transaction): Either[String, SValue] = {
    val messageTy = triggerIds.getId("Message")
    val transactionTy = triggerIds.getId("Transaction")
    for {
      name <- Name.fromString("MTransaction")
      transactionId = fromTransactionId(triggerIds, t.transactionId)
      commandId = fromOptionalCommandId(triggerIds, t.commandId)
      events <- FrontStack(t.events).traverseU(fromEvent(triggerIds, _)).map(SList)
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

  private def toAnyContractId(v: SValue): Either[String, AnyContractId] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          templateId <- toTemplateTypeRep(vals.get(0))
          contractId <- toText(vals.get(1))
        } yield AnyContractId(templateId, contractId)
      }
      case _ => Left(s"Expected AnyContractId but got $v")
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
        vals.get(0) match {
          case SRecord(_, _, vals) if vals.size == 1 =>
            vals.get(0) match {
              case SAny(_, tpl) =>
                for {
                  templateId <- extractTemplateId(tpl)
                  templateArg <- toLedgerRecord(tpl)
                } yield CreateCommand(Some(toApiIdentifier(templateId)), Some(templateArg))
              case v => Left(s"Expected Any but got $v")
            }
          case v => Left(s"Expected AnyTemplate but got $v")
        }
      }
      case _ => Left(s"Expected CreateCommand but got $v")
    }
  }

  private def toExercise(triggerIds: TriggerIds, v: SValue): Either[String, ExerciseCommand] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          anyContractId <- toAnyContractId(vals.get(0))
          choiceVal <- vals.get(1) match {
            case SRecord(_, _, vals) if vals.size == 1 =>
              vals.get(0) match {
                case SAny(_, choiceVal) => Right(choiceVal)
                case v => Left(s"Expected Any but got $v")
              }
            case v => Left(s"Expected Any but got $v")
          }
          choiceName <- extractChoiceName(choiceVal)
          choiceArg <- toLedgerValue(choiceVal)
        } yield {
          ExerciseCommand(
            Some(toApiIdentifier(anyContractId.templateId)),
            anyContractId.contractId,
            choiceName,
            Some(choiceArg))
        }
      }
      case _ => Left(s"Expected ExerciseCommand but got $v")
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
      triggerIds: TriggerIds,
      createdEvents: Seq[CreatedEvent]): Either[String, SValue] = {
    val activeContractsTy = triggerIds.getId("ActiveContracts")
    for {
      events <- FrontStack(createdEvents).traverseU(fromCreatedEvent(triggerIds, _)).map(SList)
    } yield record(activeContractsTy, ("activeContracts", events))
  }

  def fromDar(dar: Dar[(PackageId, Package)]): Converter = {
    val triggerIds = TriggerIds.fromDar(dar)
    Converter(
      fromTransaction(triggerIds, _),
      fromCompletion(triggerIds, _),
      fromACS(triggerIds, _),
      toCommands(triggerIds, _)
    )
  }
}
