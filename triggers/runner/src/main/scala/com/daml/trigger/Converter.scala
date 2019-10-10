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
    fromTransaction: Transaction => SValue,
    fromCompletion: Completion => SValue,
    fromACS: Seq[CreatedEvent] => SValue,
    toCommands: SValue => Either[String, (String, Seq[Command])]
)

// Helper to create identifiers pointing to the DAML.Trigger module
case class TriggerIds(
    triggerPackageId: PackageId,
    triggerModuleName: ModuleName,
    stdlibPackageId: PackageId,
    mainPackageId: PackageId) {
  def getId(n: String): Identifier =
    Identifier(triggerPackageId, QualifiedName(triggerModuleName, DottedName.assertFromString(n)))
}

object TriggerIds {
  def fromDar(dar: Dar[(PackageId, Package)]): TriggerIds = {
    val triggerModuleName = DottedName.assertFromString("Daml.Trigger.LowLevel")
    // We might want to just fix this at compile time at some point
    // once we ship the trigger lib with the SDK.
    val triggerPackageId: PackageId = dar.all
      .find {
        case (pkgId, pkg) => pkg.modules.contains(triggerModuleName)
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
    TriggerIds(triggerPackageId, triggerModuleName, stdlibPackageId, dar.main._1)
  }
}

case class AnyContractId(templateId: Identifier, contractId: String)

object Converter {
  // Helper to make constructing an SRecord more convenient
  private def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = Name.Array(fields.map({ case (n, _) => Name.assertFromString(n) }): _*)
    val args = new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  private def toLedgerRecord(v: SValue) = {
    lfValueToApiRecord(
      true,
      v.toValue.mapContractId {
        case rcoid: RelativeContractId =>
          throw new RuntimeException(s"Unexpected contract id $rcoid")
        case acoid: AbsoluteContractId => acoid
      }
    )
  }
  private def toLedgerValue(v: SValue) = {
    lfValueToApiValue(
      true,
      v.toValue.mapContractId {
        case rcoid: RelativeContractId =>
          throw new RuntimeException(s"Unexpected contract id $rcoid")
        case acoid: AbsoluteContractId => acoid
      }
    )
  }

  private def fromIdentifier(triggerIds: TriggerIds, id: value.Identifier): SValue = {
    val identifierTy = triggerIds.getId("Identifier")
    record(
      identifierTy,
      ("packageId", SText(id.packageId)),
      ("moduleName", SText(id.moduleName)),
      ("name", SText(id.entityName)))
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

  private def fromAnyContractId(
      triggerIds: TriggerIds,
      templateId: value.Identifier,
      contractId: String): SValue = {
    val contractIdTy = triggerIds.getId("AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromIdentifier(triggerIds, templateId)),
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

  private def fromCreatedEvent(triggerIds: TriggerIds, created: CreatedEvent): SValue = {
    val createdTy = triggerIds.getId("Created")
    val anyTemplateTyCon =
      Identifier(
        triggerIds.stdlibPackageId,
        QualifiedName(
          DottedName.assertFromString("DA.Internal.LF"),
          DottedName.assertFromString("AnyTemplate")))
    ValueValidator.validateRecord(created.getCreateArguments) match {
      case Right(createArguments) =>
        SValue.fromValue(createArguments) match {
          case r @ SRecord(tyCon, _, _) =>
            record(
              createdTy,
              ("eventId", fromEventId(triggerIds, created.eventId)),
              (
                "contractId",
                fromAnyContractId(triggerIds, created.getTemplateId, created.contractId)),
              ("argument", record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), r))))
            )
          case v => throw new RuntimeException(s"Expected record but got $v")
        }
      case Left(err) => throw err
    }
  }

  private def fromEvent(triggerIds: TriggerIds, ev: Event): SValue = {
    val eventTy = triggerIds.getId("Event")
    ev.event match {
      case Event.Event.Archived(archivedEvent) => {
        SVariant(
          eventTy,
          Name.assertFromString("ArchivedEvent"),
          fromArchivedEvent(triggerIds, archivedEvent)
        )
      }
      case Event.Event.Created(createdEvent) => {
        SVariant(
          eventTy,
          Name.assertFromString("CreatedEvent"),
          fromCreatedEvent(triggerIds, createdEvent)
        )
      }
      case _ => {
        throw new RuntimeException(s"Expected Archived or Created but got $ev.event")
      }
    }
  }

  private def fromTransaction(triggerIds: TriggerIds, t: Transaction): SValue = {
    val messageTy = triggerIds.getId("Message")
    val transactionTy = triggerIds.getId("Transaction")
    SVariant(
      messageTy,
      Name.assertFromString("MTransaction"),
      record(
        transactionTy,
        ("transactionId", fromTransactionId(triggerIds, t.transactionId)),
        ("commandId", fromOptionalCommandId(triggerIds, t.commandId)),
        ("events", SList(FrontStack(t.events.map(ev => fromEvent(triggerIds, ev)))))
      )
    )
  }

  private def fromCompletion(triggerIds: TriggerIds, c: Completion): SValue = {
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
    SVariant(
      messageTy,
      Name.assertFromString("MCompletion"),
      record(
        completionTy,
        ("commandId", fromCommandId(triggerIds, c.commandId)),
        ("status", status)
      )
    )
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
      case SRecord(_, _, vals) => {
        assert(vals.size == 3)
        for {
          packageId <- toText(vals.get(0)).flatMap(PackageId.fromString)
          moduleName <- toText(vals.get(1)).flatMap(DottedName.fromString)
          entityName <- toText(vals.get(2)).flatMap(DottedName.fromString)
        } yield Identifier(packageId, QualifiedName(moduleName, entityName))
      }
      case _ => Left(s"Expected Identifier but got $v")
    }
  }

  private def extractTemplateId(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(templateId, _, _) => Right(templateId)
      case _ => Left(s"Expected contract value but got $v")
    }
  }

  private def toAnyContractId(v: SValue): Either[String, AnyContractId] = {
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 2)
        for {
          templateId <- toIdentifier(vals.get(0))
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
      case SRecord(_, _, vals) => {
        assert(vals.size == 1)
        vals.get(0) match {
          case SRecord(_, _, vals) =>
            assert(vals.size == 1)
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
      case SRecord(_, _, vals) => {
        assert(vals.size == 2)
        for {
          anyContractId <- toAnyContractId(vals.get(0))
          choiceName <- extractChoiceName(vals.get(1))
          choiceArg <- toLedgerValue(vals.get(1))
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
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 2)
        for {
          commandId <- toCommandId(vals.get(0))
          commands <- vals.get(1) match {
            case SList(cmdValues) => cmdValues.traverseU(v => toCommand(triggerIds, v))
            case _ => Left("Expected List but got ${vals.get(1)}")
          }
        } yield (commandId, commands.toImmArray.toSeq)
      }
      case _ => Left("Expected Commands but got $v")
    }
  }

  private def fromACS(triggerIds: TriggerIds, createdEvents: Seq[CreatedEvent]): SValue = {
    val activeContractsTy = triggerIds.getId("ActiveContracts")
    record(
      activeContractsTy,
      ("activeContracts", SList(FrontStack(createdEvents.map(fromCreatedEvent(triggerIds, _))))))
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
