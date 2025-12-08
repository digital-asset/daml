// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import com.daml.ledger.api.v2.event.{Event, ExercisedEvent}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.javaapi.data.{Transaction => JavaTransaction}
import com.daml.ledger.javaapi.data.{ExercisedEvent => JavaExercisedEvent}
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.Speedy.Machine.{
  ExtendedValue,
  ExtendedValueClosureBlob,
  ExtendedValueAny,
  ExtendedValueTypeRep,
}
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import scala.jdk.CollectionConverters._
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._

object Converter extends script.ConverterMethods(StablePackagesV2) {
  import com.digitalasset.daml.lf.script.converter.Converter._

  def translateTransactionTree(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      scriptIds: ScriptIds,
      tree: ScriptLedgerClient.TransactionTree,
  ): Either[String, ExtendedValue] = {
    def damlTree(s: String) =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.TransactionTree", s)
    def translateTreeEvent(ev: ScriptLedgerClient.TreeEvent): Either[String, ExtendedValue] =
      ev match {
        case ScriptLedgerClient.Created(tplId, contractId, argument, _) =>
          Right(
            ValueVariant(
              Some(damlTree("TreeEvent")),
              Name.assertFromString("CreatedEvent"),
              record(
                damlTree("Created"),
                ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId)),
                ("argument", fromAnyTemplate(tplId, argument)),
              ),
            )
          )
        case ScriptLedgerClient.Exercised(
              tplId,
              ifaceId,
              contractId,
              choiceName,
              arg,
              _, // Result cannot be encoded in daml without some kind of `AnyChoiceResult` type, likely using the `Choice` constraint to unpack.
              childEvents,
            ) =>
          for {
            evs <- childEvents.traverse(translateTreeEvent(_))
            anyChoice <- fromAnyChoice(
              lookupChoice,
              tplId,
              ifaceId,
              choiceName,
              arg,
            )
          } yield ValueVariant(
            Some(damlTree("TreeEvent")),
            Name.assertFromString("ExercisedEvent"),
            record(
              damlTree("Exercised"),
              ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId)),
              ("choice", ValueText(choiceName)),
              ("argument", anyChoice),
              ("childEvents", ValueList(evs.to(FrontStack))),
            ),
          )
      }
    for {
      events <- tree.rootEvents.traverse(translateTreeEvent(_)): Either[String, List[ExtendedValue]]
    } yield record(
      damlTree("TransactionTree"),
      ("rootEvents", ValueList(events.to(FrontStack))),
    )
  }

  def fromCommandResult(
      scriptIds: ScriptIds,
      commandResult: ScriptLedgerClient.CommandResult,
  ): ExtendedValue = {
    def scriptCommands(s: String) =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Commands", s)
    commandResult match {
      case ScriptLedgerClient.CreateResult(contractId) =>
        ValueVariant(
          Some(scriptCommands("CommandResult")),
          Ref.Name.assertFromString("CreateResult"),
          ValueContractId(contractId),
        )
      case r: ScriptLedgerClient.ExerciseResult =>
        ValueVariant(
          Some(scriptCommands("CommandResult")),
          Ref.Name.assertFromString("ExerciseResult"),
          r.result,
        )
    }
  }

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      contract: ScriptLedgerClient.ActiveContract,
      targetTemplateId: Identifier,
  ): ExtendedValue = {
    makeTuple(
      ValueContractId(contract.contractId),
      fromAnyTemplate(
        targetTemplateId,
        contract.argument,
      ),
    )
  }

  def fromTransaction(
      tx: Transaction,
      intendedPackageIds: List[PackageId],
  ): Either[String, ScriptLedgerClient.TransactionTree] = {
    def convEvent(
        ev: Int,
        oIntendedPackageId: Option[PackageId],
    ): Either[String, ScriptLedgerClient.TreeEvent] = {
      val javaTx = JavaTransaction.fromProto(Transaction.toJavaProto(tx))

      javaTx.getEventsById.asScala.get(ev).toRight(s"Event id $ev does not exist").flatMap {
        event =>
          Event.fromJavaProto(event.toProtoEvent).event match {
            case Event.Event.Created(created) =>
              for {
                tplId <- Converter.fromApiIdentifier(created.getTemplateId)
                cid <- ContractId.fromString(created.contractId)
                arg <-
                  NoLoggingValueValidator
                    .validateRecord(created.getCreateArguments)
                    .left
                    .map(err => s"Failed to validate create argument: $err")
              } yield ScriptLedgerClient.Created(
                oIntendedPackageId
                  .fold(tplId)(intendedPackageId => tplId.copy(pkg = intendedPackageId)),
                cid,
                arg,
                Bytes.fromByteString(created.createdEventBlob),
              )
            case Event.Event.Exercised(exercised) =>
              for {
                tplId <- Converter.fromApiIdentifier(exercised.getTemplateId)
                ifaceId <- exercised.interfaceId.traverse(Converter.fromApiIdentifier)
                cid <- ContractId.fromString(exercised.contractId)
                choice <- ChoiceName.fromString(exercised.choice)
                choiceArg <- NoLoggingValueValidator
                  .validateValue(exercised.getChoiceArgument)
                  .left
                  .map(err => s"Failed to validate exercise argument: $err")
                choiceResult <- NoLoggingValueValidator
                  .validateValue(exercised.getExerciseResult)
                  .left
                  .map(_.toString)
                childEvents <- javaTx
                  .getChildNodeIds(
                    JavaExercisedEvent.fromProto(ExercisedEvent.toJavaProto(exercised))
                  )
                  .asScala
                  .toList
                  .traverse(convEvent(_, None))
              } yield ScriptLedgerClient.Exercised(
                oIntendedPackageId
                  .fold(tplId)(intendedPackageId => tplId.copy(pkg = intendedPackageId)),
                ifaceId,
                cid,
                choice,
                choiceArg,
                choiceResult,
                childEvents,
              )
            case Event.Event.Archived(_) =>
              throw new RuntimeException(
                "Unexpected archived event in transaction with LedgerEffects shape"
              )
            case Event.Event.Empty =>
              throw new RuntimeException("Unexpected empty event encountered in transaction")
          }
      }
    }
    for {
      rootEvents <- JavaTransaction
        .fromProto(Transaction.toJavaProto(tx))
        .getRootNodeIds()
        .asScala
        .toList
        .zip(intendedPackageIds)
        .traverse { case (nodeId, intendedPackageId) =>
          convEvent(nodeId, Some(intendedPackageId))
        }
    } yield {
      ScriptLedgerClient.TransactionTree(rootEvents)
    }
  }

  // final case class Question[A](
  //     name: String,
  //     version: Int,
  //     payload: A,
  //     stackTrace: StackTrace,
  //     continue: SValue,
  // )

  def toPackageId(v: ExtendedValue): Either[String, PackageId] =
    v match {
      case ValueRecord(_, ImmArray((_, ValueText(packageId)))) =>
        Right(PackageId.assertFromString(packageId))
      case _ => Left(s"Expected PackageId but got $v")
    }

  def toCommandWithMeta(v: ExtendedValue): Either[String, ScriptLedgerClient.CommandWithMeta] =
    v match {
      case ValueRecord(_, ImmArray((_, command), (_, ValueBool(explicitPackageId)))) =>
        for {
          command <- toCommand(command)
        } yield ScriptLedgerClient.CommandWithMeta(command, explicitPackageId)
      case _ => Left(s"Expected CommandWithMeta but got $v")
    }

  def castCommandExtendedValue(value: ExtendedValue): Either[String, Value] =
    castExtendedValue(
      value,
      (_: ExtendedValueClosureBlob) => Left(new RuntimeException("Illegal Value Blob in command!")),
      (_: ExtendedValueAny) => Left(new RuntimeException("Illegal Value Any in command!")),
      (_: ExtendedValueTypeRep) => Left(new RuntimeException("Illegal Value TypeRep in command!")),
    ).left.map(_.getMessage)

  def toCommand(v: ExtendedValue): Either[String, command.ApiCommand] =
    v match {
      case ValueVariant(_, "Create", ValueRecord(_, ImmArray((_, anyTemplateSValue)))) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
          argument <- castCommandExtendedValue(anyTemplate.arg)
        } yield command.ApiCommand.Create(
          templateRef = anyTemplate.ty.toRef,
          argument = argument,
        )
      case ValueVariant(
            _,
            "Exercise",
            ValueRecord(_, ImmArray((_, tIdSValue), (_, cIdSValue), (_, anyChoiceSValue))),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          cid <- toContractId(cIdSValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
          argument <- castCommandExtendedValue(anyChoice.arg)
        } yield command.ApiCommand.Exercise(
          typeRef = typeId.toRef,
          contractId = cid,
          choiceId = anyChoice.name,
          argument = argument,
        )
      case ValueVariant(
            _,
            "ExerciseByKey",
            ValueRecord(_, ImmArray((_, tIdSValue), (_, anyKeySValue), (_, anyChoiceSValue))),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          anyKey <- toAnyContractKey(anyKeySValue)
          contractKey <- castCommandExtendedValue(anyKey.key)
          anyChoice <- toAnyChoice(anyChoiceSValue)
          argument <- castCommandExtendedValue(anyChoice.arg)
        } yield command.ApiCommand.ExerciseByKey(
          templateRef = typeId.toRef,
          contractKey = contractKey,
          choiceId = anyChoice.name,
          argument = argument,
        )
      case ValueVariant(
            _,
            "CreateAndExercise",
            ValueRecord(_, ImmArray((_, anyTemplateSValue), (_, anyChoiceSValue))),
          ) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
          createArgument <- castCommandExtendedValue(anyTemplate.arg)
          anyChoice <- toAnyChoice(anyChoiceSValue)
          choiceArgument <- castCommandExtendedValue(anyChoice.arg)
        } yield command.ApiCommand.CreateAndExercise(
          templateRef = anyTemplate.ty.toRef,
          createArgument = createArgument,
          choiceId = anyChoice.name,
          choiceArgument = choiceArgument,
        )
      case _ => Left(s"Expected command but got $v")
    }

  // Encodes as Daml.Script.Internal.Questions.Packages.PackageName
  def fromReadablePackageId(
      scriptIds: ScriptIds,
      packageName: ScriptLedgerClient.ReadablePackageId,
  ): ExtendedValue = {
    val packageNameTy =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Packages", "PackageName")
    record(
      packageNameTy,
      ("name", ValueText(packageName.name.toString)),
      ("version", ValueText(packageName.version.toString)),
    )
  }
}
