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
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import scala.jdk.CollectionConverters._
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._

object Converter extends script.ConverterMethods(StablePackagesV2) {
  import com.daml.script.converter.Converter._

  def translateExerciseResult(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      result: ScriptLedgerClient.ExerciseResult,
  ) = {
    for {
      choice <- Name.fromString(result.choice)
      c <- lookupChoice(result.templateId, result.interfaceId, choice)
      translated <- translator
        .translateValue(
          c.returnType,
          result.result,
        )
        .left
        .map(err => s"Failed to translate exercise result: $err")
    } yield translated
  }

  def translateTransactionTree(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      scriptIds: ScriptIds,
      tree: ScriptLedgerClient.TransactionTree,
  ): Either[String, SValue] = {
    def damlTree(s: String) =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.TransactionTree", s)
    def translateTreeEvent(ev: ScriptLedgerClient.TreeEvent): Either[String, SValue] = ev match {
      case ScriptLedgerClient.Created(tplId, contractId, argument, _) =>
        for {
          anyTemplate <- fromAnyTemplate(translator, tplId, argument)
        } yield SVariant(
          damlTree("TreeEvent"),
          Name.assertFromString("CreatedEvent"),
          0,
          record(
            damlTree("Created"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId)),
            ("argument", anyTemplate),
          ),
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
            translator,
            tplId,
            ifaceId,
            choiceName,
            arg,
          )
        } yield SVariant(
          damlTree("TreeEvent"),
          Name.assertFromString("ExercisedEvent"),
          1,
          record(
            damlTree("Exercised"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId)),
            ("choice", SText(choiceName)),
            ("argument", anyChoice),
            ("childEvents", SList(evs.to(FrontStack))),
          ),
        )
    }
    for {
      events <- tree.rootEvents.traverse(translateTreeEvent(_)): Either[String, List[SValue]]
    } yield record(
      damlTree("TransactionTree"),
      ("rootEvents", SList(events.to(FrontStack))),
    )
  }

  def fromCommandResult(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      scriptIds: ScriptIds,
      commandResult: ScriptLedgerClient.CommandResult,
  ): Either[String, SValue] = {
    def scriptCommands(s: String) =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Commands", s)
    commandResult match {
      case ScriptLedgerClient.CreateResult(contractId) =>
        Right(
          SVariant(
            scriptCommands("CommandResult"),
            Ref.Name.assertFromString("CreateResult"),
            0,
            SContractId(contractId),
          )
        )
      case r: ScriptLedgerClient.ExerciseResult =>
        for {
          translated <- translateExerciseResult(
            lookupChoice,
            translator,
            r,
          )
        } yield SVariant(
          scriptCommands("CommandResult"),
          Ref.Name.assertFromString("ExerciseResult"),
          1,
          translated,
        )
    }
  }

  def fromSubmitResult[T](
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      translateError: T => SValue,
      scriptIds: ScriptIds,
      submitResult: Either[T, Seq[ScriptLedgerClient.CommandResult]],
  ): Either[String, SValue] = submitResult match {
    case Right(commandResults) =>
      commandResults
        .to(FrontStack)
        .traverse(
          fromCommandResult(lookupChoice, translator, scriptIds, _)
        )
        .map { rs =>
          SVariant(
            StablePackagesV2.Either,
            Ref.Name.assertFromString("Right"),
            1,
            SList(rs),
          )
        }
    case Left(submitError) =>
      Right(
        SVariant(
          StablePackagesV2.Either,
          Ref.Name.assertFromString("Left"),
          0,
          translateError(submitError),
        )
      )
  }

  def fromSubmitResultList[T](
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      translateError: T => SValue,
      scriptIds: ScriptIds,
      submitResultList: List[Either[T, Seq[ScriptLedgerClient.CommandResult]]],
  ): Either[String, SValue] =
    submitResultList
      .traverse(
        fromSubmitResult(
          lookupChoice,
          translator,
          translateError,
          scriptIds,
          _,
        )
      )
      .map { xs => SList(xs.to(FrontStack)) }

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract,
      targetTemplateId: Identifier,
  ): Either[String, SValue] = {
    for {
      anyTpl <- fromAnyTemplate(
        translator,
        targetTemplateId,
        contract.argument,
      )
    } yield makeTuple(
      SContractId(contract.contractId),
      anyTpl,
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

  final case class Question[A](
      name: String,
      version: Int,
      payload: A,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends script.Script.FailableCmd {
    override def description = name.toString
  }

  def toPackageId(v: SValue): Either[String, PackageId] =
    v match {
      case SRecord(_, _, Array(SText(packageId))) =>
        Right(PackageId.assertFromString(packageId))
      case _ => Left(s"Expected PackageId but got $v")
    }

  def unrollFree(ctx: ScriptF.Ctx, v: SValue): ErrorOr[SValue Either Question[SValue]] =
    // ScriptF is a newtype over the question with its payload, locations and continue. It's modelled as a record with a single field.
    // Thus the extra SRecord
    v match {
      case SVariant(
            _,
            "Free",
            _,
            SRecord(
              _,
              _,
              Array(
                SRecord(_, _, Array(SText(name), SInt64(version), payload, locations, continue))
              ),
            ),
          ) =>
        for {
          stackTrace <- toStackTrace(ctx.knownPackages, locations)
        } yield Right(Question(name, version.toInt, payload, stackTrace, continue))
      case SVariant(_, "Pure", _, v) => Right(Left(v))
      case _ => Left(s"Expected Free Question or Pure, got $v")
    }

  def toCommandWithMeta(v: SValue): Either[String, ScriptLedgerClient.CommandWithMeta] =
    v match {
      case SRecord(_, _, Array(command, SBool(explicitPackageId))) =>
        for {
          command <- toCommand(command)
        } yield ScriptLedgerClient.CommandWithMeta(command, explicitPackageId)
      case _ => Left(s"Expected CommandWithMeta but got $v")
    }

  def toCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      case SVariant(_, "Create", _, SRecord(_, _, Array(anyTemplateSValue))) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
        } yield command.ApiCommand.Create(
          templateRef = anyTemplate.ty.toRef,
          argument = anyTemplate.arg.toUnnormalizedValue,
        )
      case SVariant(
            _,
            "Exercise",
            _,
            SRecord(_, _, Array(tIdSValue, cIdSValue, anyChoiceSValue)),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          cid <- toContractId(cIdSValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
        } yield command.ApiCommand.Exercise(
          typeRef = typeId.toRef,
          contractId = cid,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      case SVariant(
            _,
            "ExerciseByKey",
            _,
            SRecord(_, _, Array(tIdSValue, anyKeySValue, anyChoiceSValue)),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          anyKey <- toAnyContractKey(anyKeySValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
        } yield command.ApiCommand.ExerciseByKey(
          templateRef = typeId.toRef,
          contractKey = anyKey.key.toUnnormalizedValue,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      case SVariant(
            _,
            "CreateAndExercise",
            _,
            SRecord(_, _, Array(anyTemplateSValue, anyChoiceSValue)),
          ) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
        } yield command.ApiCommand.CreateAndExercise(
          templateRef = anyTemplate.ty.toRef,
          createArgument = anyTemplate.arg.toUnnormalizedValue,
          choiceId = anyChoice.name,
          choiceArgument = anyChoice.arg.toUnnormalizedValue,
        )
      case _ => Left(s"Expected command but got $v")
    }

  // Encodes as Daml.Script.Internal.Questions.Packages.PackageName
  def fromReadablePackageId(
      scriptIds: ScriptIds,
      packageName: ScriptLedgerClient.ReadablePackageId,
  ): SValue = {
    val packageNameTy =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Packages", "PackageName")
    record(
      packageNameTy,
      ("name", SText(packageName.name.toString)),
      ("version", SText(packageName.version.toString)),
    )
  }
}
