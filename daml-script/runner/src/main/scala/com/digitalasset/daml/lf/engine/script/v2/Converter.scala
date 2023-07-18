// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v2

import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.language.Ast._
import com.daml.lf.language.StablePackage.DA
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._

object Converter extends script.ConverterMethods {
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
        .translateValue(c.returnType, result.result)
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
    def damlTree(s: String) = scriptIds.damlScriptModule("Daml.Script.Questions.TransactionTree", s)
    def translateTreeEvent(ev: ScriptLedgerClient.TreeEvent): Either[String, SValue] = ev match {
      case ScriptLedgerClient.Created(tplId, contractId, argument) =>
        for {
          anyTemplate <- fromAnyTemplate(translator, tplId, argument)
        } yield SVariant(
          damlTree("TreeEvent"),
          Name.assertFromString("CreatedEvent"),
          0,
          record(
            damlTree("Created"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId.coid)),
            ("argument", anyTemplate),
          ),
        )
      case ScriptLedgerClient.Exercised(
            tplId,
            ifaceId,
            contractId,
            choiceName,
            arg,
            childEvents,
          ) =>
        for {
          evs <- childEvents.traverse(translateTreeEvent(_))
          anyChoice <- fromAnyChoice(lookupChoice, translator, tplId, ifaceId, choiceName, arg)
        } yield SVariant(
          damlTree("TreeEvent"),
          Name.assertFromString("ExercisedEvent"),
          1,
          record(
            damlTree("Exercised"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId.coid)),
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
    def scriptCommands(s: String) = scriptIds.damlScriptModule("Daml.Script.Commands", s)
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
          translated <- translateExerciseResult(lookupChoice, translator, r)
        } yield SVariant(
          scriptCommands("CommandResult"),
          Ref.Name.assertFromString("ExerciseResult"),
          1,
          translated,
        )
    }
  }

  // Convert an active contract to AnyTemplate
  def fromContract(
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract,
  ): Either[String, SValue] = fromAnyTemplate(translator, contract.templateId, contract.argument)

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract,
  ): Either[String, SValue] = {
    for {
      anyTpl <- fromContract(translator, contract)
    } yield record(DA.Types.Tuple2, ("_1", SContractId(contract.contractId)), ("_2", anyTpl))
  }

  def fromTransactionTree(
      tree: TransactionTree
  ): Either[String, ScriptLedgerClient.TransactionTree] = {
    def convEvent(ev: String): Either[String, ScriptLedgerClient.TreeEvent] =
      tree.eventsById.get(ev).toRight(s"Event id $ev does not exist").flatMap { event =>
        event.kind match {
          case TreeEvent.Kind.Created(created) =>
            for {
              tplId <- Converter.fromApiIdentifier(created.getTemplateId)
              cid <- ContractId.fromString(created.contractId)
              arg <-
                NoLoggingValueValidator
                  .validateRecord(created.getCreateArguments)
                  .left
                  .map(err => s"Failed to validate create argument: $err")
            } yield ScriptLedgerClient.Created(
              tplId,
              cid,
              arg,
            )
          case TreeEvent.Kind.Exercised(exercised) =>
            for {
              tplId <- Converter.fromApiIdentifier(exercised.getTemplateId)
              ifaceId <- exercised.interfaceId.traverse(Converter.fromApiIdentifier)
              cid <- ContractId.fromString(exercised.contractId)
              choice <- ChoiceName.fromString(exercised.choice)
              choiceArg <- NoLoggingValueValidator
                .validateValue(exercised.getChoiceArgument)
                .left
                .map(err => s"Failed to validate exercise argument: $err")
              childEvents <- exercised.childEventIds.toList.traverse(convEvent(_))
            } yield ScriptLedgerClient.Exercised(
              tplId,
              ifaceId,
              cid,
              choice,
              choiceArg,
              childEvents,
            )
          case TreeEvent.Kind.Empty => throw new RuntimeException("foo")
        }
      }
    for {
      rootEvents <- tree.rootEventIds.toList.traverse(convEvent(_))
    } yield {
      ScriptLedgerClient.TransactionTree(rootEvents)
    }
  }

  final case class Question[A](
      name: VariantConName,
      payload: A,
      continue: SValue,
      stackTrace: StackTrace,
  ) extends script.Script.FailableCmd {
    override def description = name.toString
  }

  def identifierToVariantConName(identifier: Identifier): VariantConName =
    identifier.qualifiedName.name.segments.last

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
              ArrayList(
                SRecord(_, _, ArrayList(payload @ SRecord(name, _, _), locations, continue))
              ),
            ),
          ) =>
        for {
          stackTrace <- toStackTrace(ctx.knownPackages, locations)
        } yield Right(Question(identifierToVariantConName(name), payload, continue, stackTrace))
      case SVariant(_, "Pure", _, v) => Right(Left(v))
      case _ => Left(s"Expected Free Question or Pure, got $v")
    }

  def toCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      case SVariant(_, "Create", _, SRecord(_, _, ArrayList(anyTemplateSValue))) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
        } yield command.ApiCommand.Create(
          templateId = anyTemplate.ty,
          argument = anyTemplate.arg.toUnnormalizedValue,
        )
      case SVariant(
            _,
            "Exercise",
            _,
            SRecord(_, _, ArrayList(tIdSValue, cIdSValue, anyChoiceSValue)),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          cid <- toContractId(cIdSValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
        } yield command.ApiCommand.Exercise(
          typeId = typeId,
          contractId = cid,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      case SVariant(
            _,
            "ExerciseByKey",
            _,
            SRecord(_, _, ArrayList(tIdSValue, anyKeySValue, anyChoiceSValue)),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          anyKey <- toAnyContractKey(anyKeySValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
        } yield command.ApiCommand.ExerciseByKey(
          templateId = typeId,
          contractKey = anyKey.key.toUnnormalizedValue,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      case SVariant(
            _,
            "CreateAndExercise",
            _,
            SRecord(_, _, ArrayList(anyTemplateSValue, anyChoiceSValue)),
          ) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
        } yield command.ApiCommand.CreateAndExercise(
          templateId = anyTemplate.ty,
          createArgument = anyTemplate.arg.toUnnormalizedValue,
          choiceId = anyChoice.name,
          choiceArgument = anyChoice.arg.toUnnormalizedValue,
        )
      case _ => Left(s"Expected command but got $v")
    }

  // Encodes as Daml.Script.Questions.Packages.PackageName
  def fromReadablePackageId(
      scriptIds: ScriptIds,
      packageName: ScriptLedgerClient.ReadablePackageId,
  ): SValue = {
    val packageNameTy = scriptIds.damlScriptModule("Daml.Script.Questions.Packages", "PackageName")
    record(
      packageNameTy,
      ("name", SText(packageName.name.toString)),
      ("version", SText(packageName.version.toString)),
    )
  }
}
