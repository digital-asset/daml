// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.{SValue, Command => SpeedyCommand}
import com.digitalasset.daml.lf.value.Value._

import scala.annotation.tailrec

private[engine] class CommandPreprocessor(compiledPackages: ConcurrentCompiledPackages) {

  private val valueTranslator = new ValueTranslator(compiledPackages)

  private[engine] def translateValue(
      ty0: Type,
      v0: VersionedValue[AbsoluteContractId]): Result[SValue] = {

    valueTranslator.translateValue(ty0, v0) match {
      case ResultNeedPackage(pkgId, resume) =>
        ResultNeedPackage(
          pkgId, {
            case None => ResultError(Error(s"Couldn't find package $pkgId"))
            case Some(pkg) =>
              compiledPackages.addPackage(pkgId, pkg).flatMap(_ => resume(Some(pkg)))
          }
        )
      case result =>
        result
    }
  }

  private[engine] def preprocessCreate(
      templateId: Identifier,
      argument: VersionedValue[AbsoluteContractId]): Result[(Type, SpeedyCommand)] =
    Result.needDataType(
      compiledPackages,
      templateId,
      dataType => {
        // we rely on datatypes which are also templates to have _no_ parameters, according
        // to the DAML-LF spec.
        if (dataType.params.length > 0) {
          ResultError(Error(
            s"Unexpected type parameters ${dataType.params} for template $templateId. Template datatypes should never have parameters."))
        } else {
          val typ = TTyCon(templateId)
          translateValue(typ, argument).map(typ -> SpeedyCommand.Create(templateId, _))
        }
      }
    )

  private[engine] def preprocessFetch(
      templateId: Identifier,
      coid: AbsoluteContractId): Result[(Type, SpeedyCommand)] =
    Result.needDataType(
      compiledPackages,
      templateId,
      dataType => {
        // we rely on datatypes which are also templates to have _no_ parameters, according
        // to the DAML-LF spec.
        if (dataType.params.length > 0) {
          ResultError(Error(
            s"Unexpected type parameters ${dataType.params} for template $templateId. Template datatypes should never have parameters."))
        } else {
          val typ = TTyCon(templateId)
          ResultDone(typ -> SpeedyCommand.Fetch(templateId, SValue.SContractId(coid)))
        }
      }
    )

  private[engine] def preprocessExercise(
      templateId: Identifier,
      contractId: ContractId,
      choiceId: ChoiceName,
      argument: VersionedValue[AbsoluteContractId]): Result[(Type, SpeedyCommand)] =
    Result.needTemplate(
      compiledPackages,
      templateId,
      template => {
        template.choices.get(choiceId) match {
          case None =>
            val choicesNames: Seq[String] = template.choices.toList.map(_._1)
            ResultError(Error(
              s"Couldn't find requested choice $choiceId for template $templateId. Available choices: $choicesNames"))
          case Some(choice) =>
            val choiceTyp = choice.argBinder._2
            translateValue(choiceTyp, argument).map(
              choiceTyp -> SpeedyCommand
                .Exercise(templateId, SValue.SContractId(contractId), choiceId, _))
        }
      }
    )

  private[engine] def preprocessExerciseByKey(
      templateId: Identifier,
      contractKey: VersionedValue[AbsoluteContractId],
      choiceId: ChoiceName,
      argument: VersionedValue[AbsoluteContractId]): Result[(Type, SpeedyCommand)] =
    Result.needTemplate(
      compiledPackages,
      templateId,
      template => {
        (template.choices.get(choiceId), template.key) match {
          case (None, _) =>
            val choicesNames: Seq[String] = template.choices.toList.map(_._1)
            ResultError(Error(
              s"Couldn't find requested choice $choiceId for template $templateId. Available choices: $choicesNames"))
          case (_, None) =>
            ResultError(
              Error(s"Impossible to exercise by key, no key is defined for template $templateId"))
          case (Some(choice), Some(ck)) =>
            val (_, choiceType) = choice.argBinder
            for {
              arg <- translateValue(choiceType, argument)
              key <- translateValue(ck.typ, contractKey)
            } yield
              choiceType -> SpeedyCommand
                .ExerciseByKey(templateId, key, choiceId, arg)
        }
      }
    )

  private[engine] def preprocessCreateAndExercise(
      templateId: ValueRef,
      createArgument: VersionedValue[AbsoluteContractId],
      choiceId: ChoiceName,
      choiceArgument: VersionedValue[AbsoluteContractId]
  ): Result[(Type, SpeedyCommand)] = {
    Result.needDataType(
      compiledPackages,
      templateId,
      dataType => {
        // we rely on datatypes which are also templates to have _no_ parameters, according
        // to the DAML-LF spec.
        if (dataType.params.length > 0) {
          ResultError(Error(
            s"Unexpected type parameters ${dataType.params} for template $templateId. Template datatypes should never have parameters."))
        } else {
          val typ = TTyCon(templateId)
          translateValue(typ, createArgument).flatMap {
            createValue =>
              Result.needTemplate(
                compiledPackages,
                templateId,
                template => {
                  template.choices.get(choiceId) match {
                    case None =>
                      val choicesNames: Seq[String] = template.choices.toList.map(_._1)
                      ResultError(Error(
                        s"Couldn't find requested choice $choiceId for template $templateId. Available choices: $choicesNames"))
                    case Some(choice) =>
                      val choiceTyp = choice.argBinder._2
                      translateValue(choiceTyp, choiceArgument).map(
                        choiceTyp -> SpeedyCommand
                          .CreateAndExercise(templateId, createValue, choiceId, _))
                  }
                }
              )
          }
        }
      }
    )
  }

  private[engine] def preprocessCommand(cmd: Command): Result[(Type, SpeedyCommand)] =
    cmd match {
      case CreateCommand(templateId, argument) =>
        preprocessCreate(templateId, argument)
      case ExerciseCommand(templateId, contractId, choiceId, argument) =>
        preprocessExercise(templateId, AbsoluteContractId(contractId), choiceId, argument)
      case ExerciseByKeyCommand(templateId, contractKey, choiceId, argument) =>
        preprocessExerciseByKey(
          templateId,
          contractKey,
          choiceId,
          argument
        )
      case CreateAndExerciseCommand(
          templateId,
          createArgument,
          choiceId,
          choiceArgument
          ) =>
        preprocessCreateAndExercise(
          templateId,
          createArgument,
          choiceId,
          choiceArgument
        )
    }

  private[engine] def preprocessCommands(
      cmds0: Commands): Result[ImmArray[(Type, SpeedyCommand)]] = {
    // before, we had
    //
    // ```
    // Result.sequence(ImmArray(cmds.commands).map(preprocessCommand))
    // ```
    //
    // however that is bad, because it'll generate a `NeedPackage` for each command,
    // if the same package is needed for every command. If we go step by step,
    // on the other hand, we will cache the package and go through with execution
    // after the first command which demands it.
    @tailrec
    def go(
        processed: BackStack[(Type, SpeedyCommand)],
        toProcess: ImmArray[Command]): Result[ImmArray[(Type, SpeedyCommand)]] = {
      toProcess match {
        case ImmArray() => ResultDone(processed.toImmArray)
        case ImmArrayCons(cmd, cmds) =>
          preprocessCommand(cmd) match {
            case ResultDone(processedCommand) => go(processed :+ processedCommand, cmds)
            case ResultError(err) => ResultError(err)
            case ResultNeedContract(acoid, resume) =>
              ResultNeedContract(acoid, { contract =>
                resume(contract).flatMap(processedCommand =>
                  goResume(processed :+ processedCommand, cmds))
              })
            case ResultNeedPackage(pkgId, resume) =>
              ResultNeedPackage(pkgId, { pkg =>
                resume(pkg).flatMap(processedCommand =>
                  goResume(processed :+ processedCommand, cmds))
              })
            case ResultNeedKey(key, resume) =>
              ResultNeedKey(key, { contract =>
                resume(contract).flatMap(processedCommand =>
                  goResume(processed :+ processedCommand, cmds))
              })
          }
      }
    }

    def goResume(processed: BackStack[(Type, SpeedyCommand)], toProcess: ImmArray[Command]) =
      go(processed, toProcess)

    go(BackStack.empty, cmds0.commands)
  }

}
