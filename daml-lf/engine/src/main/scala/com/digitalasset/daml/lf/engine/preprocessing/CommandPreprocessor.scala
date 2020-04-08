// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value

import scala.annotation.tailrec

private[preprocessing] final class CommandPreprocessor(compiledPackages: MutableCompiledPackages) {

  import Preprocessor._

  val valueTranslator = new ValueTranslator(compiledPackages)

  @throws[PreprocessorException]
  private def unsafeGetPackage(pkgId: Ref.PackageId) =
    compiledPackages.getPackage(pkgId).getOrElse(throw PreprocessorMissingPackage(pkgId))

  @throws[PreprocessorException]
  private def unsafeGetTemplate(templateId: Ref.Identifier) =
    assertRight(
      PackageLookup.lookupTemplate(
        unsafeGetPackage(templateId.packageId),
        templateId.qualifiedName
      ))

  @throws[PreprocessorException]
  private def unsafeGetChoiceArgType(
      tmplId: Ref.Identifier,
      tmpl: Ast.Template,
      choiceId: Ref.ChoiceName) =
    tmpl.choices.get(choiceId) match {
      case Some(choice) => choice.argBinder._2
      case None =>
        val choiceNames = tmpl.choices.toList.map(_._1)
        fail(
          s"Couldn't find requested choice $choiceId for template $tmplId. Available choices: $choiceNames"
        )
    }

  @throws[PreprocessorException]
  private def unsafeGetContractKeyType(tmplId: Ref.Identifier, tmpl: Ast.Template) =
    tmpl.key match {
      case Some(ck) => ck.typ
      case None =>
        fail(s"Impossible to exercise by key, no key is defined for template $tmplId")
    }

  @throws[PreprocessorException]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value[Value.AbsoluteContractId],
  ): speedy.Command.Create = {
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg)
  }

  def unsafePreprocessFetch(
      templateId: Ref.Identifier,
      coid: Value.AbsoluteContractId,
  ): speedy.Command.Fetch =
    speedy.Command.Fetch(templateId, SValue.SContractId(coid))

  @throws[PreprocessorException]
  def unsafePreprocessExercise(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value[Value.AbsoluteContractId],
  ): speedy.Command.Exercise = {
    val template = unsafeGetTemplate(templateId)
    val choiceArgType = unsafeGetChoiceArgType(templateId, template, choiceId)
    val arg = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    speedy.Command.Exercise(templateId, SValue.SContractId(contractId), choiceId, arg)
  }

  @throws[PreprocessorException]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value[Value.AbsoluteContractId],
      choiceId: Ref.ChoiceName,
      argument: Value[Value.AbsoluteContractId],
  ): speedy.Command.ExerciseByKey = {
    val template = unsafeGetTemplate(templateId)
    val choiceArgType = unsafeGetChoiceArgType(templateId, template, choiceId)
    val ckTtype = unsafeGetContractKeyType(templateId, template)
    val arg = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    val key = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg)
  }

  @throws[PreprocessorException]
  private[preprocessing] def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value[Value.AbsoluteContractId],
      choiceId: Ref.ChoiceName,
      choiceArgument: Value[Value.AbsoluteContractId],
  ): speedy.Command.CreateAndExercise = {
    val createArg =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), createArgument)
    val template = unsafeGetTemplate(templateId)
    val choiceArgType = unsafeGetChoiceArgType(templateId, template, choiceId)
    val choiceArg =
      valueTranslator.unsafeTranslateValue(choiceArgType, choiceArgument)
    speedy.Command
      .CreateAndExercise(templateId, createArg, choiceId, choiceArg)
  }

  @throws[PreprocessorException]
  private[preprocessing] def unsafePreprocessLookupByKey(
      templateId: Ref.ValueRef,
      contractKey: Value[Nothing],
  ): speedy.Command.LookupByKey = {
    val template = unsafeGetTemplate(templateId)
    val ckTtype = unsafeGetContractKeyType(templateId, template)
    val key = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    speedy.Command.LookupByKey(templateId, key)
  }

  @throws[PreprocessorException]
  def unsafePreprocessCommands(
      cmds: ImmArray[command.Command],
  ): ImmArray[speedy.Command] = {
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
        toProcess: FrontStack[command.Command],
        processed: BackStack[speedy.Command],
    ): ImmArray[speedy.Command] = {
      toProcess match {
        case FrontStackCons(cmd, rest) =>
          val speedyCmd = cmd match {
            case command.CreateCommand(templateId, argument) =>
              unsafePreprocessCreate(templateId, argument)
            case command.ExerciseCommand(templateId, contractId, choiceId, argument) =>
              unsafePreprocessExercise(templateId, contractId, choiceId, argument)
            case command.ExerciseByKeyCommand(templateId, contractKey, choiceId, argument) =>
              unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
            case command.CreateAndExerciseCommand(
                templateId,
                createArgument,
                choiceId,
                choiceArgument) =>
              unsafePreprocessCreateAndExercise(
                templateId,
                createArgument,
                choiceId,
                choiceArgument)
          }
          go(rest, processed :+ speedyCmd)
        case FrontStack() =>
          processed.toImmArray
      }
    }
    go(FrontStack(cmds), BackStack.empty)
  }

}
