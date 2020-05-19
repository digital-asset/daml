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
  ): (speedy.Command.Create, Set[Value.AbsoluteContractId]) = {
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg) -> argCids
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
  ): (speedy.Command.Exercise, Set[Value.AbsoluteContractId]) = {
    val template = unsafeGetTemplate(templateId)
    val choiceArgType = unsafeGetChoiceArgType(templateId, template, choiceId)
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    val cids = contractId match {
      case acoid: Value.AbsoluteContractId => argCids + acoid
      case _ => argCids
    }
    speedy.Command.Exercise(templateId, SValue.SContractId(contractId), choiceId, arg) -> cids
  }

  @throws[PreprocessorException]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value[Value.AbsoluteContractId],
      choiceId: Ref.ChoiceName,
      argument: Value[Value.AbsoluteContractId],
  ): (speedy.Command.ExerciseByKey, Set[Value.AbsoluteContractId]) = {
    val template = unsafeGetTemplate(templateId)
    val choiceArgType = unsafeGetChoiceArgType(templateId, template, choiceId)
    val ckTtype = unsafeGetContractKeyType(templateId, template)
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    val (key, keyCids) = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    keyCids.foreach { coid =>
      fail(s"Unexpected contract id in key: $coid")
    }
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg) -> argCids
  }

  @throws[PreprocessorException]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value[Value.AbsoluteContractId],
      choiceId: Ref.ChoiceName,
      choiceArgument: Value[Value.AbsoluteContractId],
  ): (speedy.Command.CreateAndExercise, Set[Value.AbsoluteContractId]) = {
    val (createArg, createArgCids) =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), createArgument)
    val template = unsafeGetTemplate(templateId)
    val choiceArgType = unsafeGetChoiceArgType(templateId, template, choiceId)
    val (choiceArg, choiceArgCids) =
      valueTranslator.unsafeTranslateValue(choiceArgType, choiceArgument)
    speedy.Command
      .CreateAndExercise(templateId, createArg, choiceId, choiceArg) -> (createArgCids | choiceArgCids)
  }

  @throws[PreprocessorException]
  private[preprocessing] def unsafePreprocessLookupByKey(
      templateId: Ref.ValueRef,
      contractKey: Value[Nothing],
  ): speedy.Command.LookupByKey = {
    val template = unsafeGetTemplate(templateId)
    val ckTtype = unsafeGetContractKeyType(templateId, template)
    val (key, keyCids) = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    keyCids.foreach { coid =>
      fail(s"Unexpected contract id in key: $coid")
    }
    speedy.Command.LookupByKey(templateId, key)
  }

  @throws[PreprocessorException]
  def unsafePreprocessCommands(
      cmds: ImmArray[command.Command],
  ): (ImmArray[speedy.Command], Set[Value.AbsoluteContractId]) = {

    var cids = Set.empty[Value.AbsoluteContractId]

    @inline
    def handleNewCids[X](tuple: (X, Set[Value.AbsoluteContractId])) = {
      val (cmd, newCids) = tuple
      cids = cids | newCids
      cmd
    }

    @tailrec
    def go(
        toProcess: FrontStack[command.Command],
        processed: BackStack[speedy.Command],
    ): ImmArray[speedy.Command] = {
      toProcess match {
        case FrontStackCons(cmd, rest) =>
          val speedyCmd = cmd match {
            case command.CreateCommand(templateId, argument) =>
              handleNewCids(unsafePreprocessCreate(templateId, argument))
            case command.ExerciseCommand(templateId, contractId, choiceId, argument) =>
              handleNewCids(unsafePreprocessExercise(templateId, contractId, choiceId, argument))
            case command.ExerciseByKeyCommand(templateId, contractKey, choiceId, argument) =>
              handleNewCids(
                unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument))
            case command.CreateAndExerciseCommand(
                templateId,
                createArgument,
                choiceId,
                choiceArgument) =>
              handleNewCids(
                unsafePreprocessCreateAndExercise(
                  templateId,
                  createArgument,
                  choiceId,
                  choiceArgument))
          }
          go(rest, processed :+ speedyCmd)
        case FrontStack() =>
          processed.toImmArray
      }
    }

    go(FrontStack(cmds), BackStack.empty) -> cids
  }

}
