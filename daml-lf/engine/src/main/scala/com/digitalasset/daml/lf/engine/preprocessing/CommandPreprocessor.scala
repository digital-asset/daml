// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec

private[lf] final class CommandPreprocessor(
    interface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
) {

  val valueTranslator =
    new ValueTranslator(
      interface = interface,
      requireV1ContractIdSuffix = requireV1ContractIdSuffix,
    )

  import Preprocessor._

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value,
  ): speedy.Command.Create = {
    discard(handleLookup(interface.lookupTemplate(templateId)))
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateByInterface(
      interfaceId: Ref.Identifier,
      templateId: Ref.Identifier,
      argument: Value,
  ): speedy.Command.CreateByInterface = {
    discard(handleLookup(interface.lookupTemplateImplements(templateId, interfaceId)))
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.CreateByInterface(interfaceId, templateId, arg)
  }

  def unsafePreprocessExercise(
      identifier: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command = {
    import language.PackageInterface.ChoiceInfo

    val cid = valueTranslator.unsafeTranslateCid(contractId)
    def command(
        choice: Ast.TemplateChoiceSignature,
        toSpeedyCommand: speedy.SValue => speedy.Command,
    ) = {
      val arg = valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument)
      toSpeedyCommand(arg)
    }

    handleLookup(interface.lookupChoice(identifier, choiceId)) match {
      case ChoiceInfo.Template(choice) =>
        command(choice, speedy.Command.Exercise(identifier, cid, choiceId, _))
      case ChoiceInfo.Interface(choice) =>
        command(choice, speedy.Command.ExerciseInterface(identifier, cid, choiceId, _))
      case ChoiceInfo.Inherited(ifaceId, choice) =>
        command(choice, speedy.Command.ExerciseByInterface(ifaceId, identifier, cid, choiceId, _))
    }
  }

  /* Like unsafePreprocessExercise, but expects the choice to come from the template specifically, not inherited from an interface. */
  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseTemplate(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command.Exercise = {
    val cid = valueTranslator.unsafeTranslateCid(contractId)
    val choiceArgType = handleLookup(
      interface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val arg = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    speedy.Command.Exercise(templateId, cid, choiceId, arg)
  }

  /* Like unsafePreprocessExercise, but expects the choice to be inherited from the given interface. */
  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByInterface(
      interfaceId: Ref.Identifier,
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command.ExerciseByInterface = {
    val cid = valueTranslator.unsafeTranslateCid(contractId)
    val choiceArgType = handleLookup(
      interface.lookupInheritedChoice(interfaceId, templateId, choiceId)
    ).argBinder._2
    val arg = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    speedy.Command.ExerciseByInterface(interfaceId, templateId, cid, choiceId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(
      interface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
    val arg = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    val key = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value,
      choiceId: Ref.ChoiceName,
      choiceArgument: Value,
  ): speedy.Command.CreateAndExercise = {
    val createArg =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), createArgument)
    val choiceArgType = handleLookup(
      interface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg =
      valueTranslator.unsafeTranslateValue(choiceArgType, choiceArgument)
    speedy.Command
      .CreateAndExercise(
        templateId,
        createArg,
        choiceId,
        choiceArg,
      )
  }

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessLookupByKey(
      templateId: Ref.ValueRef,
      contractKey: Value,
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
    val key = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    speedy.Command.LookupByKey(templateId, key)
  }

  // returns the speedy translation of an API command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessApiCommand(
      cmd: command.ApiCommand
  ): speedy.Command =
    cmd match {
      case command.ApiCommand.Create(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument)
      case command.ApiCommand.Exercise(templateId, contractId, choiceId, argument) =>
        unsafePreprocessExercise(templateId, contractId, choiceId, argument)
      case command.ApiCommand.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
      case command.ApiCommand.CreateAndExercise(
            templateId,
            createArgument,
            choiceId,
            choiceArgument,
          ) =>
        unsafePreprocessCreateAndExercise(
          templateId,
          createArgument,
          choiceId,
          choiceArgument,
        )
    }

  // returns the speedy translation of an Replay command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessReplayCommand(
      cmd: command.ReplayCommand
  ): speedy.Command =
    cmd match {
      case command.ReplayCommand.CreateByTemplate(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument)
      case command.ReplayCommand.CreateByInterface(interfaceId, templateId, argument) =>
        unsafePreprocessCreateByInterface(interfaceId, templateId, argument)
      case command.ReplayCommand.LenientExercise(templateId, coid, choiceId, argument) =>
        // https://github.com/digital-asset/daml/issues/12051
        //   We temporary add a lenient version of Exercise acting like ApiCommand.Exercise.
        //   It will be shipped in 2.1 but dropped in 2.2.
        unsafePreprocessExercise(templateId, coid, choiceId, argument)
      case command.ReplayCommand.ExerciseTemplate(templateId, contractId, choiceId, argument) =>
        unsafePreprocessExerciseTemplate(templateId, contractId, choiceId, argument)
      case command.ReplayCommand.ExerciseByInterface(
            interfaceId,
            templateId,
            contractId,
            choiceId,
            argument,
          ) =>
        unsafePreprocessExerciseByInterface(interfaceId, templateId, contractId, choiceId, argument)
      case command.ReplayCommand.ExerciseTemplateByKey(
            templateId,
            contractKey,
            choiceId,
            argument,
          ) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
      case command.ReplayCommand.FetchTemplate(templateId, coid) => {
        discard(handleLookup(interface.lookupTemplate(templateId)))
        val cid = valueTranslator.unsafeTranslateCid(coid)
        speedy.Command.Fetch(templateId, cid)
      }
      case command.ReplayCommand.FetchByInterface(interfaceId, templateId, coid) => {
        discard(handleLookup(interface.lookupTemplateImplements(templateId, interfaceId)))
        val cid = valueTranslator.unsafeTranslateCid(coid)
        speedy.Command.FetchByInterface(interfaceId, templateId, cid)
      }
      case command.ReplayCommand.FetchTemplateByKey(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupTemplateByKey(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.LookupByKey(templateId, sKey)
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(cmds: ImmArray[command.ApiCommand]): ImmArray[speedy.Command] = {

    @tailrec
    def go(
        toProcess: FrontStack[command.ApiCommand],
        processed: BackStack[speedy.Command],
    ): ImmArray[speedy.Command] = {
      toProcess match {
        case FrontStackCons(cmd, rest) =>
          val speedyCmd = unsafePreprocessApiCommand(cmd)
          go(rest, processed :+ speedyCmd)
        case FrontStack() =>
          processed.toImmArray
      }
    }

    go(cmds.toFrontStack, BackStack.empty)
  }

}
