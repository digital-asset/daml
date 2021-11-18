// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    // See Preprocessor scala doc for more details about the following flags.
    forbidV0ContractId: Boolean,
    requireV1ContractIdSuffix: Boolean,
) {

  val valueTranslator =
    new ValueTranslator(
      interface = interface,
      forbidV0ContractId = forbidV0ContractId,
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

  // returns the speedy translation of an LF command together with all the contract IDs contains inside.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessCommand(
      cmd: command.Command
  ): speedy.Command = {
    cmd match {
      case command.CreateCommand(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument)
      case command.CreateByInterfaceCommand(interfaceId, templateId, argument) =>
        unsafePreprocessCreateByInterface(interfaceId, templateId, argument)
      case command.ExerciseCommand(templateId, contractId, choiceId, argument) =>
        unsafePreprocessExercise(templateId, contractId, choiceId, argument)
      case command.ExerciseTemplateCommand(templateId, contractId, choiceId, argument) =>
        unsafePreprocessExerciseTemplate(templateId, contractId, choiceId, argument)
      case command.ExerciseByInterfaceCommand(
            interfaceId,
            templateId,
            contractId,
            choiceId,
            argument,
          ) =>
        unsafePreprocessExerciseByInterface(interfaceId, templateId, contractId, choiceId, argument)
      case command.ExerciseByKeyCommand(templateId, contractKey, choiceId, argument) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
      case command.CreateAndExerciseCommand(
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
      case command.FetchCommand(templateId, coid) => {
        discard(handleLookup(interface.lookupTemplate(templateId)))
        val cid = valueTranslator.unsafeTranslateCid(coid)
        speedy.Command.Fetch(templateId, cid)
      }
      case command.FetchByInterfaceCommand(interfaceId, templateId, coid) => {
        discard(handleLookup(interface.lookupTemplateImplements(templateId, interfaceId)))
        val cid = valueTranslator.unsafeTranslateCid(coid)
        speedy.Command.FetchByInterface(interfaceId, templateId, cid)
      }
      case command.FetchByKeyCommand(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.LookupByKeyCommand(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.LookupByKey(templateId, sKey)
    }
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCommands(cmds: ImmArray[command.ApiCommand]): ImmArray[speedy.Command] = {

    @tailrec
    def go(
        toProcess: FrontStack[command.ApiCommand],
        processed: BackStack[speedy.Command],
    ): ImmArray[speedy.Command] = {
      toProcess match {
        case FrontStackCons(cmd, rest) =>
          val speedyCmd = unsafePreprocessCommand(cmd)
          go(rest, processed :+ speedyCmd)
        case FrontStack() =>
          processed.toImmArray
      }
    }

    go(cmds.toFrontStack, BackStack.empty)
  }

}
