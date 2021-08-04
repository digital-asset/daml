// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.nameof.NameOf

private[lf] final class CommandPreprocessor(compiledPackages: CompiledPackages) {

  import Preprocessor._
  import compiledPackages.interface

  val valueTranslator = new ValueTranslator(interface)

  private[this] def assertEmpty(
      location: String,
      templateId: Identifier,
      cids: Set[ContractId.V1],
  ) =
    cids.foreach { coid =>
      // The type checking of contractKey done by unsafeTranslateValue should ensure
      // keyCids is empty
      throw Error.Preprocessing.Internal(
        location,
        s"Unexpected contract IDs in contract key of $templateId: $coid",
      )
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value[ContractId],
  ): speedy.Command.Create = {
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg, argCids)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExercise(
      templateId: Ref.Identifier,
      contractId: ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value[ContractId],
  ): speedy.Command.Exercise = {
    val choice = handleLookup(interface.lookupChoice(templateId, choiceId)).argBinder._2
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(choice, argument)
    val cids = contractId match {
      case cid: ContractId.V1 => argCids + cid
      case _: ContractId.V0 => argCids
    }
    speedy.Command.Exercise(
      templateId,
      SValue.SContractId(contractId),
      choiceId,
      arg,
      cids,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value[ContractId],
      choiceId: Ref.ChoiceName,
      argument: Value[ContractId],
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(interface.lookupChoice(templateId, choiceId)).argBinder._2
    val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    val (key, keyCids) = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    assertEmpty(NameOf.qualifiedNameOfCurrentFunc, templateId, keyCids)
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg, argCids)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value[Value.ContractId],
      choiceId: Ref.ChoiceName,
      choiceArgument: Value[Value.ContractId],
  ): speedy.Command.CreateAndExercise = {
    val (createArg, createArgCids) =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), createArgument)
    val choiceArgType = handleLookup(interface.lookupChoice(templateId, choiceId)).argBinder._2
    val (choiceArg, choiceArgCids) =
      valueTranslator.unsafeTranslateValue(choiceArgType, choiceArgument)
    speedy.Command
      .CreateAndExercise(
        templateId,
        createArg,
        choiceId,
        choiceArg,
        createArgCids | choiceArgCids,
      )
  }

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessLookupByKey(
      templateId: Ref.ValueRef,
      contractKey: Value[Nothing],
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
    val (key, keyCids) = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    assertEmpty(NameOf.qualifiedNameOfCurrentFunc, templateId, keyCids)
    speedy.Command.LookupByKey(templateId, key)
  }

  // returns the speedy translation of an LF command together with all the contract IDs contains inside.
  private[lf] def unsafePreprocessCommand(
      cmd: command.Command
  ): speedy.Command = {
    cmd match {
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
            choiceArgument,
          ) =>
        unsafePreprocessCreateAndExercise(
          templateId,
          createArgument,
          choiceId,
          choiceArgument,
        )
      case command.FetchCommand(templateId, coid) =>
        speedy.Command.Fetch(templateId, SValue.SContractId(coid))
      case command.FetchByKeyCommand(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val (sKey, cids) = valueTranslator.unsafeTranslateValue(ckTtype, key)
        assertEmpty(NameOf.qualifiedNameOfCurrentFunc, templateId, cids)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.LookupByKeyCommand(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val (sKey, cids) = valueTranslator.unsafeTranslateValue(ckTtype, key)
        assertEmpty(NameOf.qualifiedNameOfCurrentFunc, templateId, cids)
        speedy.Command.LookupByKey(templateId, sKey)
    }
  }

}
