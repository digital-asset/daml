// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command.ContractMetadata
import com.daml.lf.data._
import com.daml.lf.engine.Error.Preprocessing.DuplicateDisclosedContractId
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

import scala.collection.mutable

private[lf] final class CommandPreprocessor(
    pkgInterface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
) {

  val valueTranslator =
    new ValueTranslator(
      pkgInterface = pkgInterface,
      requireV1ContractIdSuffix = requireV1ContractIdSuffix,
    )

  import Preprocessor._

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContract[Metadata <: ContractMetadata](
      disc: command.DisclosedContract[Metadata]
  ): speedy.DisclosedContract[Metadata] = {
    discard(handleLookup(pkgInterface.lookupTemplate(disc.templateId)))
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(disc.templateId), disc.argument)
    val coid = valueTranslator.unsafeTranslateCid(disc.contractId)
    speedy.DisclosedContract(
      templateId = disc.templateId,
      contractId = coid,
      argument = arg,
      metadata = disc.metadata,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value,
  ): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg)
  }

  def unsafePreprocessExercise(
      typeId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command =
    handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
      case TemplateOrInterface.Template(_) =>
        unsafePreprocessExerciseTemplate(typeId, contractId, choiceId, argument)
      case TemplateOrInterface.Interface(_) =>
        unsafePreprocessExerciseInterface(typeId, contractId, choiceId, argument)
    }

  def unsafePreprocessExerciseTemplate(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command = {
    val choice = handleLookup(pkgInterface.lookupTemplateChoice(templateId, choiceId))
    speedy.Command.ExerciseTemplate(
      templateId = templateId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument),
    )
  }

  def unsafePreprocessExerciseInterface(
      ifaceId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command = {
    val choice = handleLookup(pkgInterface.lookupInterfaceChoice(ifaceId, choiceId))
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument),
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
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
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
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
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
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
      case command.ApiCommand.Exercise(typeId, contractId, choiceId, argument) =>
        unsafePreprocessExercise(typeId, contractId, choiceId, argument)
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
      case command.ReplayCommand.Create(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument)
      case command.ReplayCommand.Exercise(templateId, mbIfaceId, coid, choiceId, argument) =>
        mbIfaceId match {
          case Some(ifaceId) =>
            unsafePreprocessExerciseInterface(ifaceId, coid, choiceId, argument)
          case None =>
            unsafePreprocessExerciseTemplate(templateId, coid, choiceId, argument)
        }
      case command.ReplayCommand.ExerciseByKey(
            templateId,
            contractKey,
            choiceId,
            argument,
          ) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
      case command.ReplayCommand.Fetch(typeId, coid) =>
        val cid = valueTranslator.unsafeTranslateCid(coid)
        handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
          case TemplateOrInterface.Template(_) =>
            speedy.Command.FetchTemplate(typeId, cid)
          case TemplateOrInterface.Interface(_) =>
            speedy.Command.FetchInterface(typeId, cid)
        }
      case command.ReplayCommand.FetchByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.LookupByKey(templateId, sKey)
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(cmds: ImmArray[command.ApiCommand]): ImmArray[speedy.Command] =
    cmds.map(unsafePreprocessApiCommand)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContracts[Metadata <: ContractMetadata](
      discs: ImmArray[command.DisclosedContract[Metadata]]
  ): ImmArray[speedy.DisclosedContract[Metadata]] = {
    val contractIds: mutable.Set[Value.ContractId] = mutable.Set.empty

    discs.map { disclosedContract =>
      if (contractIds.contains(disclosedContract.contractId)) {
        throw DuplicateDisclosedContractId(
          disclosedContract.contractId,
          disclosedContract.templateId,
        )
      } else {
        discard(contractIds += disclosedContract.contractId)
        unsafePreprocessDisclosedContract(disclosedContract)
      }
    }
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessInterfaceView(
      templateId: Ref.Identifier,
      argument: Value,
      interfaceId: Ref.Identifier,
  ): speedy.InterfaceView = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    discard(handleLookup(pkgInterface.lookupInterface(interfaceId)))
    discard(handleLookup(pkgInterface.lookupInterfaceInstance(interfaceId, templateId)))

    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)

    speedy.InterfaceView(templateId, arg, interfaceId)
  }
}
