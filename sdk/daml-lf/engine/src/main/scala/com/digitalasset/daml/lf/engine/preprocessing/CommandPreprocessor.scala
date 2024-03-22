// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.{Ast, PackageInterface}
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

import scala.annotation.nowarn

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
  def unsafePreprocessDisclosedContract(
      disc: command.DisclosedContract
  ): speedy.DisclosedContract = {
    discard(handleLookup(interface.lookupTemplate(disc.templateId)))
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
    discard(handleLookup(interface.lookupTemplate(templateId)))
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg)
  }

  // TODO: https://github.com/digital-asset/daml/issues/12051
  //  Drop this once Canton support ambiguous choices properly
  @throws[Error.Preprocessing.Error]
  @deprecated
  private def unsafePreprocessLenientExercise(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command =
    handleLookup(interface.lookupLenientChoice(templateId, choiceId)) match {
      case PackageInterface.ChoiceInfo.Template(choice) =>
        speedy.Command.ExerciseTemplate(
          templateId = templateId,
          contractId = valueTranslator.unsafeTranslateCid(contractId),
          choiceId = choiceId,
          argument = valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument),
        )
      case PackageInterface.ChoiceInfo.Inherited(ifaceId, choice) =>
        speedy.Command.ExerciseInterface(
          interfaceId = ifaceId,
          contractId = valueTranslator.unsafeTranslateCid(contractId),
          choiceId = choiceId,
          argument = valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument),
        )
    }

  def unsafePreprocessExercise(
      typeId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command =
    handleLookup(interface.lookupTemplateOrInterface(typeId)) match {
      case Left(_) =>
        unsafePreprocessExerciseTemplate(typeId, contractId, choiceId, argument)
      case Right(_) =>
        unsafePreprocessExerciseInterface(typeId, contractId, choiceId, argument)
    }

  def unsafePreprocessExerciseTemplate(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command = {
    val choice = handleLookup(interface.lookupTemplateChoice(templateId, choiceId))
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
    val choice = handleLookup(interface.lookupInterfaceChoice(ifaceId, choiceId))
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
  @nowarn("msg=deprecated")
  private[preprocessing] def unsafePreprocessReplayCommand(
      cmd: command.ReplayCommand
  ): speedy.Command =
    cmd match {
      case command.ReplayCommand.Create(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument)
      case command.ReplayCommand.LenientExercise(typeId, coid, choiceId, argument) =>
        unsafePreprocessLenientExercise(typeId, coid, choiceId, argument)
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
        handleLookup(interface.lookupTemplateOrInterface(typeId)) match {
          case Left(_) =>
            speedy.Command.FetchTemplate(typeId, cid)
          case Right(_) =>
            speedy.Command.FetchInterface(typeId, cid)
        }
      case command.ReplayCommand.FetchByKey(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key)
        speedy.Command.LookupByKey(templateId, sKey)
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(cmds: ImmArray[command.ApiCommand]): ImmArray[speedy.Command] =
    cmds.map(unsafePreprocessApiCommand)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContracts(
      discs: ImmArray[command.DisclosedContract]
  ): ImmArray[speedy.DisclosedContract] =
    discs.map(unsafePreprocessDisclosedContract)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessInterfaceView(
      templateId: Ref.Identifier,
      argument: Value,
      interfaceId: Ref.Identifier,
  ): speedy.InterfaceView = {
    discard(handleLookup(interface.lookupTemplate(templateId)))
    discard(handleLookup(interface.lookupInterface(interfaceId)))
    val version =
      TransactionVersion.assignNodeVersion(interface.packageLanguageVersion(interfaceId.packageId))
    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    // TODO https://github.com/digital-asset/daml/issues/14112
    // Add check if interface does not have magic view method.
    speedy.InterfaceView(templateId, arg, interfaceId, version)
  }
}
