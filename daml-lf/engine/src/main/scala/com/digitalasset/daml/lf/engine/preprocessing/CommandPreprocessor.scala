// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

private[lf] final class CommandPreprocessor(
    pkgInterface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
    enableContractUpgrading: Boolean,
) {

  import Preprocessor._

  // config for creteArgument in ApiCommand
  private[this] val LenientCreateArgTranslateConfig =
    if (enableContractUpgrading)
      ValueTranslator.Config(
        allowFieldReordering = false,
        ignorePackageId = true,
        enableUpgrade = true,
      )
    else
      ValueTranslator.Config(
        allowFieldReordering = true,
        ignorePackageId = false,
        enableUpgrade = false,
      )

  // config for creteArgument in ApiCommand
  private[this] val LenientNonCreateArgTranslateConfig =
    if (enableContractUpgrading)
      // in case of upgrade
      // - we need to ignore package id
      // - for the sake of consistency we forbid field reordering
      ValueTranslator.Config(
        allowFieldReordering = false,
        ignorePackageId = true,
        enableUpgrade = false,
      )
    else
      ValueTranslator.Config(
        allowFieldReordering = true,
        ignorePackageId = false,
        enableUpgrade = false,
      )

  // Config for all values translation in ReplayCommand and explicit disclosure
  // is it strict in the sens, upgrade is completely disable.
  // TODO: https://github.com/digital-asset/daml/issues/17082
  //  We allow field reordering for backward compatibility reason
  //  However we probably could and should prevent it
  private[this] val StrictTranslateConfig =
    ValueTranslator.Config(
      allowFieldReordering = true,
      ignorePackageId = false,
      enableUpgrade = false,
    )

  private val valueTranslator =
    new ValueTranslator(
      pkgInterface = pkgInterface,
      requireV1ContractIdSuffix = requireV1ContractIdSuffix,
    )

  import valueTranslator.validateCid

  private[this] def translateCreateArg(tyCon: Ref.TypeConName, value: Value, strict: Boolean) =
    valueTranslator.unsafeTranslateValue(
      ty = Ast.TTyCon(tyCon),
      value = value,
      config = if (strict) StrictTranslateConfig else LenientCreateArgTranslateConfig,
    )

  private[this] def translateNonCreateArg(typ: Ast.Type, value: Value, strict: Boolean) =
    valueTranslator.unsafeTranslateValue(
      ty = typ,
      value = value,
      config = if (strict) StrictTranslateConfig else LenientNonCreateArgTranslateConfig,
    )

  // This is used by value enricher,
  // TODO: https://github.com/digital-asset/daml/issues/17082
  //  This should problaby use ValueTranslator.Config.Legacy
  def unsafeStrictTranslateValue(typ: Ast.Type, value: Value) =
    valueTranslator.unsafeTranslateValue(typ, value, ValueTranslator.Config.Legacy)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContract(
      disc: command.DisclosedContract
  ): speedy.DisclosedContract = {
    val tmpl = handleLookup(pkgInterface.lookupTemplate(disc.templateId))
    (tmpl.key, disc.keyHash) match {
      case (Some(_), None) =>
        throw Error.Preprocessing.MissingDisclosedContractKeyHash(
          disc.contractId,
          disc.templateId,
        )
      case (None, Some(hash)) =>
        throw Error.Preprocessing.UnexpectedDisclosedContractKeyHash(
          disc.contractId,
          disc.templateId,
          hash,
        )
      case _ =>
    }
    // TODO: https://github.com/digital-asset/daml/issues/17082
    //  for now we need the package of the disclosed contract
    val arg = translateCreateArg(disc.templateId, disc.argument, strict = true)
    validateCid(disc.contractId)
    speedy.DisclosedContract(
      templateId = disc.templateId,
      contractId = disc.contractId,
      argument = arg,
      keyHash = disc.keyHash,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value,
      strict: Boolean,
  ): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg = translateCreateArg(templateId, argument, strict = strict)
    speedy.Command.Create(templateId, arg)
  }

  def unsafePreprocessExercise(
      typeId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      strict: Boolean,
  ): speedy.Command =
    handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
      case TemplateOrInterface.Template(_) =>
        unsafePreprocessExerciseTemplate(typeId, contractId, choiceId, argument, strict)
      case TemplateOrInterface.Interface(_) =>
        unsafePreprocessExerciseInterface(typeId, contractId, choiceId, argument, strict)
    }

  def unsafePreprocessExerciseTemplate(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      strict: Boolean,
  ): speedy.Command = {
    val choice = handleLookup(pkgInterface.lookupTemplateChoice(templateId, choiceId))
    validateCid(contractId)
    speedy.Command.ExerciseTemplate(
      templateId = templateId,
      contractId = contractId,
      choiceId = choiceId,
      argument = translateNonCreateArg(
        choice.argBinder._2,
        argument,
        strict = strict,
      ),
    )
  }

  def unsafePreprocessExerciseInterface(
      ifaceId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      strict: Boolean,
  ): speedy.Command = {
    val choice = handleLookup(pkgInterface.lookupInterfaceChoice(ifaceId, choiceId))
    validateCid(contractId)
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId = contractId,
      choiceId = choiceId,
      argument = translateNonCreateArg(
        choice.argBinder._2,
        argument,
        strict = strict,
      ),
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value,
      choiceId: Ref.ChoiceName,
      argument: Value,
      strict: Boolean,
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val arg = translateNonCreateArg(
      choiceArgType,
      argument,
      strict = strict,
    )
    val key = translateNonCreateArg(
      ckTtype,
      contractKey,
      strict = strict,
    )
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value,
      choiceId: Ref.ChoiceName,
      choiceArgument: Value,
      strict: Boolean,
  ): speedy.Command.CreateAndExercise = {
    val createArg = translateCreateArg(templateId, createArgument, strict = strict)
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg = translateNonCreateArg(choiceArgType, choiceArgument, strict = strict)
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
      strict: Boolean,
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val key = translateNonCreateArg(
      ckTtype,
      contractKey,
      strict = strict,
    )
    speedy.Command.LookupByKey(templateId, key)
  }

  // returns the speedy translation of an API command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessApiCommand(
      cmd: command.ApiCommand
  ): speedy.Command =
    cmd match {
      case command.ApiCommand.Create(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument, strict = false)
      case command.ApiCommand.Exercise(typeId, contractId, choiceId, argument) =>
        unsafePreprocessExercise(typeId, contractId, choiceId, argument, strict = false)
      case command.ApiCommand.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument, strict = false)
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
          strict = false,
        )
    }

  // returns the speedy translation of an Replay command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessReplayCommand(
      cmd: command.ReplayCommand
  ): speedy.Command =
    cmd match {
      case command.ReplayCommand.Create(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument, strict = true)
      case command.ReplayCommand.Exercise(templateId, mbIfaceId, coid, choiceId, argument) =>
        mbIfaceId match {
          case Some(ifaceId) =>
            unsafePreprocessExerciseInterface(ifaceId, coid, choiceId, argument, strict = true)
          case None =>
            unsafePreprocessExerciseTemplate(templateId, coid, choiceId, argument, strict = true)
        }
      case command.ReplayCommand.ExerciseByKey(
            templateId,
            contractKey,
            choiceId,
            argument,
          ) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument, strict = true)
      case command.ReplayCommand.Fetch(typeId, coid) =>
        validateCid(coid)
        handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
          case TemplateOrInterface.Template(_) =>
            speedy.Command.FetchTemplate(typeId, coid)
          case TemplateOrInterface.Interface(_) =>
            speedy.Command.FetchInterface(typeId, coid)
        }
      case command.ReplayCommand.FetchByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateNonCreateArg(
          ckTtype,
          key,
          strict = true,
        )
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateNonCreateArg(
          ckTtype,
          key,
          strict = true,
        )
        speedy.Command.LookupByKey(templateId, sKey)
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(cmds: ImmArray[command.ApiCommand]): ImmArray[speedy.Command] =
    cmds.map(unsafePreprocessApiCommand)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContracts(
      discs: ImmArray[command.DisclosedContract]
  ): ImmArray[speedy.DisclosedContract] = {
    var contractIds: Set[Value.ContractId] = Set.empty
    var contractKeys: Set[crypto.Hash] = Set.empty

    discs.map { disclosedContract =>
      if (contractIds.contains(disclosedContract.contractId))
        throw Error.Preprocessing.DuplicateDisclosedContractId(disclosedContract.contractId)
      contractIds += disclosedContract.contractId
      disclosedContract.keyHash.foreach { hash =>
        if (contractKeys.contains(hash))
          throw Error.Preprocessing.DuplicateDisclosedContractKey(hash)
        contractKeys += hash
      }
      unsafePreprocessDisclosedContract(disclosedContract)
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

    val arg = translateNonCreateArg(
      Ast.TTyCon(templateId),
      argument,
      strict = true,
    )

    speedy.InterfaceView(templateId, arg, interfaceId)
  }
}
