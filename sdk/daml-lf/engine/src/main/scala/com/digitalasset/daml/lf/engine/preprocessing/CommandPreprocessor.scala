// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.data.Ref.PackageRef
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

private[lf] final class CommandPreprocessor(
    pkgInterface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
) {

  import Preprocessor._

  private val valueTranslator =
    new ValueTranslator(
      pkgInterface = pkgInterface,
      requireV1ContractIdSuffix = requireV1ContractIdSuffix,
    )

  import valueTranslator.validateCid

  private def valueTranslatorConfig(strict: Boolean) =
    if (strict) ValueTranslator.Config.Strict else ValueTranslator.Config.Upgradeable

  private def translateUpgradableArg(typ: Ast.Type, value: Value, strict: Boolean) =
    valueTranslator.unsafeTranslateValue(
      ty = typ,
      value = value,
      config = valueTranslatorConfig(strict),
    )

  private def translateNonUpgradableArg(typ: Ast.Type, value: Value, strict: Boolean) =
    valueTranslator.unsafeTranslateValue(
      ty = typ,
      value = value,
      config = valueTranslatorConfig(strict),
    )

  // This is used by value enricher,
  // TODO: https://github.com/digital-asset/daml/issues/17082
  //  This should problaby use ValueTranslator.Config.Strict
  def unsafeStrictTranslateValue(typ: Ast.Type, value: Value) =
    valueTranslator.unsafeTranslateValue(typ, value, ValueTranslator.Config.Strict)

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
    val arg = translateUpgradableArg(Ast.TTyCon(disc.templateId), disc.argument, strict = true)
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
    val arg = translateUpgradableArg(Ast.TTyCon(templateId), argument, strict = strict)
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
    speedy.Command.ExerciseTemplate(
      templateId = templateId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = translateUpgradableArg(choice.argBinder._2, argument, strict = strict),
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
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = translateNonUpgradableArg(choice.argBinder._2, argument, strict = strict),
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
    val arg = translateUpgradableArg(choiceArgType, argument, strict = strict)
    val key = translateNonUpgradableArg(ckTtype, contractKey, strict = strict)
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
    val createArg = translateUpgradableArg(Ast.TTyCon(templateId), createArgument, strict = strict)
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg = translateUpgradableArg(choiceArgType, choiceArgument, strict = strict)
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
    val key = translateNonUpgradableArg(ckTtype, contractKey, strict = strict)
    speedy.Command.LookupByKey(templateId, key)
  }

  private[preprocessing] def unsafeResolveTyConName(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyConRef: Ref.TypeConRef,
      context: => language.Reference,
  ): Ref.TypeConName = {
    val pkgId = tyConRef.pkgRef match {
      case PackageRef.Id(id) => id
      case PackageRef.Name(name) =>
        pkgResolution.getOrElse(
          name,
          throw Error.Preprocessing.UnresolvedPackageName(name, context),
        )
    }
    Ref.TypeConName(pkgId, tyConRef.qName)
  }

  // returns the speedy translation of an API command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessApiCommand(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmd: command.ApiCommand,
  ): speedy.Command =
    cmd match {
      case command.ApiCommand.Create(templateRef, argument) =>
        val templateId =
          unsafeResolveTyConName(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessCreate(templateId, argument, strict = false)
      case command.ApiCommand.Exercise(typeRef, contractId, choiceId, argument) =>
        val typeId = unsafeResolveTyConName(
          pkgResolution,
          typeRef,
          language.Reference.TemplateOrInterface(typeRef),
        )
        unsafePreprocessExercise(typeId, contractId, choiceId, argument, strict = false)
      case command.ApiCommand.ExerciseByKey(templateRef, contractKey, choiceId, argument) =>
        val templateId =
          unsafeResolveTyConName(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument, strict = false)
      case command.ApiCommand.CreateAndExercise(
            templateRef,
            createArgument,
            choiceId,
            choiceArgument,
          ) =>
        val templateId =
          unsafeResolveTyConName(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
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
        val cid = valueTranslator.unsafeTranslateCid(coid)
        handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
          case TemplateOrInterface.Template(_) =>
            speedy.Command.FetchTemplate(typeId, cid)
          case TemplateOrInterface.Interface(_) =>
            speedy.Command.FetchInterface(typeId, cid)
        }
      case command.ReplayCommand.FetchByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateNonUpgradableArg(ckTtype, key, strict = true)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateNonUpgradableArg(ckTtype, key, strict = true)
        speedy.Command.LookupByKey(templateId, sKey)
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmds: ImmArray[command.ApiCommand],
  ): ImmArray[speedy.Command] =
    cmds.map(unsafePreprocessApiCommand(pkgResolution, _))

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

    val arg = translateNonUpgradableArg(Ast.TTyCon(templateId), argument, strict = true)

    speedy.InterfaceView(templateId, arg, interfaceId)
  }

}
