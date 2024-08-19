// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.Ref.PackageRef
import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

import scala.math.Ordered.orderingToOrdered

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

  private[this] def valueTranslatorConfig(
      strict: Boolean,
      enableUpgrading: Boolean,
  ): ValueTranslator.Config =
    if (strict || !enableUpgrading) ValueTranslator.Config.Strict
    else ValueTranslator.Config.Upgradeable

  private[this] def translateArg(
      typ: Ast.Type,
      value: Value,
      strict: Boolean,
      enableUpgrading: Boolean,
  ) =
    valueTranslator.unsafeTranslateValue(
      ty = typ,
      value = value,
      config = valueTranslatorConfig(strict, enableUpgrading),
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
    val arg = translateArg(
      Ast.TTyCon(disc.templateId),
      disc.argument,
      strict = true,
      isUpgradable(disc.templateId.packageId),
    )
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
    val arg = translateArg(
      Ast.TTyCon(templateId),
      argument,
      strict = strict,
      isUpgradable(templateId.packageId),
    )
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
      argument = translateArg(
        choice.argBinder._2,
        argument,
        strict = strict,
        isUpgradable(templateId.packageId),
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
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = translateArg(
        choice.argBinder._2,
        argument,
        strict = strict,
        isUpgradable(ifaceId.packageId),
      ),
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value,
      choiceId: Ref.ChoiceName,
      argument: Value,
      strictArgument: Boolean,
      strictKey: Boolean,
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val arg = translateArg(
      choiceArgType,
      argument,
      strict = strictArgument,
      isUpgradable(templateId.packageId),
    )
    val key = translateArg(
      ckTtype,
      contractKey,
      strict = strictKey,
      isUpgradable(templateId.packageId),
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
    val createArg = translateArg(
      Ast.TTyCon(templateId),
      createArgument,
      strict = strict,
      isUpgradable(templateId.packageId),
    )
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg = translateArg(
      choiceArgType,
      choiceArgument,
      strict = strict,
      isUpgradable(templateId.packageId),
    )
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
    val key = translateArg(
      ckTtype,
      contractKey,
      strict = strict,
      enableUpgrading = isUpgradable(templateId.packageId),
    )
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
        unsafePreprocessExerciseByKey(
          templateId,
          contractKey,
          choiceId,
          argument,
          strictArgument = false,
          strictKey = false,
        )
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
        unsafePreprocessExerciseByKey(
          templateId,
          contractKey,
          choiceId,
          argument,
          strictArgument = true,
          // When reconstructing an exercise-by-key node from a view, we pull the key from the corresponding input
          // contract. That input contract may use a different package version for the type of its input key than
          // the version that was used for constructing the node in the submitting participant, which needs to be the
          // same when re-interpreting the node. Therefore, we need to allow upgrading keys when re-interpreting
          // exercise-by-key nodes.
          strictKey = false,
        )
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
        val sKey = translateArg(
          ckTtype,
          key,
          strict = true,
          enableUpgrading = isUpgradable(templateId.packageId),
        )
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateArg(
          ckTtype,
          key,
          strict = true,
          enableUpgrading = isUpgradable(templateId.packageId),
        )
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

    val arg = translateArg(
      Ast.TTyCon(templateId),
      argument,
      strict = true,
      isUpgradable(templateId.packageId),
    )

    speedy.InterfaceView(templateId, arg, interfaceId)
  }

  private def isUpgradable(packageId: Ref.PackageId): Boolean = {
    discard(handleLookup(pkgInterface.lookupPackage(packageId)))
    pkgInterface.packageLanguageVersion(packageId) >= LanguageVersion.Features.packageUpgrades
  }
}
