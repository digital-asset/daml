// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.data.Ref.PackageRef
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.value.Value
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.command.ApiContractKey
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey}

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

  def unsafeTranslateValue(
      ty: Ast.Type,
      value: Value,
  ): SValue = valueTranslator.unsafeTranslateValue(ty, value)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContract(
      disc: FatContractInstance
  ): speedy.DisclosedContract = {
    // TODO: https://github.com/digital-asset/daml/issues/17082
    //  for now we need the package of the disclosed contract
    val arg = unsafeTranslateValue(Ast.TTyCon(disc.templateId), disc.createArg)
    validateCid(disc.contractId)
    speedy.DisclosedContract(
      disc,
      argument = arg,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value,
  ): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg = unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg)
  }

  def unsafePreprocessExercise(
      typeId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.ApiCommand =
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
  ): speedy.Command.ExerciseTemplate = {
    val choice = handleLookup(pkgInterface.lookupTemplateChoice(templateId, choiceId))
    speedy.Command.ExerciseTemplate(
      templateId = templateId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = unsafeTranslateValue(choice.argBinder._2, argument),
    )
  }

  def unsafePreprocessExerciseInterface(
      ifaceId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
  ): speedy.Command.ExerciseInterface = {
    val choice = handleLookup(pkgInterface.lookupInterfaceChoice(ifaceId, choiceId))
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId = valueTranslator.unsafeTranslateCid(contractId),
      choiceId = choiceId,
      argument = unsafeTranslateValue(choice.argBinder._2, argument),
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
    val arg = unsafeTranslateValue(choiceArgType, argument)
    val key = unsafeTranslateValue(ckTtype, contractKey)
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value,
      choiceId: Ref.ChoiceName,
      choiceArgument: Value,
  ): speedy.Command.CreateAndExercise = {
    val createArg = unsafeTranslateValue(Ast.TTyCon(templateId), createArgument)
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg = unsafeTranslateValue(choiceArgType, choiceArgument)
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
    val key = unsafeTranslateValue(ckTtype, contractKey)
    speedy.Command.LookupByKey(templateId, key)
  }

  private[preprocessing] def unsafeResolveTyConName(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyConRef: Ref.TypeConRef,
      context: => language.Reference,
  ): Ref.TypeConName = {
    val pkgId = tyConRef.pkg match {
      case PackageRef.Id(id) => id
      case PackageRef.Name(name) =>
        pkgResolution.getOrElse(
          name,
          throw Error.Preprocessing.UnresolvedPackageName(name, context),
        )
    }
    Ref.TypeConName(pkgId, tyConRef.qualifiedName)
  }

  // returns the speedy translation of an API command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessApiCommand(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmd: command.ApiCommand,
  ): speedy.ApiCommand =
    cmd match {
      case command.ApiCommand.Create(templateRef, argument) =>
        val templateId =
          unsafeResolveTyConName(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessCreate(templateId, argument)
      case command.ApiCommand.Exercise(typeRef, contractId, choiceId, argument) =>
        val typeId = unsafeResolveTyConName(
          pkgResolution,
          typeRef,
          language.Reference.TemplateOrInterface(typeRef),
        )
        unsafePreprocessExercise(typeId, contractId, choiceId, argument)
      case command.ApiCommand.ExerciseByKey(templateRef, contractKey, choiceId, argument) =>
        val templateId =
          unsafeResolveTyConName(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
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
      case command.ReplayCommand.Fetch(tmplId, ifaceIdOpt, coid) =>
        val cid = valueTranslator.unsafeTranslateCid(coid)
        ifaceIdOpt match {
          case Some(ifaceId) =>
            discard(handleLookup(pkgInterface.lookupInterfaceInstance(ifaceId, tmplId)))
            speedy.Command.FetchInterface(ifaceId, cid)
          case None =>
            discard(handleLookup(pkgInterface.lookupTemplate(tmplId)))
            speedy.Command.FetchTemplate(tmplId, cid)
        }
      case command.ReplayCommand.FetchByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = unsafeTranslateValue(ckTtype, key)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = unsafeTranslateValue(ckTtype, key)
        speedy.Command.LookupByKey(templateId, sKey)
    }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmds: ImmArray[command.ApiCommand],
  ): ImmArray[speedy.ApiCommand] =
    cmds.map(unsafePreprocessApiCommand(pkgResolution, _))

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContracts(
      discs: ImmArray[FatContractInstance]
  ): (ImmArray[speedy.DisclosedContract], Set[Value.ContractId], Set[Hash]) = {
    var contractIds: Set[Value.ContractId] = Set.empty
    val contractKeyHashes = Set.newBuilder[Hash]

    val preprocessedDiscs = discs.map { disclosedContract =>
      if (contractIds.contains(disclosedContract.contractId))
        throw Error.Preprocessing.DuplicateDisclosedContractId(disclosedContract.contractId)
      contractIds += disclosedContract.contractId
      disclosedContract.contractKeyWithMaintainers.foreach { keyWithMaintainers =>
        contractKeyHashes += keyWithMaintainers.globalKey.hash
      }
      unsafePreprocessDisclosedContract(disclosedContract)
    }
    (preprocessedDiscs, contractIds, contractKeyHashes.result())
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

    val arg = unsafeTranslateValue(Ast.TTyCon(templateId), argument)

    speedy.InterfaceView(templateId, arg, interfaceId)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiContractKeys(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      keys: Seq[ApiContractKey],
  ): Seq[GlobalKey] =
    keys.map(unsafePreprocessApiContractKey(pkgResolution, _))

  @throws[Error.Preprocessing.Error]
  private def unsafePreprocessApiContractKey(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      key: ApiContractKey,
  ): GlobalKey = {
    val templateRef = key.templateRef
    val templateId = unsafeResolveTyConName(
      pkgResolution,
      templateRef,
      language.Reference.Template(templateRef),
    )
    val tmpl = handleLookup(pkgInterface.lookupTemplate(templateId))
    if (tmpl.key.isEmpty)
      throw Error.Preprocessing.UnexpectedContractKeyPrefetch(
        templateRef,
        key.contractKey,
      )
    unsafePreprocessContractKey(key.contractKey, templateId)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessContractKey(contractKey: Value, templateId: Ref.TypeConName): GlobalKey = {
    val ckTtype: Ast.Type = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val preprocessedKey = unsafeTranslateValue(ckTtype, contractKey)
    speedy.Speedy.Machine
      .globalKey(pkgInterface, templateId, preprocessedKey)
      .getOrElse(
        throw Error.Preprocessing.ContractIdInContractKey(contractKey)
      )
  }
}
