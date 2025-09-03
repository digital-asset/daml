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
    requireContractIdSuffix: Boolean,
) {

  import Preprocessor._

  private val valueTranslator =
    new ValueTranslator(
      pkgInterface = pkgInterface,
      requireContractIdSuffix = requireContractIdSuffix,
    )

  def unsafeTranslateValue(
      ty: Ast.Type,
      value: Value,
      allowRelativeContractIds: Boolean,
  ): SValue = valueTranslator.unsafeTranslateValue(ty, value, allowRelativeContractIds)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContract(
      disc: FatContractInstance
  ): speedy.DisclosedContract = {
    // TODO: https://github.com/digital-asset/daml/issues/17082
    //  for now we need the package of the disclosed contract
    val arg = unsafeTranslateValue(
      Ast.TTyCon(disc.templateId),
      disc.createArg,
      allowRelativeContractIds = false,
    )
    discard(valueTranslator.unsafeTranslateCid(disc.contractId, allowRelative = false))
    speedy.DisclosedContract(
      disc,
      argument = arg,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value,
      allowRelativeContractIds: Boolean,
  ): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg = unsafeTranslateValue(Ast.TTyCon(templateId), argument, allowRelativeContractIds)
    speedy.Command.Create(templateId, arg)
  }

  def unsafePreprocessExercise(
      typeId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      allowRelativeContractIds: Boolean,
  ): speedy.ApiCommand =
    handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
      case TemplateOrInterface.Template(_) =>
        unsafePreprocessExerciseTemplate(
          typeId,
          contractId,
          choiceId,
          argument,
          allowRelativeContractIds,
        )
      case TemplateOrInterface.Interface(_) =>
        unsafePreprocessExerciseInterface(
          typeId,
          contractId,
          choiceId,
          argument,
          allowRelativeContractIds,
        )
    }

  def unsafePreprocessExerciseTemplate(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      allowRelativeContractIds: Boolean,
  ): speedy.Command.ExerciseTemplate = {
    val choice = handleLookup(pkgInterface.lookupTemplateChoice(templateId, choiceId))
    speedy.Command.ExerciseTemplate(
      templateId = templateId,
      contractId = valueTranslator.unsafeTranslateCid(contractId, allowRelativeContractIds),
      choiceId = choiceId,
      argument = unsafeTranslateValue(choice.argBinder._2, argument, allowRelativeContractIds),
    )
  }

  def unsafePreprocessExerciseInterface(
      ifaceId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      allowRelativeContractIds: Boolean,
  ): speedy.Command.ExerciseInterface = {
    val choice = handleLookup(pkgInterface.lookupInterfaceChoice(ifaceId, choiceId))
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId = valueTranslator.unsafeTranslateCid(contractId, allowRelativeContractIds),
      choiceId = choiceId,
      argument = unsafeTranslateValue(choice.argBinder._2, argument, allowRelativeContractIds),
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value,
      choiceId: Ref.ChoiceName,
      argument: Value,
      allowRelativeContractIds: Boolean,
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val arg = unsafeTranslateValue(choiceArgType, argument, allowRelativeContractIds)
    val key = unsafeTranslateValue(ckTtype, contractKey, allowRelativeContractIds)
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
      unsafeTranslateValue(Ast.TTyCon(templateId), createArgument, allowRelativeContractIds = false)
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg =
      unsafeTranslateValue(choiceArgType, choiceArgument, allowRelativeContractIds = false)
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
      allowRelativeContractIds: Boolean,
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val key = unsafeTranslateValue(ckTtype, contractKey, allowRelativeContractIds)
    speedy.Command.LookupByKey(templateId, key)
  }

  private[preprocessing] def unsafeResolveTyConId(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyConRef: Ref.TypeConRef,
      context: => language.Reference,
  ): Ref.TypeConId = {
    val pkgId = tyConRef.pkg match {
      case PackageRef.Id(id) => id
      case PackageRef.Name(name) =>
        pkgResolution.getOrElse(
          name,
          throw Error.Preprocessing.UnresolvedPackageName(name, context),
        )
    }
    Ref.TypeConId(pkgId, tyConRef.qualifiedName)
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
          unsafeResolveTyConId(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessCreate(templateId, argument, allowRelativeContractIds = false)
      case command.ApiCommand.Exercise(typeRef, contractId, choiceId, argument) =>
        val typeId = unsafeResolveTyConId(
          pkgResolution,
          typeRef,
          language.Reference.TemplateOrInterface(typeRef),
        )
        unsafePreprocessExercise(
          typeId,
          contractId,
          choiceId,
          argument,
          allowRelativeContractIds = false,
        )
      case command.ApiCommand.ExerciseByKey(templateRef, contractKey, choiceId, argument) =>
        val templateId =
          unsafeResolveTyConId(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessExerciseByKey(
          templateId,
          contractKey,
          choiceId,
          argument,
          allowRelativeContractIds = false,
        )
      case command.ApiCommand.CreateAndExercise(
            templateRef,
            createArgument,
            choiceId,
            choiceArgument,
          ) =>
        val templateId =
          unsafeResolveTyConId(
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
        unsafePreprocessCreate(templateId, argument, allowRelativeContractIds = true)
      case command.ReplayCommand.Exercise(templateId, mbIfaceId, coid, choiceId, argument) =>
        mbIfaceId match {
          case Some(ifaceId) =>
            unsafePreprocessExerciseInterface(
              ifaceId,
              coid,
              choiceId,
              argument,
              allowRelativeContractIds = true,
            )
          case None =>
            unsafePreprocessExerciseTemplate(
              templateId,
              coid,
              choiceId,
              argument,
              allowRelativeContractIds = true,
            )
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
          allowRelativeContractIds = true,
        )
      case command.ReplayCommand.Fetch(tmplId, ifaceIdOpt, coid) =>
        val cid = valueTranslator.unsafeTranslateCid(coid, allowRelative = true)
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
        val sKey = unsafeTranslateValue(ckTtype, key, allowRelativeContractIds = true)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = unsafeTranslateValue(ckTtype, key, allowRelativeContractIds = true)
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

    val arg =
      unsafeTranslateValue(Ast.TTyCon(templateId), argument, allowRelativeContractIds = true)

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
    val templateId = unsafeResolveTyConId(
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
    unsafePreprocessContractKey(key.contractKey, templateId, allowRelativeContractIds = false)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessContractKey(
      contractKey: Value,
      templateId: Ref.TypeConId,
      allowRelativeContractIds: Boolean,
  ): GlobalKey = {
    val ckTtype: Ast.Type = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val preprocessedKey = unsafeTranslateValue(ckTtype, contractKey, allowRelativeContractIds)
    speedy.Speedy.Machine
      .globalKey(pkgInterface, templateId, preprocessedKey)
      .getOrElse(
        throw Error.Preprocessing.ContractIdInContractKey(contractKey)
      )
  }
}
