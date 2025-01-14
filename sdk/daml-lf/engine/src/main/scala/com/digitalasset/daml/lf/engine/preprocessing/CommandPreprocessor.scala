// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command.ApiContractKey
import com.daml.lf.data.Ref.{PackageRef, TypeConName}
import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.scalautil.Statement.discard

private[lf] final class CommandPreprocessor(
    pkgInterface: language.PackageInterface,
    checkV1ContractIdSuffix: Boolean,
) {

  import Preprocessor._

  private val valueTranslator =
    new ValueTranslator(
      pkgInterface = pkgInterface,
      checkV1ContractIdSuffixes = checkV1ContractIdSuffix,
    )

  import valueTranslator.validateCid

  private[this] def translateArg(
      typ: Ast.Type,
      upgradable: Boolean,
      value: Value,
  ) =
    valueTranslator.unsafeTranslateValue(ty = typ, upgradable = upgradable, value = value)

  // This is used by value enricher
  def unsafeTranslateValue(typ: Ast.Type, upgradable: Boolean, value: Value) =
    valueTranslator.unsafeTranslateValue(typ, upgradable = upgradable, value)

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
    val arg = translateArg(Ast.TTyCon(disc.templateId), upgradable(disc.templateId), disc.argument)
    validateCid(disc.contractId)
    speedy.DisclosedContract(
      templateId = disc.templateId,
      contractId = disc.contractId,
      argument = arg,
      keyHash = disc.keyHash,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(templateId: Ref.Identifier, argument: Value): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg = translateArg(Ast.TTyCon(templateId), upgradable(templateId), argument)
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
      argument = translateArg(choice.argBinder._2, upgradable(templateId), argument),
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
      argument = translateArg(choice.argBinder._2, upgradable(ifaceId), argument),
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
    val upgradable = this.upgradable(templateId)
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val arg = translateArg(choiceArgType, upgradable, argument)
    val key = translateArg(ckTtype, upgradable, contractKey)
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value,
      choiceId: Ref.ChoiceName,
      choiceArgument: Value,
  ): speedy.Command.CreateAndExercise = {
    val upgradable = this.upgradable(templateId)
    val createArg = translateArg(Ast.TTyCon(templateId), upgradable, createArgument)
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg = translateArg(choiceArgType, upgradable, choiceArgument)
    speedy.Command.CreateAndExercise(templateId, createArg, choiceId, choiceArg)
  }

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessLookupByKey(
      templateId: Ref.ValueRef,
      contractKey: Value,
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val key = translateArg(ckTtype, upgradable(templateId), contractKey)
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
        unsafePreprocessExerciseByKey(
          templateId,
          contractKey,
          choiceId,
          argument,
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
        unsafePreprocessExerciseByKey(
          templateId,
          contractKey,
          choiceId,
          argument,
        )
      case command.ReplayCommand.Fetch(typeId, ifaceIdOpt, coid) =>
        val cid = valueTranslator.unsafeTranslateCid(coid)
        ifaceIdOpt match {
          case Some(ifaceId) =>
            val _ = handleLookup(pkgInterface.lookupInterfaceInstance(ifaceId, typeId))
            speedy.Command.FetchInterface(ifaceId, cid)
          case None =>
            handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
              case TemplateOrInterface.Template(_) =>
                speedy.Command.FetchTemplate(typeId, cid)
              case TemplateOrInterface.Interface(_) =>
                // We should not accept interface ID, however we keep it
                // - for backward compatibility with PV 5
                // - @Matthias agrees this does not provide to a malicious submitter any advantage
                speedy.Command.FetchInterface(typeId, cid)
            }
        }
      case command.ReplayCommand.FetchByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateArg(ckTtype, upgradable(templateId), key)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = translateArg(ckTtype, upgradable(templateId), key)
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
  ): (ImmArray[speedy.DisclosedContract], Set[crypto.Hash]) = {
    var contractIds: Set[Value.ContractId] = Set.empty
    var contractKeys: Set[crypto.Hash] = Set.empty

    val processedDiscs = discs.map { disclosedContract =>
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
    processedDiscs -> contractKeys
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

    val arg = translateArg(Ast.TTyCon(templateId), upgradable(templateId), argument)

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
  def unsafePreprocessContractKey(contractKey: Value, templateId: TypeConName): GlobalKey = {
    val ckTtype: Ast.Type = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val preprocessedKey = translateArg(ckTtype, upgradable(templateId), contractKey)
    speedy.Speedy.Machine
      .globalKey(pkgInterface, templateId, preprocessedKey)
      .getOrElse(
        throw Error.Preprocessing.ContractIdInContractKey(contractKey)
      )
  }

  def upgradable(tyCon: Ref.TypeConName): Boolean =
    handleLookup(pkgInterface.lookupPackage(tyCon.packageId)).upgradable

}
