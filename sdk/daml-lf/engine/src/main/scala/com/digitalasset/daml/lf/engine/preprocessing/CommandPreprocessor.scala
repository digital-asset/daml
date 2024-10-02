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
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.FatContractInstance

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
  ): SValue = valueTranslator.unsafeTranslateValue(ty, value, strict = false)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessDisclosedContract(
      disc: FatContractInstance
  ): speedy.DisclosedContract = {
    val arg = valueTranslator.unsafeTranslateValue(
      Ast.TTyCon(disc.templateId),
      disc.createArg,
      strict = true,
    )
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
      strict: Boolean,
  ): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument, strict = strict)
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
        unsafePreprocessExerciseTemplate(typeId, contractId, choiceId, argument, strict = strict)
      case TemplateOrInterface.Interface(_) =>
        unsafePreprocessExerciseInterface(typeId, contractId, choiceId, argument, strict = strict)
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
      argument =
        valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument, strict = strict),
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
      argument =
        valueTranslator.unsafeTranslateValue(choice.argBinder._2, argument, strict = strict),
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
    val arg = valueTranslator.unsafeTranslateValue(choiceArgType, argument, strict = strict)
    val key = valueTranslator.unsafeTranslateValue(ckTtype, contractKey, strict = strict)
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
    val createArg =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), createArgument, strict = strict)
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg =
      valueTranslator.unsafeTranslateValue(choiceArgType, choiceArgument, strict = strict)
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
    val key = valueTranslator.unsafeTranslateValue(ckTtype, contractKey, strict = strict)
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
            discard(handleLookup(pkgInterface.lookupInterfaceInstance(ifaceId, templateId)))
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
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key, strict = true)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = valueTranslator.unsafeTranslateValue(ckTtype, key, strict = true)
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
      discs: ImmArray[FatContractInstance]
  ): ImmArray[speedy.DisclosedContract] = {
    var contractIds: Set[Value.ContractId] = Set.empty

    discs.map { disclosedContract =>
      if (contractIds.contains(disclosedContract.contractId))
        throw Error.Preprocessing.DuplicateDisclosedContractId(disclosedContract.contractId)
      contractIds += disclosedContract.contractId
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

    val arg = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument, strict = true)

    speedy.InterfaceView(templateId, arg, interfaceId)
  }

}
