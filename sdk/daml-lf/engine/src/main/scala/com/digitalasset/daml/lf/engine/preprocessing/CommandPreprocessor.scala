// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.GlobalKey

private[lf] final class CommandPreprocessor(
    pkgInterface: language.PackageInterface,
    forbidLocalContractIds: Boolean,
) {

  import Preprocessor._

  private val valueTranslator =
    new ValueTranslator(
      pkgInterface = pkgInterface,
      forbidLocalContractIds = forbidLocalContractIds,
    )

  def unsafeTranslateValue(
      ty: Ast.Type,
      value: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): SValue = valueTranslator.unsafeTranslateValue(ty, value, extendLocalIdForbiddanceToRelativeV2)

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): speedy.Command.Create = {
    discard(handleLookup(pkgInterface.lookupTemplate(templateId)))
    val arg =
      unsafeTranslateValue(Ast.TTyCon(templateId), argument, extendLocalIdForbiddanceToRelativeV2)
    speedy.Command.Create(templateId, arg)
  }

  def unsafePreprocessExercise(
      typeId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): speedy.ApiCommand =
    handleLookup(pkgInterface.lookupTemplateOrInterface(typeId)) match {
      case TemplateOrInterface.Template(_) =>
        unsafePreprocessExerciseTemplate(
          typeId,
          contractId,
          choiceId,
          argument,
          extendLocalIdForbiddanceToRelativeV2,
        )
      case TemplateOrInterface.Interface(_) =>
        unsafePreprocessExerciseInterface(
          typeId,
          contractId,
          choiceId,
          argument,
          extendLocalIdForbiddanceToRelativeV2,
        )
    }

  def unsafePreprocessExerciseTemplate(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): speedy.Command.ExerciseTemplate = {
    val choice = handleLookup(pkgInterface.lookupTemplateChoice(templateId, choiceId))
    speedy.Command.ExerciseTemplate(
      templateId = templateId,
      contractId =
        valueTranslator.unsafeTranslateCid(contractId, extendLocalIdForbiddanceToRelativeV2),
      choiceId = choiceId,
      argument =
        unsafeTranslateValue(choice.argBinder._2, argument, extendLocalIdForbiddanceToRelativeV2),
    )
  }

  def unsafePreprocessExerciseInterface(
      ifaceId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): speedy.Command.ExerciseInterface = {
    val choice = handleLookup(pkgInterface.lookupInterfaceChoice(ifaceId, choiceId))
    speedy.Command.ExerciseInterface(
      interfaceId = ifaceId,
      contractId =
        valueTranslator.unsafeTranslateCid(contractId, extendLocalIdForbiddanceToRelativeV2),
      choiceId = choiceId,
      argument =
        unsafeTranslateValue(choice.argBinder._2, argument, extendLocalIdForbiddanceToRelativeV2),
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value,
      choiceId: Ref.ChoiceName,
      argument: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): speedy.Command.ExerciseByKey = {
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val arg = unsafeTranslateValue(choiceArgType, argument, extendLocalIdForbiddanceToRelativeV2)
    val key = unsafeTranslateValue(ckTtype, contractKey, extendLocalIdForbiddanceToRelativeV2)
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg)
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value,
      choiceId: Ref.ChoiceName,
      choiceArgument: Value,
  ): speedy.Command.CreateAndExercise = {
    val extendLocalIdForbiddanceToRelativeV2 = true
    val createArg =
      unsafeTranslateValue(
        Ast.TTyCon(templateId),
        createArgument,
        extendLocalIdForbiddanceToRelativeV2,
      )
    val choiceArgType = handleLookup(
      pkgInterface.lookupTemplateChoice(templateId, choiceId)
    ).argBinder._2
    val choiceArg =
      unsafeTranslateValue(
        choiceArgType,
        choiceArgument,
        extendLocalIdForbiddanceToRelativeV2,
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
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val key = unsafeTranslateValue(ckTtype, contractKey, extendLocalIdForbiddanceToRelativeV2)
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
  ): speedy.ApiCommand = {
    val extendLocalIdForbiddanceToRelativeV2 = true
    cmd match {
      case command.ApiCommand.Create(templateRef, argument) =>
        val templateId =
          unsafeResolveTyConId(
            pkgResolution,
            templateRef,
            language.Reference.Template(templateRef),
          )
        unsafePreprocessCreate(templateId, argument, extendLocalIdForbiddanceToRelativeV2)
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
          extendLocalIdForbiddanceToRelativeV2,
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
          extendLocalIdForbiddanceToRelativeV2,
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
  }

  // returns the speedy translation of an Replay command.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafePreprocessReplayCommand(
      cmd: command.ReplayCommand
  ): speedy.Command = {
    val extendLocalIdForbiddanceToRelativeV2 = true
    cmd match {
      case command.ReplayCommand.Create(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument, extendLocalIdForbiddanceToRelativeV2)
      case command.ReplayCommand.Exercise(templateId, mbIfaceId, coid, choiceId, argument) =>
        mbIfaceId match {
          case Some(ifaceId) =>
            unsafePreprocessExerciseInterface(
              ifaceId,
              coid,
              choiceId,
              argument,
              extendLocalIdForbiddanceToRelativeV2,
            )
          case None =>
            unsafePreprocessExerciseTemplate(
              templateId,
              coid,
              choiceId,
              argument,
              extendLocalIdForbiddanceToRelativeV2,
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
          extendLocalIdForbiddanceToRelativeV2,
        )
      case command.ReplayCommand.Fetch(tmplId, ifaceIdOpt, coid) =>
        val cid = valueTranslator.unsafeTranslateCid(coid, extendLocalIdForbiddanceToRelativeV2)
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
        val sKey = unsafeTranslateValue(ckTtype, key, extendLocalIdForbiddanceToRelativeV2)
        speedy.Command.FetchByKey(templateId, sKey)
      case command.ReplayCommand.LookupByKey(templateId, key) =>
        val ckTtype = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
        val sKey = unsafeTranslateValue(ckTtype, key, extendLocalIdForbiddanceToRelativeV2)
        speedy.Command.LookupByKey(templateId, sKey)
    }
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessApiCommands(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmds: ImmArray[command.ApiCommand],
  ): ImmArray[speedy.ApiCommand] =
    cmds.map(unsafePreprocessApiCommand(pkgResolution, _))

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
      unsafeTranslateValue(
        Ast.TTyCon(templateId),
        argument,
        extendLocalIdForbiddanceToRelativeV2 = true,
      )

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
    unsafePreprocessContractKey(
      key.contractKey,
      templateId,
      extendLocalIdForbiddanceToRelativeV2 = true,
    )
  }

  @throws[Error.Preprocessing.Error]
  def unsafePreprocessContractKey(
      contractKey: Value,
      templateId: Ref.TypeConId,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): GlobalKey = {
    val ckTtype: Ast.Type = handleLookup(pkgInterface.lookupTemplateKey(templateId)).typ
    val preprocessedKey =
      unsafeTranslateValue(ckTtype, contractKey, extendLocalIdForbiddanceToRelativeV2)
    speedy.Speedy.Machine
      .globalKey(pkgInterface, templateId, preprocessedKey)
      .getOrElse(
        throw Error.Preprocessing.ContractIdInContractKey(contractKey)
      )
  }
}
