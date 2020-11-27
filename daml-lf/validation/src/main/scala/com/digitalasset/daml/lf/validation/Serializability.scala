// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion

private[validation] object Serializability {

  case class Env(
      languageVersion: LanguageVersion,
      world: World,
      ctx: Context,
      requirement: SerializabilityRequirement,
      typeToSerialize: Type,
      vars: Set[TypeVarName] = Set.empty,
  ) {

    import world._

    def unserializable(reason: UnserializabilityReason): Nothing =
      throw EExpectedSerializableType(ctx, requirement, typeToSerialize, reason)

    def introVar(v: (TypeVarName, Kind)): Env =
      v match {
        case (x, KStar) => this.copy(vars = vars + x)
        case (x, k) => unserializable(URHigherKinded(x, k))
      }

    def checkType(): Unit = checkType(typeToSerialize)

    def checkType(typ0: Type): Unit = typ0 match {
      case TApp(TBuiltin(BTContractId), tArg) =>
        checkType(tArg)
      case TVar(name) =>
        if (!vars(name)) unserializable(URFreeVar(name))
      case TNat(_) =>
        unserializable(URNat)
      case TSynApp(syn, _) => unserializable(URTypeSyn(syn))
      case TTyCon(tycon) =>
        lookupDefinition(ctx, tycon) match {
          case DDataType(true, _, _) =>
            ()
          case _ =>
            unserializable(URDataType(tycon))
        }
      case TApp(TBuiltin(BTNumeric), TNat(_)) =>
      case TApp(TBuiltin(BTList), tArg) =>
        checkType(tArg)
      case TApp(TBuiltin(BTOptional), tArg) =>
        checkType(tArg)
      case TApp(TBuiltin(BTTextMap), tArg) =>
        checkType(tArg)
      case TApp(TApp(TBuiltin(BTGenMap), tKeys), tValues) =>
        checkType(tKeys)
        checkType(tValues)
      case TApp(tyfun, targ) =>
        checkType(tyfun)
        checkType(targ)
      case TBuiltin(builtinType) =>
        builtinType match {
          case BTInt64 | BTText | BTTimestamp | BTDate | BTParty | BTBool | BTUnit =>
          case BTNumeric =>
            unserializable(URNumeric)
          case BTList =>
            unserializable(URList)
          case BTOptional =>
            unserializable(UROptional)
          case BTTextMap =>
            unserializable(URTextMap)
          case BTGenMap =>
            unserializable(URGenMap)
          case BTUpdate =>
            unserializable(URUpdate)
          case BTScenario =>
            unserializable(URScenario)
          case BTContractId =>
            unserializable(URContractId)
          case BTArrow =>
            unserializable(URFunction)
          case BTAny =>
            unserializable(URAny)
          case BTTypeRep =>
            unserializable(URTypeRep)
        }
      case TForall(_, _) =>
        unserializable(URForall)
      case TStruct(_) =>
        unserializable(URStruct)
    }
  }

  def checkDataType(
      version: LanguageVersion,
      world: World,
      tyCon: TTyCon,
      params: ImmArray[(TypeVarName, Kind)],
      dataCons: DataCons): Unit = {
    val context = ContextDefDataType(tyCon.tycon)
    val env =
      (params.iterator foldLeft Env(version, world, context, SRDataType, tyCon))(_.introVar(_))
    val typs = dataCons match {
      case DataVariant(variants) =>
        if (variants.isEmpty) env.unserializable(URUninhabitatedType)
        else variants.iterator.map(_._2)
      case DataEnum(constructors) =>
        if (constructors.isEmpty) env.unserializable(URUninhabitatedType)
        else Iterator.empty
      case DataRecord(fields) =>
        fields.iterator.map(_._2)
    }
    typs.foreach(env.checkType)
  }

  // Assumes template are well typed,
  // in particular choice argument types and choice return types are of kind KStar
  def checkTemplate(
      version: LanguageVersion,
      world: World,
      tyCon: TTyCon,
      template: Template,
  ): Unit = {
    val context = ContextTemplate(tyCon.tycon)
    Env(version, world, context, SRTemplateArg, tyCon).checkType()
    template.choices.values.foreach { choice =>
      Env(version, world, context, SRChoiceArg, choice.argBinder._2).checkType()
      Env(version, world, context, SRChoiceRes, choice.returnType).checkType()
    }
    template.key.foreach(k => Env(version, world, context, SRKey, k.typ).checkType())
  }

  def checkModule(world: World, pkgId: PackageId, module: Module): Unit = {
    val version = world.lookupPackage(NoContext, pkgId).languageVersion
    module.definitions.foreach {
      case (defName, DDataType(serializable, params, dataCons)) =>
        val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
        if (serializable) checkDataType(version, world, tyCon, params, dataCons)
      case _ =>
    }
    module.templates.foreach {
      case (defName, template) =>
        val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
        checkTemplate(version, world, tyCon, template)
    }
  }
}
