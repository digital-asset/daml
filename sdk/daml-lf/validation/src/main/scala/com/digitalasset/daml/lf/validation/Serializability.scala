// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.PackageInterface

private[validation] object Serializability {

  import Util.handleLookup

  case class Env(
      pkgInterface: PackageInterface,
      ctx: Context,
      requirement: SerializabilityRequirement,
      typeToSerialize: Type,
      vars: Set[TypeVarName] = Set.empty,
  ) {

    def unserializable(reason: UnserializabilityReason): Nothing =
      throw EExpectedSerializableType(ctx, requirement, typeToSerialize, reason)

    def introVar(v: (TypeVarName, Kind)): Env =
      v match {
        case (x, KStar) => this.copy(vars = vars + x)
        case (x, k) => unserializable(URHigherKinded(x, k))
      }

    def checkType(): Unit = checkType(typeToSerialize)

    def checkType(typ0: Type): Unit = typ0 match {
      case TApp(TBuiltin(BTContractId), _) =>
      /* Nothing to check. */
      case TVar(name) =>
        if (!vars(name)) unserializable(URFreeVar(name))
      case TNat(_) =>
        unserializable(URNat)
      case TSynApp(syn, _) => unserializable(URTypeSyn(syn))
      case TTyCon(tycon) =>
        handleLookup(ctx, pkgInterface.lookupDefinition(tycon)) match {
          case DDataType(true, _, _) => ()
          case _ => unserializable(URDataType(tycon))
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
          case BTInt64 | BTText | BTTimestamp | BTDate | BTParty | BTBool | BTUnit |
              BTAnyException =>
            ()
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
          case BTRoundingMode =>
            unserializable(URRoundingMode)
          case BTBigNumeric =>
            unserializable(URBigNumeric)
          case BTFailureCategory =>
            unserializable(URFailureCategory)
        }
      case TForall(_, _) =>
        unserializable(URForall)
      case TStruct(_) =>
        unserializable(URStruct)
    }
  }

  def checkDataType(
      pkgInterface: PackageInterface,
      tyCon: TTyCon,
      params: ImmArray[(TypeVarName, Kind)],
      dataCons: DataCons,
  ): Unit = {
    val context = Context.DefDataType(tyCon.tycon)
    val env =
      (params.iterator foldLeft Env(pkgInterface, context, SRDataType, tyCon))(
        _.introVar(_)
      )
    val typs = dataCons match {
      case DataVariant(variants) =>
        if (variants.isEmpty) env.unserializable(URUninhabitatedType)
        else variants.iterator.map(_._2)
      case DataEnum(constructors) =>
        if (constructors.isEmpty) env.unserializable(URUninhabitatedType)
        else Iterator.empty
      case DataRecord(fields) =>
        fields.iterator.map(_._2)
      case DataInterface =>
        env.unserializable(URInterface)
    }
    typs.foreach(env.checkType)
  }

  // Assumes template are well typed,
  // in particular choice argument types and choice return types are of kind KStar
  def checkTemplate(
      pkgInterface: PackageInterface,
      tyCon: TTyCon,
      template: Template,
  ): Unit = {
    val context = Context.Template(tyCon.tycon)
    Env(pkgInterface, context, SRTemplateArg, tyCon).checkType()
    template.choices.values.foreach { choice =>
      Env(pkgInterface, context, SRChoiceArg, choice.argBinder._2).checkType()
      Env(pkgInterface, context, SRChoiceRes, choice.returnType).checkType()
    }
    template.key.foreach(k => Env(pkgInterface, context, SRKey, k.typ).checkType())
  }

  def checkException(
      pkgInterface: PackageInterface,
      tyCon: TTyCon,
  ): Unit = {
    val context = Context.DefException(tyCon.tycon)
    Env(pkgInterface, context, SRExceptionArg, tyCon).checkType()
  }

  def checkInterface(
      pkgInterface: PackageInterface,
      tyCon: TTyCon,
      defInterface: DefInterface,
  ): Unit = {
    val context = Context.DefInterface(tyCon.tycon)

    defInterface.choices.values.foreach { choice =>
      Env(pkgInterface, context, SRChoiceArg, choice.argBinder._2).checkType()
      Env(pkgInterface, context, SRChoiceRes, choice.returnType).checkType()
    }

    Env(pkgInterface, context, SRView, defInterface.view).checkType()
  }

  def checkModule(pkgInterface: PackageInterface, pkgId: PackageId, module: Module): Unit = {
    module.definitions.foreach {
      case (defName, DDataType(serializable, params, dataCons)) =>
        val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
        if (serializable) checkDataType(pkgInterface, tyCon, params, dataCons)
      case _ =>
    }
    module.templates.foreach { case (defName, template) =>
      val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
      checkTemplate(pkgInterface, tyCon, template)
    }
    module.exceptions.keys.foreach { defName =>
      val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
      checkException(pkgInterface, tyCon)
    }
    module.interfaces.foreach { case (defName, defInterface) =>
      val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
      checkInterface(pkgInterface, tyCon, defInterface)
    }
  }
}
