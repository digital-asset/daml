// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Util.{TContractId, TList, TOptional, TMap}

private[validation] object Serializability {

  case class Env(
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
      case TContractId(TTyCon(tCon)) =>
        lookupTemplate(ctx, tCon)
        ()
      case TVar(name) =>
        if (!vars(name)) unserializable(URFreeVar(name))
      case TTyCon(tycon) =>
        lookupDefinition(ctx, tycon) match {
          case DDataType(true, _, _) =>
            ()
          case _ =>
            unserializable(URDataType(tycon))
        }
      case TList(tArg) =>
        checkType(tArg)
      case TOptional(tArg) =>
        checkType(tArg)
      case TMap(tArg) =>
        checkType(tArg)
      case TApp(tyfun, targ) =>
        checkType(tyfun)
        checkType(targ)
      case TBuiltin(builtinType) =>
        builtinType match {
          case BTInt64 | BTDecimal | BTText | BTTimestamp | BTDate | BTParty | BTBool | BTUnit =>
          case BTList =>
            unserializable(URList)
          case BTOptional =>
            unserializable(UROptional)
          case BTMap =>
            unserializable(URMap)
          case BTUpdate =>
            unserializable(URUpdate)
          case BTScenario =>
            unserializable(URScenario)
          case BTContractId =>
            unserializable(URContractId)
          case BTArrow =>
            unserializable(URFunction)
        }
      case TForall(_, _) =>
        unserializable(URForall)
      case TTuple(_) =>
        unserializable(URTuple)
    }
  }

  def checkDataType(
      world: World,
      tyCon: TTyCon,
      params: ImmArray[(TypeVarName, Kind)],
      dataCons: DataCons): Unit = {
    val context = ContextDefDataType(tyCon.tycon)
    val env = (Env(world, context, SRDataType, tyCon) /: params.iterator)(_.introVar(_))
    val typs = dataCons match {
      case DataVariant(variants) if variants.isEmpty =>
        env.unserializable(URUninhabitatedType)
      case DataVariant(variants) =>
        variants.iterator.map(_._2)
      case DataRecord(fields, _) =>
        fields.iterator.map(_._2)
    }

    typs.foreach(env.checkType)
  }

  // Assumes template are well typed,
  // in particular choice argument types and choice return types are of kind KStar
  def checkTemplate(world: World, tyCon: TTyCon, template: Template): Unit = {
    val context = ContextTemplate(tyCon.tycon)
    Env(world, context, SRTemplateArg, tyCon).checkType()
    template.choices.values.foreach { choice =>
      Env(world, context, SRChoiceArg, choice.argBinder._2).checkType()
      Env(world, context, SRChoiceRes, choice.returnType).checkType()
    }
    template.key.foreach(k => Env(world, context, SRKey, k.typ).checkType())
  }

  def checkModule(world: World, pkgId: PackageId, module: Module): Unit =
    module.definitions.foreach {
      case (defName, DDataType(serializable, params, dataCons)) =>
        val tyCon = TTyCon(Identifier(pkgId, QualifiedName(module.name, defName)))
        if (serializable) checkDataType(world, tyCon, params, dataCons)
        dataCons match {
          case DataRecord(_, Some(template)) =>
            checkTemplate(world, tyCon, template)
          case _ =>
        }
      case _ =>
    }

}
