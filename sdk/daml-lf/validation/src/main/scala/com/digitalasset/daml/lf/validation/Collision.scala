// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast
import com.daml.lf.validation.NamedEntity._
import com.daml.lf.validation.Util._

private[validation] object Collision {

  def checkPackage(pkgId: PackageId, pkg: Ast.Package): Unit = {
    val entitiesMap = namedEntitiesFromPkg(pkg.modules).groupBy(_.fullyResolvedName)
    entitiesMap.values.foreach(cs => checkCollisions(pkgId, cs.toList))
  }

  private def allowedCollision(entity1: NamedEntity, entity2: NamedEntity): Boolean =
    (entity1, entity2) match {
      case (varCon: NVarCon, recDef: NRecDef) =>
        // This check is case sensitive
        varCon.module == recDef.module && (varCon.dfn.name + varCon.name) == recDef.name
      case (recDef: NRecDef, varCon: NVarCon) =>
        allowedCollision(varCon, recDef)
      case _ =>
        false
    }

  private def checkCollisions(pkgId: PackageId, candidates: List[NamedEntity]): Unit =
    for {
      (entity1, idx1) <- candidates.zipWithIndex
      (entity2, idx2) <- candidates.zipWithIndex
      if idx1 < idx2
      if !allowedCollision(entity1, entity2)
    } throw ECollision(pkgId, entity1, entity2)

  private def namedEntitiesFromPkg(
      modules: Iterable[(ModuleName, Ast.Module)]
  ): Iterable[NamedEntity] =
    modules.flatMap { case (modName, module) =>
      val namedModule = NModDef(modName)
      namedModule :: namedEntitiesFromMod(namedModule, module)
    }

  private def namedEntitiesFromMod(
      module: NModDef,
      astModule: Ast.Module,
  ): List[NamedEntity] =
    (astModule.definitions.toList.flatMap { case (defName, defn) =>
      namedEntitiesFromDef(module, defName, defn)
    } ++
      astModule.templates.toList.flatMap { case (tplName, tpl) =>
        namedEntitiesFromTemplate(module, tplName, tpl)
      }) ++
      astModule.interfaces.toList.flatMap { case (ifaceName, iface) =>
        namedEntitiesFromInterface(module, ifaceName, iface)
      }

  private def namedEntitiesFromDef(
      module: NModDef,
      defName: DottedName,
      defn: Ast.Definition,
  ): List[NamedEntity] =
    defn match {
      case Ast.DDataType(_, _, Ast.DataRecord(fields)) =>
        val recordDef = NRecDef(module, defName)
        recordDef :: fields.toList.map { case (name, _) => NField(recordDef, name) }
      case Ast.DDataType(_, _, Ast.DataVariant(variants)) =>
        val variantDef = NVarDef(module, defName)
        variantDef :: variants.toList.map { case (name, _) => NVarCon(variantDef, name) }
      case Ast.DDataType(_, _, Ast.DataEnum(values)) =>
        val enumDef = NEnumDef(module, defName)
        enumDef :: values.toList.map(NEnumCon(enumDef, _))
      case Ast.DDataType(_, _, Ast.DataInterface) =>
        val interfaceDef = NInterface(module, defName)
        interfaceDef :: List.empty
      case _: Ast.DValue =>
        // ignore values
        List.empty
      case _: Ast.DTypeSyn =>
        val synDef = NSynDef(module, defName)
        synDef :: List.empty
    }

  private def namedEntitiesFromTemplate(
      module: NModDef,
      tplName: DottedName,
      tpl: Ast.Template,
  ): List[NamedEntity] =
    tpl.choices.keys.map(NChoice(module, tplName, _)).toList

  private def namedEntitiesFromInterface(
      module: NModDef,
      ifaceName: DottedName,
      iface: Ast.DefInterface,
  ): List[NamedEntity] =
    iface.choices.keys.map(NInterfaceChoice(module, ifaceName, _)).toList
}
