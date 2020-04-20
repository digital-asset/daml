// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Graphs
import com.daml.lf.validation.traversable.{ExprTraversable, TypeTraversable}

private[validation] object Recursion {

  /* Check there are no cycles in the module references */

  @throws[ValidationError]
  def checkPackage(pkgId: PackageId, modules: Map[ModuleName, Module]): Unit = {
    val g = modules.map {
      case (name, mod) => name -> (mod.definitions.values.flatMap(modRefs(pkgId, _)).toSet - name)
    }

    Graphs.topoSort(g).left.foreach(cycle => throw EImportCycle(NoContext, cycle.vertices))

    modules.foreach { case (modName, mod) => checkModule(pkgId, modName, mod) }
  }

  def modRefs(pkgId: PackageId, definition: Definition): Set[ModuleName] = {

    val modRefsInType: Set[ModuleName] = {

      def modRefsInType(acc: Set[ModuleName], typ0: Type): Set[ModuleName] = typ0 match {
        case TSynApp(typeSynName, _) if typeSynName.packageId == pkgId =>
          ((acc + typeSynName.qualifiedName.module) /: TypeTraversable(typ0))(modRefsInType)
        case TTyCon(typeConName) if typeConName.packageId == pkgId =>
          acc + typeConName.qualifiedName.module
        case otherwise =>
          (acc /: TypeTraversable(otherwise))(modRefsInType)
      }

      (Set.empty[ModuleName] /: TypeTraversable(definition))(modRefsInType)
    }

    val modRefsInVal: Set[ModuleName] = {

      def modRefsInVal(acc: Set[ModuleName], expr0: Expr): Set[ModuleName] = expr0 match {
        case EVal(valRef) if valRef.packageId == pkgId =>
          acc + valRef.qualifiedName.module
        case EAbs(binder @ _, body, ref) =>
          ref.iterator.toSet.filter(_.packageId == pkgId).map(_.qualifiedName.module) |
            (acc /: ExprTraversable(body))(modRefsInVal)
        case otherwise =>
          (acc /: ExprTraversable(otherwise))(modRefsInVal)
      }

      (Set.empty[ModuleName] /: ExprTraversable(definition))(modRefsInVal)

    }

    modRefsInType | modRefsInVal

  }

  /* Check there are no cycles in the type synonym definitions of a module */

  private def checkModule(pkgId: PackageId, modName: ModuleName, mod: Module): Unit = {
    val g =
      mod.definitions.collect {
        case (dottedName, DTypeSyn(_, replacementTyp)) =>
          val name = Identifier(pkgId, QualifiedName(modName, dottedName))
          (name, synRefsOfType(Set.empty, replacementTyp))
      }
    Graphs.topoSort(g).left.foreach(cycle => throw ETypeSynCycle(NoContext, cycle.vertices))
  }

  private def synRefsOfType(acc: Set[TypeSynName], typ: Type): Set[TypeSynName] = typ match {
    case TSynApp(typeSynName, _) =>
      ((acc + typeSynName) /: TypeTraversable(typ))(synRefsOfType)
    case otherwise =>
      (acc /: TypeTraversable(otherwise))(synRefsOfType)
  }

}
