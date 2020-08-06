// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Graphs
import com.daml.lf.validation.traversable.TypeTraversable

private[validation] object Recursion {

  /* Check there are no cycles in the module references */

  @throws[ValidationError]
  def checkPackage(world: World, pkgId: PackageId, modules: Map[ModuleName, Module]): Unit = {
    val g = modules.transform { (name, _) =>
      def context = ContextModule(pkgId, name)
      world.idsInModule(context, pkgId, name).collect {
        case Identifier(`pkgId`, QualifiedName(modName, _)) => modName
      } - name
    }

    Graphs.topoSort(g).left.foreach(cycle => throw EImportCycle(NoContext, cycle.vertices))

    modules.foreach { case (modName, mod) => checkModule(pkgId, modName, mod) }
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
      (TypeTraversable(typ) foldLeft (acc + typeSynName))(synRefsOfType)
    case otherwise =>
      (TypeTraversable(otherwise) foldLeft acc)(synRefsOfType)
  }

}
