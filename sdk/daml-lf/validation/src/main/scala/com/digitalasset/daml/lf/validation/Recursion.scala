// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{Graphs, Util => AstUtil}
import com.digitalasset.daml.lf.language.iterable.TypeIterable

private[validation] object Recursion {

  /* Check there are no cycles in the module references */

  @throws[ValidationError]
  def checkPackage(pkgId: PackageId, pkg: Package): Unit = {
    val g = pkg.modules.map { case (modName0, mod) =>
      modName0 ->
        AstUtil
          .collectIdentifiers(mod)
          .collect {
            case Identifier(`pkgId`, QualifiedName(modName, _)) if modName != modName0 => modName
          }
          .toSet
    }

    Graphs.topoSort(g).left.foreach(cycle => throw EImportCycle(Context.None, cycle.vertices))

    pkg.modules.foreach { case (modName, mod) => checkModule(pkgId, modName, mod) }
  }

  /* Check there are no cycles in the type synonym definitions of a module */

  private def checkModule(pkgId: PackageId, modName: ModuleName, mod: Module): Unit = {
    val g =
      mod.definitions.collect { case (dottedName, DTypeSyn(_, replacementTyp)) =>
        val name = Identifier(pkgId, QualifiedName(modName, dottedName))
        (name, synRefsOfType(Set.empty, replacementTyp))
      }
    Graphs.topoSort(g).left.foreach(cycle => throw ETypeSynCycle(Context.None, cycle.vertices))
  }

  private def synRefsOfType(acc: Set[TypeSynName], typ: Type): Set[TypeSynName] = typ match {
    case TSynApp(typeSynName, _) =>
      (TypeIterable(typ) foldLeft (acc + typeSynName))(synRefsOfType)
    case otherwise =>
      (TypeIterable(otherwise) foldLeft acc)(synRefsOfType)
  }

}
