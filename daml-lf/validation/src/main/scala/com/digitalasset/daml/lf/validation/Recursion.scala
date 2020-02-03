// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.validation.traversable.{ExprTraversable, TypeTraversable}

private[validation] object Recursion {

  /* Check there are no cycles in the module references */

  @throws[ValidationError]
  def checkPackage(pkgId: PackageId, modules: Map[ModuleName, Module]): Unit = {
    val g = modules.map {
      case (name, mod) => name -> (mod.definitions.values.flatMap(modRefs(pkgId, _)).toSet - name)
    }

    cycle(g).foreach(c => throw EImportCycle(NoContext, c))

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
    cycle(g).foreach(c => throw ETypeSynCycle(NoContext, c))
  }

  private def synRefsOfType(acc: Set[TypeSynName], typ: Type): Set[TypeSynName] = typ match {
    case TSynApp(typeSynName, _) =>
      ((acc + typeSynName) /: TypeTraversable(typ))(synRefsOfType)
    case otherwise =>
      (acc /: TypeTraversable(otherwise))(synRefsOfType)
  }

  private def cycle[X](graph: Map[X, Set[X]]): Option[List[X]] = {

    var white = graph.keySet
    var black = (Set.empty[X] /: graph.values)(_ | _) -- white
    def gray(x: X): Boolean = !white(x) && !black(x)

    def visitSet(xs: Set[X]): Option[X] = (Option.empty[X] /: xs)(_ orElse visit(_))

    def visit(x: X): Option[X] =
      if (black(x))
        None
      else if (!white(x))
        Some(x)
      else { white -= x; visitSet(graph(x)) } orElse { black += x; None }

    def buildCycle(curr: X, start: X, list: List[X] = List.empty): List[X] = {
      val next = graph(curr).find(gray).getOrElse(throw new UnknownError)
      if (next == start)
        curr :: list
      else
        buildCycle(next, start, curr :: list)
    }

    visitSet(graph.keySet).map(x => buildCycle(x, x))
  }
}
