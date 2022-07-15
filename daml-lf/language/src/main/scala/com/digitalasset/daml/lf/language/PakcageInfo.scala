// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language
package util

import data.Ref
import data.Relation.Relation

private[daml] class PackageInfo(pkgSignature: Map[Ref.PackageId, Ast.GenPackage[_]]) {

  def definedTemplates: Set[Ref.Identifier] = templates.map(_._1).toSet

  def definedInterfaces: Set[Ref.Identifier] = interfaces.map(_._1).toSet

  def interfaceDirectImplementation: Relation[Ref.Identifier, Ref.Identifier] =
    Relation.from(
      templates.flatMap { case (tmplId, tmpl) =>
        tmpl.implements.keysIterator.map(_ -> Set(tmplId))
      }
    )

  def interfaceRetroactiveInstances: Relation[Ref.Identifier, Ref.Identifier] =
    Relation.from(
      interfaces.flatMap { case (ifaceId, iface) =>
        iface.coImplements.keysIterator.map(tmplId => ifaceId -> Set(tmplId))
      }
    )

  def interfaceInstances: Relation[Ref.Identifier, Ref.Identifier] =
    Relation.union(interfaceDirectImplementation, interfaceRetroactiveInstances)

  private[this] def withFullId[X](
      pkgId: Ref.PackageId,
      mod: Ast.GenModule[_],
      tuple: (Ref.DottedName, X),
  ): (Ref.Identifier, X) = tuple match {
    case (name, x) =>
      Ref.Identifier(pkgId, Ref.QualifiedName(mod.name, name)) -> x
  }

  private[this] def modules: Iterable[(Ref.PackageId, Ast.GenModule[_])] =
    pkgSignature.view.flatMap { case (pkgId, pkg) => pkg.modules.values.map(pkgId -> _) }

  private[this] def templates: Iterable[(Ref.Identifier, Ast.GenTemplate[_])] =
    modules.flatMap { case (pkgId, mod) => mod.templates.map(withFullId(pkgId, mod, _)) }

  private[this] def interfaces: Iterable[(Ref.Identifier, Ast.GenDefInterface[_])] =
    modules.flatMap { case (pkgId, mod) => mod.interfaces.map(withFullId(pkgId, mod, _)) }

}

