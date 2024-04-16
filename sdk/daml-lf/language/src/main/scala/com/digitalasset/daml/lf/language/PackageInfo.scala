// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language
package util

import data.{Ref, Relation}

class PackageInfo(pkgSignature: Map[Ref.PackageId, Ast.GenPackage[_]]) {

  /** returns the set of templates defined in `pkgSignature` */
  def definedTemplates: Set[Ref.Identifier] = templates.map(_._1).toSet

  /** returns the set of interfaces defined in `pkgSignature` */
  def definedInterfaces: Set[Ref.Identifier] = interfaces.map(_._1).toSet

  /** return the relation between interfaces and all their direct implementation
    * as defined in `pkgSignature`.
    * The domain of the relation is the set of interface names, and the codomain
    * is the set of template name.
    * Note that while interfaces may not be defined in `pkgSignature`, all template
    * are.
    */
  def interfacesDirectImplementations: Relation[Ref.Identifier, Ref.Identifier] =
    Relation.from(
      templates.flatMap { case (tmplId, tmpl) => tmpl.implements.keysIterator.map(_ -> tmplId) }
    )

  /** return the relation between interfaces and all their retroactive implementations
    * as defined in `pkgSignature`.
    * The domain of the relation is the set of interface names, while the codomain
    * is the set of template names.
    * Note that while all interfaces are defined in `pkgSignature`, templates may not
    * be.
    */
  def interfacesRetroactiveInstances: Relation[Ref.Identifier, Ref.Identifier] =
    Relation.from(
      interfaces.flatMap { case (ifaceId, iface) =>
        iface.coImplements.keysIterator.map(tmplId => ifaceId -> tmplId)
      }
    )

  /* Union of interfacesDirectImplementations and interfacesRetroactiveInstances */
  def interfaceInstances: Relation[Ref.Identifier, Ref.Identifier] =
    Relation.union(interfacesDirectImplementations, interfacesRetroactiveInstances)

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
