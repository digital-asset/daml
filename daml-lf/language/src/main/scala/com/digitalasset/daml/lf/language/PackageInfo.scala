// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language
package util

import data.{Ref, Relation}

private[daml] class PackageInfo(pkgSignature: Map[Ref.PackageId, Ast.GenPackage[_]]) {

  /** returns the set of templates defined in `pkgSignature` */
  def definedTemplates: Set[Ref.TypeConName] = templates.map(_._1).toSet

  /** returns the set of interfaces defined in `pkgSignature` */
  def definedInterfaces: Set[Ref.TypeConName] = interfaces.map(_._1).toSet

  /** return the relation between interfaces and all their direct implementation
    * as defined in `pkgSignature`.
    * The domain of the relation is the set of interface names, and the codomain
    * is the set of template name.
    * Note that while interfaces may not be defined in `pkgSignature`, all template
    * are.
    */
  def interfacesDirectImplementations: Relation[Ref.TypeConName, Ref.TypeConName] =
    Relation.from(
      templates.flatMap { case (tmplId, tmpl) => tmpl.implements.keysIterator.map(_ -> tmplId) }
    )

  /** return the relation between interfaces and all their retroactive implementation
    * as defined in `pkgSignature`.
    * The domain of the relation is the set of interface names, while the codomain
    * is the set of template name.
    * Note that while all interfaces are defined in `pkgSignature`, template may not
    * be.orm
    */
  def interfacesRetroactiveInstances: Relation[Ref.TypeConName, Ref.TypeConName] =
    Relation.from(
      interfaces.flatMap { case (ifaceId, iface) =>
        iface.coImplements.keysIterator.map(tmplId => ifaceId -> tmplId)
      }
    )

  /* Union of interfacesDirectImplementations and interfacesRetroactiveInstances */
  def interfaceInstances: Relation[Ref.TypeConName, Ref.TypeConName] =
    Relation.union(interfacesDirectImplementations, interfacesRetroactiveInstances)

  def instanceInterfaces: Relation[Ref.TypeConName, Ref.TypeConName] =
    Relation.invert(interfaceInstances)

  private[this] def withFullId[X](
      pkgId: Ref.PackageId,
      mod: Ast.GenModule[_],
      tuple: (Ref.DottedName, X),
  ): (Ref.TypeConName, X) = tuple match {
    case (name, x) =>
      Ref.TypeConName(pkgId, Ref.QualifiedName(mod.name, name)) -> x
  }

  private[this] def modules: Iterable[(Ref.PackageId, Ast.GenModule[_])] =
    pkgSignature.view.flatMap { case (pkgId, pkg) => pkg.modules.values.map(pkgId -> _) }

  private[this] def templates: Iterable[(Ref.TypeConName, Ast.GenTemplate[_])] =
    modules.flatMap { case (pkgId, mod) => mod.templates.map(withFullId(pkgId, mod, _)) }

  private[this] def interfaces: Iterable[(Ref.TypeConName, Ast.GenDefInterface[_])] =
    modules.flatMap { case (pkgId, mod) => mod.interfaces.map(withFullId(pkgId, mod, _)) }

}
