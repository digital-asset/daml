// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.speedy.SExpr.SDefinitionRef
import com.digitalasset.daml.lf.speedy.{Compiler, SExpr}

/** Trait to abstract over a collection holding onto DAML-LF package definitions + the
  * compiled speedy expressions.
  */
trait CompiledPackages {
  def getPackage(pkgId: PackageId): Option[Package]
  def getDefinition(dref: SDefinitionRef): Option[SExpr]

  def packages: PartialFunction[PackageId, Package] = Function.unlift(this.getPackage)
  def packageIds: Set[PackageId]
  def definitions: PartialFunction[SDefinitionRef, SExpr] =
    Function.unlift(this.getDefinition)
}

final class PureCompiledPackages private (
    packages: Map[PackageId, Package],
    defns: Map[SDefinitionRef, SExpr],
) extends CompiledPackages {
  override def packageIds = packages.keySet
  override def getPackage(pkgId: PackageId): Option[Package] = packages.get(pkgId)
  override def getDefinition(dref: SDefinitionRef): Option[SExpr] = defns.get(dref)
}

object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  def apply(
      packages: Map[PackageId, Package],
      defns: Map[SDefinitionRef, SExpr],
  ): Either[String, PureCompiledPackages] = {
    Right(new PureCompiledPackages(packages, defns))
  }

  def apply(packages: Map[PackageId, Package]): Either[String, PureCompiledPackages] = {
    apply(packages, Compiler(packages).compilePackages(packages.keys))
  }
}
