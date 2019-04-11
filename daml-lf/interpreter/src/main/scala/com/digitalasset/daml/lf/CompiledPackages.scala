// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.data.Ref.{DefinitionRef, PackageId}
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.daml.lf.speedy.{Compiler, SExpr}

/** Trait to abstract over a collection holding onto DAML-LF package definitions + the
  * compiled speedy expressions.
  */
trait CompiledPackages {
  def getPackage(pkgId: PackageId): Option[Package]
  def getDefinition(dref: DefinitionRef[PackageId]): Option[SExpr]

  def packages: PartialFunction[PackageId, Package] = Function.unlift(this.getPackage)
  def definitions: PartialFunction[DefinitionRef[PackageId], SExpr] =
    Function.unlift(this.getDefinition)
}

final class PureCompiledPackages private (
    packages: Map[PackageId, Package],
    defns: Map[DefinitionRef[PackageId], SExpr])
    extends CompiledPackages {
  override def getPackage(pkgId: PackageId): Option[Package] = packages.get(pkgId)
  override def getDefinition(dref: DefinitionRef[PackageId]): Option[SExpr] = defns.get(dref)
}

object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  def apply(
      packages: Map[PackageId, Package],
      defns: Map[DefinitionRef[PackageId], SExpr]): Either[String, PureCompiledPackages] = {
    Right(new PureCompiledPackages(packages, defns))
  }

  def apply(packages: Map[PackageId, Package]): Either[String, PureCompiledPackages] = {
    apply(packages, Compiler(packages).compilePackages(packages.keys))
  }
}
