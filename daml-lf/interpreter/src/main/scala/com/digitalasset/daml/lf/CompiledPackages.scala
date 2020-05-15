// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.speedy.SExpr.SDefinitionRef
import com.daml.lf.speedy.{Compiler, SExpr}

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

  def profilingMode: Compiler.ProfilingMode

  def compiler: Compiler = Compiler(packages, profilingMode)
}

final class PureCompiledPackages private (
    packages: Map[PackageId, Package],
    defns: Map[SDefinitionRef, SExpr],
    profiling: Compiler.ProfilingMode,
) extends CompiledPackages {
  override def packageIds: Set[PackageId] = packages.keySet
  override def getPackage(pkgId: PackageId): Option[Package] = packages.get(pkgId)
  override def getDefinition(dref: SDefinitionRef): Option[SExpr] = defns.get(dref)
  override def profilingMode = profiling
}

object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  private[lf] def apply(
      packages: Map[PackageId, Package],
      defns: Map[SDefinitionRef, SExpr],
      profiling: Compiler.ProfilingMode
  ): PureCompiledPackages =
    new PureCompiledPackages(packages, defns, profiling)

  def apply(
      packages: Map[PackageId, Package],
      profiling: Compiler.ProfilingMode = Compiler.NoProfile,
  ): Either[String, PureCompiledPackages] =
    Compiler
      .compilePackages(packages, profiling)
      .map(new PureCompiledPackages(packages, _, profiling))

}
