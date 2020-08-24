// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SExpr.SDefinitionRef
import com.daml.lf.speedy.{Compiler, SExpr}

/** Trait to abstract over a collection holding onto DAML-LF package definitions + the
  * compiled speedy expressions.
  */
private[lf] abstract class CompiledPackages(
    stackTraceMode: Compiler.StackTraceMode,
    profilingMode: Compiler.ProfilingMode,
) {
  def getPackage(pkgId: PackageId): Option[Package]
  def getDefinition(dref: SDefinitionRef): Option[SExpr]

  def packages: PartialFunction[PackageId, Package] = Function.unlift(this.getPackage)
  def packageIds: Set[PackageId]
  def definitions: PartialFunction[SDefinitionRef, SExpr] =
    Function.unlift(this.getDefinition)

  def packageLanguageVersion: PartialFunction[PackageId, LanguageVersion] =
    packages andThen (_.languageVersion)

  final def compiler: Compiler = Compiler(packages, stackTraceMode, profilingMode)
}

/** Important: use the constructor only if you _know_ you have all the definitions! Otherwise
  * use the apply in the companion object, which will compile them for you.
  */
private[lf] final class PureCompiledPackages(
    packages: Map[PackageId, Package],
    defns: Map[SDefinitionRef, SExpr],
    stackTraceMode: Compiler.StackTraceMode,
    profilingMode: Compiler.ProfilingMode,
) extends CompiledPackages(stackTraceMode, profilingMode) {
  override def packageIds: Set[PackageId] = packages.keySet
  override def getPackage(pkgId: PackageId): Option[Package] = packages.get(pkgId)
  override def getDefinition(dref: SDefinitionRef): Option[SExpr] = defns.get(dref)
}

private[lf] object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  def apply(
      packages: Map[PackageId, Package],
      defns: Map[SDefinitionRef, SExpr],
      stacktracing: Compiler.StackTraceMode,
      profiling: Compiler.ProfilingMode
  ): PureCompiledPackages =
    new PureCompiledPackages(packages, defns, stacktracing, profiling)

  def apply(
      packages: Map[PackageId, Package],
      stacktracing: Compiler.StackTraceMode = Compiler.FullStackTrace,
      profiling: Compiler.ProfilingMode = Compiler.NoProfile,
  ): Either[String, PureCompiledPackages] =
    Compiler
      .compilePackages(packages, stacktracing, profiling)
      .map(apply(packages, _, stacktracing, profiling))

}
