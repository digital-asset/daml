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
abstract class CompiledPackages {
  def getPackage(pkgId: PackageId): Option[Package]
  def getDefinition(dref: SDefinitionRef): Option[SExpr]

  def packages: PartialFunction[PackageId, Package] = Function.unlift(this.getPackage)
  def packageIds: Set[PackageId]
  def definitions: PartialFunction[SDefinitionRef, SExpr] =
    Function.unlift(this.getDefinition)

  def packageLanguageVersion: PartialFunction[PackageId, LanguageVersion]

  def stackTraceMode: Compiler.StackTraceMode
  def profilingMode: Compiler.ProfilingMode

  def compiler: Compiler = Compiler(packages, stackTraceMode, profilingMode)

  // computes the newest language version used in `pkg` and all its dependencies.
  // assumes that `maxVersionOfDependencies` is defined for all dependencies of `pkg`
  // returns None iff the package is empty.
  protected def computePackageLanguageVersion(
      dependenciesPackageVersion: PartialFunction[PackageId, LanguageVersion],
      pkg: Package,
  ): Option[LanguageVersion] = {
    import transaction.VersionTimeline.maxVersion
    val moduleVersions = pkg.modules.values.iterator.map(_.languageVersion)
    val dependencyVersions = pkg.directDeps.iterator.map(dependenciesPackageVersion)
    (moduleVersions ++ dependencyVersions).reduceOption(maxVersion[LanguageVersion])
  }
}

final class PureCompiledPackages private (
    packages: Map[PackageId, Package],
    defns: Map[SDefinitionRef, SExpr],
    stacktracing: Compiler.StackTraceMode,
    profiling: Compiler.ProfilingMode,
) extends CompiledPackages {
  override def packageIds: Set[PackageId] = packages.keySet
  override def getPackage(pkgId: PackageId): Option[Package] = packages.get(pkgId)
  override def getDefinition(dref: SDefinitionRef): Option[SExpr] = defns.get(dref)
  override def stackTraceMode = stacktracing
  override def profilingMode = profiling

  private[this] def sortedPkgIds: List[PackageId] = {
    val dependencyGraph = packageIds.view
      .flatMap(pkgId => getPackage(pkgId).map(pkg => pkgId -> pkg.directDeps).toList)
      .toMap
    language.Graphs
      .topoSort(dependencyGraph)
      .getOrElse(throw new IllegalArgumentException("cyclic package definitions"))
  }

  override val packageLanguageVersion: Map[PackageId, LanguageVersion] =
    sortedPkgIds.foldLeft(Map.empty[PackageId, LanguageVersion])(
      (acc, pkgId) =>
        computePackageLanguageVersion(acc, packages(pkgId)).fold(acc)(acc.updated(pkgId, _))
    )
}

object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  private[lf] def apply(
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
      .map(new PureCompiledPackages(packages, _, stacktracing, profiling))

}
