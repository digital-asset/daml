// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.Compiler

/** Trait that extends [[CompiledPackages]] with the ability to
  * add new packages.
  */
abstract class MutableCompiledPackages(
    allowedLanguageVersions: VersionRange[LanguageVersion],
    stackTraceMode: speedy.Compiler.StackTraceMode,
    profilingMode: Compiler.ProfilingMode,
) extends CompiledPackages(stackTraceMode, profilingMode) {

  /** Add a new package and compile it to internal form. If package
    * depends on another package the call may return with [[ResultNeedPackage]].
    */
  final def addPackage(pkgId: PackageId, pkg: Package): Result[Unit] =
    if (allowedLanguageVersions.contains(pkg.languageVersion))
      addPackageInternal(pkgId, pkg)
    else
      ResultError(
        Error(
          s"Disallowed language version in package $pkgId: " +
            s"Expected version between ${allowedLanguageVersions.min} and ${allowedLanguageVersions.max} but got ${pkg.languageVersion}"
        )
      )

  protected def addPackageInternal(pkgId: PackageId, pkg: Package): Result[Unit]

  /** Get the transitive dependencies of the given package.
    * Returns 'None' if the package does not exist. */
  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]]

  def clear(): Unit
}
