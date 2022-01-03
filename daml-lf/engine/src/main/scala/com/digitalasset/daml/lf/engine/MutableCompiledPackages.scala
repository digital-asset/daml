// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.speedy.Compiler

/** Trait that extends [[CompiledPackages]] with the ability to
  * add new packages.
  */
abstract class MutableCompiledPackages(
    compilerConfig: Compiler.Config
) extends CompiledPackages(compilerConfig) {

  def addPackage(pkgId: PackageId, pkg: Package): Result[Unit]

  /** Get the transitive dependencies of the given package.
    * Returns 'None' if the package does not exist.
    */
  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]]

  def clear(): Unit
}
