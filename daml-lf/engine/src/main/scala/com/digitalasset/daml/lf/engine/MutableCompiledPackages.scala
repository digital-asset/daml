// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.Package

/** Trait that extends [[CompiledPackages]] with the ability to
  * add new packages.
  */
trait MutableCompiledPackages extends CompiledPackages {

  /** Add a new package and compile it to internal form. If package
    * depends on another package the call may return with [[ResultNeedPackage]].
    */
  def addPackage(pkgId: PackageId, pkg: Package): Result[Unit]

  /** Get the transitive dependencies of the given package.
    * Returns 'None' if the package does not exist. */
  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]]

  def clear(): Unit
}
