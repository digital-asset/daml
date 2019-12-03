// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.Package

/** Trait that extends [[CompiledPackages]] with the ability to
 * add new packages.
 */
trait MutableCompiledPackages extends CompiledPackages {
  def addPackage(pkgId: PackageId, pkg: Package): Result[Unit]
  def clear(): Unit
}

