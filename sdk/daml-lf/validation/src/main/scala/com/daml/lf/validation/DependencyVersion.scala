// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

import scala.Ordering.Implicits.infixOrderingOps

private[validation] object DependencyVersion {

  @throws[ValidationError]
  def checkPackage(pkgInterface: language.PackageInterface, pkgId: PackageId, pkg: Package): Unit =
    for {
      depPkgId <- pkg.directDeps
      depPkg = Util.handleLookup(Context.None, pkgInterface.lookupPackage(depPkgId))
      if pkg.languageVersion < depPkg.languageVersion
    } throw EModuleVersionDependencies(
      pkgId,
      pkg.languageVersion,
      depPkgId,
      depPkg.languageVersion,
    )

}
