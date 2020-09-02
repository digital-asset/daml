// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.VersionTimeline

private[validation] object DependencyVersion {

  @throws[ValidationError]
  def checkPackage(world: World, pkgId: PackageId, pkg: Package): Unit = {
    import VersionTimeline.Implicits._

    for {
      depPkgId <- pkg.directDeps
      depPkg = world.lookupPackage(NoContext, depPkgId)
      if pkg.languageVersion precedes depPkg.languageVersion
    } throw EModuleVersionDependencies(
      pkgId,
      pkg.languageVersion,
      depPkgId,
      depPkg.languageVersion
    )
  }

}
