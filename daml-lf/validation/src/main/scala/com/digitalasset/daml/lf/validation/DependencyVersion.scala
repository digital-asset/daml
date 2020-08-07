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
      pkgFirstModule <- pkg.modules.values.take(1)
      // all modules of a package are compiled to the same LF version
      pkgLangVersion = pkgFirstModule.languageVersion
      depPkgId <- pkg.directDeps
      depPkg = world.lookupPackage(NoContext, depPkgId)
      depFirstModule <- depPkg.modules.values.take(1)
      depLangVersion = depFirstModule.languageVersion
    } if (pkgLangVersion precedes depLangVersion)
      throw EModuleVersionDependencies(
        pkgId,
        pkgLangVersion,
        depPkgId,
        depLangVersion
      )
  }

}
