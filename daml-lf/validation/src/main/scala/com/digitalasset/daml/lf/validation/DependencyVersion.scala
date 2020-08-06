// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.VersionTimeline

private[validation] object DependencyVersion {

  @throws[ValidationError]
  def checkModule(world: World, pkgId: PackageId, mod: Module): Unit = {
    import VersionTimeline.Implicits._

    val modLangVersion = mod.languageVersion
    val modName = mod.name
    val ctx = ContextModule(pkgId, modName)

    val dependencies = world.idsInModule(ctx, pkgId, modName).collect {
      case Identifier(depPkgId, QualifiedName(depModName, _))
          if depPkgId != pkgId || depModName != modName =>
        (depPkgId, depModName)
    }

    dependencies.foreach {
      case (depPkgId, depModName) =>
        val depLangVersion = world.lookupModule(ctx, depPkgId, depModName).languageVersion
        if (modLangVersion precedes depLangVersion)
          throw EModuleVersionDependencies(
            pkgId,
            modName,
            modLangVersion,
            depPkgId,
            depModName,
            depLangVersion)
    }
  }

}
