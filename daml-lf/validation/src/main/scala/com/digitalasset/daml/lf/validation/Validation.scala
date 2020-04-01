// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.{ModuleName, PackageId}
import com.daml.lf.language.Ast.{Module, Package}

object Validation {

  def checkPackage(
      pkgs: PartialFunction[PackageId, Package],
      pkgId: PackageId
  ): Either[ValidationError, Unit] =
    try {
      val world = new World(pkgs)
      Right(checkPackage(world, pkgId, world.lookupPackage(NoContext, pkgId).modules))
    } catch {
      case e: ValidationError =>
        Left(e)
    }

  private def checkPackage(
      world: World,
      pkgId: PackageId,
      modules: Map[ModuleName, Module]
  ): Unit = {
    Collision.checkPackage(pkgId, modules)
    Recursion.checkPackage(pkgId, modules)
    modules.values.foreach(checkModule(world, pkgId, _))
  }

  private def checkModule(world: World, pkgId: PackageId, mod: Module): Unit = {
    Typing.checkModule(world, pkgId, mod)
    Serializability.checkModule(world, pkgId, mod)
    PartyLiterals.checkModule(world, pkgId, mod)
  }
}
