// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.{ModuleName, PackageId}
import com.daml.lf.language.Ast.{Module, Package}

object Validation {

  private def runSafely[X](x: => X): Either[ValidationError, X] =
    try {
      Right(x)
    } catch {
      case e: ValidationError => Left(e)
    }

  def checkPackage(
      pkgs: PartialFunction[PackageId, Package],
      pkgId: PackageId
  ): Either[ValidationError, Unit] =
    runSafely {
      val world = new World(pkgs)
      unsafeCheckPackage(world, pkgId, world.lookupPackage(NoContext, pkgId).modules)
    }

  private def unsafeCheckPackage(
      world: World,
      pkgId: PackageId,
      modules: Map[ModuleName, Module]
  ): Unit = {
    Collision.checkPackage(pkgId, modules)
    Recursion.checkPackage(pkgId, modules)
    modules.values.foreach(unsafeCheckModule(world, pkgId, _))
  }

  def checkModule(
      pkgs: PartialFunction[PackageId, Package],
      pkgId: PackageId,
      modName: ModuleName,
  ): Either[ValidationError, Unit] =
    runSafely {
      val world = new World(pkgs)
      unsafeCheckModule(world, pkgId, world.lookupModule(NoContext, pkgId, modName))
    }

  private def unsafeCheckModule(world: World, pkgId: PackageId, mod: Module): Unit = {
    Typing.checkModule(world, pkgId, mod)
    Serializability.checkModule(world, pkgId, mod)
    PartyLiterals.checkModule(world, pkgId, mod)
  }
}
