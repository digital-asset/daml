// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast._

object Validation {

  private def runSafely[X](x: => X): Either[ValidationError, X] =
    try {
      Right(x)
    } catch {
      case e: ValidationError => Left(e)
    }

  def checkPackages(pkgs: Map[PackageId, Package]): Either[ValidationError, Unit] =
    runSafely {
      val world = new World(pkgs)
      pkgs.foreach { case (pkgId, pkg) => unsafeCheckPackage(world, pkgId, pkg) }
    }

  def checkPackage(
      interfaces: PartialFunction[PackageId, GenPackage[_]],
      pkgId: PackageId,
      pkg: Package,
  ): Either[ValidationError, Unit] =
    runSafely {
      val lookForThisPackage: PartialFunction[PackageId, Package] = { case `pkgId` => pkg }
      val world = new World(lookForThisPackage orElse interfaces)
      unsafeCheckPackage(world, pkgId, pkg)
    }

  private def unsafeCheckPackage(
      world: World,
      pkgId: PackageId,
      pkg: Package
  ): Unit = {
    Collision.checkPackage(pkgId, pkg)
    Recursion.checkPackage(pkgId, pkg)
    DependencyVersion.checkPackage(world, pkgId, pkg)
    pkg.modules.values.foreach(unsafeCheckModule(world, pkgId, _))
  }

  private[lf] def checkModule(
      pkgs: PartialFunction[PackageId, GenPackage[_]],
      pkgId: PackageId,
      module: Module
  ): Either[ValidationError, Unit] =
    runSafely {
      val world = new World(pkgs)
      unsafeCheckModule(world, pkgId, module)
    }

  private def unsafeCheckModule(world: World, pkgId: PackageId, mod: Module): Unit = {
    Typing.checkModule(world, pkgId, mod)
    Serializability.checkModule(world, pkgId, mod)
    PartyLiterals.checkModule(world, pkgId, mod)
  }
}
