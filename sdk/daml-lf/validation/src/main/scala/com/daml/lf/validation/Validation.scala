// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface

object Validation {

  private def runSafely[X](x: => X): Either[ValidationError, X] =
    try {
      Right(x)
    } catch {
      case e: ValidationError => Left(e)
    }

  def checkPackages(pkgs: Map[PackageId, Package]): Either[ValidationError, Unit] =
    runSafely {
      unsafeCheckPackages(PackageInterface(pkgs), pkgs)
    }

  private[lf] def checkPackages(
      pkgInterface: PackageInterface,
      pkgs: Map[PackageId, Package],
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckPackages(pkgInterface, pkgs))

  private[lf] def unsafeCheckPackages(
      pkgInterface: PackageInterface,
      pkgs: Map[PackageId, Package],
  ): Unit =
    pkgs.foreach { case (pkgId, pkg) => unsafeCheckPackage(pkgInterface, pkgId, pkg) }

  def checkPackage(
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      pkg: Package,
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckPackage(pkgInterface, pkgId, pkg))

  private def unsafeCheckPackage(
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      pkg: Package,
  ): Unit = {
    Collision.checkPackage(pkgId, pkg)
    Recursion.checkPackage(pkgId, pkg)
    DependencyVersion.checkPackage(pkgInterface, pkgId, pkg)
    pkg.modules.values.foreach(unsafeCheckModule(pkgInterface, pkgId, _))
  }

  private[lf] def checkModule(
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      module: Module,
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckModule(pkgInterface, pkgId, module))

  private def unsafeCheckModule(
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      mod: Module,
  ): Unit = {
    Typing.checkModule(pkgInterface, pkgId, mod)
    Serializability.checkModule(pkgInterface, pkgId, mod)
  }
}
