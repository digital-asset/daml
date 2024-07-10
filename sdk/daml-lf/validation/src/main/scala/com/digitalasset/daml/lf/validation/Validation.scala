// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package validation

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{PackageInterface, StablePackages}

object Validation {

  private def runSafely[X](x: => X): Either[ValidationError, X] =
    try {
      Right(x)
    } catch {
      case e: ValidationError => Left(e)
    }

  def checkPackages(
      stablePackages: StablePackages,
      pkgs: Map[PackageId, Package],
  ): Either[ValidationError, Unit] =
    runSafely(
      unsafeCheckPackages(stablePackages, PackageInterface(pkgs), pkgs)
    )

  private[lf] def checkPackages(
      stablePackages: StablePackages,
      pkgInterface: PackageInterface,
      pkgs: Map[PackageId, Package],
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckPackages(stablePackages, pkgInterface, pkgs))

  private[lf] def unsafeCheckPackages(
      stablePackages: StablePackages,
      pkgInterface: PackageInterface,
      pkgs: Map[PackageId, Package],
  ): Unit =
    pkgs.foreach { case (pkgId, pkg) =>
      unsafeCheckPackage(stablePackages, pkgInterface, pkgId, pkg)
    }

  def checkPackage(
      stablePackages: StablePackages,
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      pkg: Package,
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckPackage(stablePackages, pkgInterface, pkgId, pkg))

  private def unsafeCheckPackage(
      stablePackages: StablePackages,
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      pkg: Package,
  ): Unit = {
    Collision.checkPackage(pkgId, pkg)
    Recursion.checkPackage(pkgId, pkg)
    DependencyVersion.checkPackage(pkgInterface, pkgId, pkg)
    pkg.modules.values.foreach(unsafeCheckModule(stablePackages, pkgInterface, pkgId, _))
  }

  private[lf] def checkModule(
      stablePackages: StablePackages,
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      module: Module,
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckModule(stablePackages, pkgInterface, pkgId, module))

  private def unsafeCheckModule(
      stablePackages: StablePackages,
      pkgInterface: PackageInterface,
      pkgId: PackageId,
      mod: Module,
  ): Unit = {
    Typing.checkModule(stablePackages, pkgInterface, pkgId, mod)
    Serializability.checkModule(pkgInterface, pkgId, mod)
  }
}
