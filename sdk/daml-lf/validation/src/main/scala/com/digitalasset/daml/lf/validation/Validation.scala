// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

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
      val interface = PackageInterface(pkgs)
      unsafeCheckPackages(interface, pkgs)
    }

  private[lf] def checkPackages(
      interface: PackageInterface,
      pkgs: Map[PackageId, Package],
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckPackages(interface, pkgs))

  private[lf] def unsafeCheckPackages(
      interface: PackageInterface,
      pkgs: Map[PackageId, Package],
  ): Unit =
    pkgs.foreach { case (pkgId, pkg) => unsafeCheckPackage(interface, pkgId, pkg) }

  def checkPackage(
      interface: PackageInterface,
      pkgId: PackageId,
      pkg: Package,
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckPackage(interface, pkgId, pkg))

  private def unsafeCheckPackage(
      interface: PackageInterface,
      pkgId: PackageId,
      pkg: Package,
  ): Unit = {
    Collision.checkPackage(pkgId, pkg)
    Recursion.checkPackage(pkgId, pkg)
    DependencyVersion.checkPackage(interface, pkgId, pkg)
    pkg.modules.values.foreach(unsafeCheckModule(interface, pkgId, _))
  }

  private[lf] def checkModule(
      interface: PackageInterface,
      pkgId: PackageId,
      module: Module,
  ): Either[ValidationError, Unit] =
    runSafely(unsafeCheckModule(interface, pkgId, module))

  private def unsafeCheckModule(
      interface: PackageInterface,
      pkgId: PackageId,
      mod: Module,
  ): Unit = {
    Typing.checkModule(interface, pkgId, mod)
    Serializability.checkModule(interface, pkgId, mod)
  }
}
