// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.InsertOrdSet
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.speedy.SExpr.SDefinitionRef
import com.digitalasset.daml.lf.speedy.SExpr

/** Wrapper around [[MutableCompiledPackages]] to track
 * fetches of packages in order to implement package dependency
 * tracking during type-checking and interpretation.
 */
case class TrackingCompiledPackages(parent: MutableCompiledPackages) extends MutableCompiledPackages {
  private var usedPackages = InsertOrdSet.empty[PackageId]
  def getUsedPackages: InsertOrdSet[PackageId] = usedPackages

  override def packageIds = parent.packageIds

  override def addPackage(pkgId: PackageId, pkg: Package): Result[Unit] =
    parent.addPackage(pkgId, pkg)

  override def clear(): Unit =
    parent.clear()

  override def getPackage(pkgId: PackageId): Option[Package] =
    parent.getPackage(pkgId)
      .map { pkg =>
        usedPackages += pkgId
        pkg
      }

  override def getDefinition(dref: SDefinitionRef): Option[SExpr] =
    parent.getDefinition(dref)
      .map { defn =>
        usedPackages += dref.packageId
        defn
      }
}
