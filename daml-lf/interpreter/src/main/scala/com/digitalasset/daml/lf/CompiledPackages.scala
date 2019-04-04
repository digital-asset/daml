// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.data.Ref.{DefinitionRef, PackageId}
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.daml.lf.speedy.{Compiler, SExpr}
import com.digitalasset.daml.lf.types.Ledger.LedgerFeatureFlags

/** Trait to abstract over a collection holding onto DAML-LF package definitions + the
  * compiled speedy expressions.
  */
trait CompiledPackages {
  def getPackage(pkgId: PackageId): Option[Package]
  def getDefinition(dref: DefinitionRef[PackageId]): Option[SExpr]
  def ledgerFlags(): LedgerFeatureFlags

  def packages: PartialFunction[PackageId, Package] = Function.unlift(this.getPackage)
  def definitions: PartialFunction[DefinitionRef[PackageId], SExpr] =
    Function.unlift(this.getDefinition)
}

object CompiledPackages {
  def newLedgerFlags(
      pkgId: PackageId,
      pkg: Package,
      mbPrefFlags: Option[LedgerFeatureFlags]): Either[String, LedgerFeatureFlags] = {
    // We seet all flags that can be mixed to false.
    val flagsList = pkg.modules.values.toList
      .map(_.featureFlags)
      .map(LedgerFeatureFlags.projectToUniqueFlags)
      .distinct
    if (flagsList.size > 1) {
      Left(s"Mixed feature flags in package $pkgId")
    } else {
      val newFlags = flagsList.headOption.getOrElse(LedgerFeatureFlags.default)
      mbPrefFlags match {
        case None => Right(newFlags)
        case Some(prevFlags) =>
          if (prevFlags != newFlags) {
            Left(s"Mixed feature flags across imported packages when importing $pkgId.")
          } else {
            Right(newFlags)
          }
      }
    }
  }
}

final class PureCompiledPackages private (
    packages: Map[PackageId, Package],
    defns: Map[DefinitionRef[PackageId], SExpr],
    _ledgerFlags: LedgerFeatureFlags)
    extends CompiledPackages {
  override def getPackage(pkgId: PackageId): Option[Package] = packages.get(pkgId)
  override def getDefinition(dref: DefinitionRef[PackageId]): Option[SExpr] = defns.get(dref)
  override def ledgerFlags(): LedgerFeatureFlags = _ledgerFlags
}

object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  def apply(
      packages: Map[PackageId, Package],
      defns: Map[DefinitionRef[PackageId], SExpr]): Either[String, PureCompiledPackages] = {
    var mbPrevFlags: Option[LedgerFeatureFlags] = None
    for (pkg <- packages) {
      CompiledPackages.newLedgerFlags(pkg._1, pkg._2, mbPrevFlags) match {
        case Left(err) => return Left(err)
        case Right(newFlags) => mbPrevFlags = Some(newFlags)
      }
    }
    Right(
      new PureCompiledPackages(packages, defns, mbPrevFlags.getOrElse(LedgerFeatureFlags.default)))
  }

  def apply(packages: Map[PackageId, Package]): Either[String, PureCompiledPackages] = {
    apply(packages, Compiler(packages).compilePackages(packages.keys))
  }
}
