// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data.Ref

private[lf] final case class StablePackage(
    moduleName: Ref.ModuleName,
    packageId: Ref.PackageId,
    pkg: Ast.Package,
) {
  require(Set(moduleName) == pkg.modules.keySet)

  def name: Ref.PackageName = pkg.pkgName

  def languageVersion: LanguageVersion = pkg.languageVersion

  def identifier(idName: Ref.DottedName): Ref.Identifier =
    Ref.Identifier(packageId, Ref.QualifiedName(moduleName, idName))

  @throws[IllegalArgumentException]
  def assertIdentifier(idName: String): Ref.Identifier =
    identifier(Ref.DottedName.assertFromString(idName))
}

private[daml] abstract class StablePackages {
  val allPackages: Seq[StablePackage]

  val ArithmeticError: Ref.TypeConName
  val AnyChoice: Ref.TypeConName
  val AnyContractKey: Ref.TypeConName
  val AnyTemplate: Ref.TypeConName
  val TemplateTypeRep: Ref.TypeConName
  val NonEmpty: Ref.TypeConName
  val Tuple2: Ref.TypeConName
  val Tuple3: Ref.TypeConName
  val Either: Ref.TypeConName

  final def packagesMap: Map[Ref.PackageId, Ast.Package] =
    allPackages.view.map(sp => sp.packageId -> sp.pkg).toMap

}
