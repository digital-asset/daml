// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package stablepackages

import com.digitalasset.daml.lf.VersionRange
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.archive.ArchiveDecoder
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{
  Ast,
  LanguageMajorVersion,
  LanguageVersion,
  StablePackage,
  StablePackages,
}

final object StablePackagesV2
    extends StablePackagesImpl("compiler/damlc/stable-packages/stable-packages-manifest-v2.txt")

private[daml] object StablePackages {
  def apply(languageMajorVersion: LanguageMajorVersion): StablePackages =
    languageMajorVersion match {
      case LanguageMajorVersion.V1 => throw new IllegalArgumentException("LF1 is not supported")
      case LanguageMajorVersion.V2 => StablePackagesV2
    }

  /** The IDs of stable packages compatible with the provided version range. */
  def ids(allowedLanguageVersions: VersionRange[LanguageVersion]): Set[Ref.PackageId] = {
    import com.digitalasset.daml.lf.language.LanguageVersionRangeOps.LanguageVersionRange

    import scala.Ordering.Implicits.infixOrderingOps

    StablePackages(allowedLanguageVersions.majorVersion).allPackages.view
      .filter(_.pkg.languageVersion <= allowedLanguageVersions.max)
      .map(_.packageId)
      .toSet
  }
}

/** @param manifestResourcePath the path of a resource that contains a newline-separated list of
  *                             paths to stable package dalf resources.
  */
private[daml] sealed class StablePackagesImpl(
    protected val manifestResourcePath: String
) extends StablePackages {

  override lazy val allPackages: Seq[StablePackage] = allPackagesByName.values.toSeq

  override lazy val ArithmeticError: Ref.TypeConName =
    DA_Exception_ArithmeticError.assertIdentifier("ArithmeticError")
  override lazy val AnyChoice: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyChoice")
  override lazy val AnyContractKey: Ref.TypeConName =
    DA_Internal_Any.assertIdentifier("AnyContractKey")
  override lazy val AnyTemplate: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyTemplate")
  override lazy val TemplateTypeRep: Ref.TypeConName =
    DA_Internal_Any.assertIdentifier("TemplateTypeRep")
  override lazy val NonEmpty: Ref.TypeConName = DA_NonEmpty_Types.assertIdentifier("NonEmpty")
  override lazy val Tuple2: Ref.TypeConName = DA_Types.assertIdentifier("Tuple2")
  override lazy val Tuple3: Ref.TypeConName = DA_Types.assertIdentifier("Tuple3")
  override lazy val Either: Ref.TypeConName = GHC_Tuple.assertIdentifier("Either")

  private lazy val DA_Exception_ArithmeticError = allPackagesByName("DA.Exception.ArithmeticError")
  private lazy val DA_Internal_Any = allPackagesByName("DA.Internal.Any")
  private lazy val DA_NonEmpty_Types = allPackagesByName("DA.NonEmpty.Types")
  private lazy val DA_Types = allPackagesByName("DA.Types")
  private lazy val GHC_Tuple = allPackagesByName("GHC.Tuple")

  /** All stable packages, indexed by module name. */
  private lazy val allPackagesByName: Map[String, StablePackage] =
    scala.io.Source
      .fromResource(manifestResourcePath)
      .getLines()
      .map(decodeDalfResource)
      .map((toStablePackage _).tupled)
      .map(pkg => pkg.moduleName.dottedName -> pkg)
      .toMap

  /** Loads and decodes a dalf embedded as a resource.
    */
  @throws[IllegalArgumentException]("if the resource cannot be found")
  @throws[archive.Error]("if the dalf cannot be decoded")
  private def decodeDalfResource(path: String): (Ref.PackageId, Ast.Package) = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(path)
    require(inputStream != null, s"Resource not found: $path")
    ArchiveDecoder
      .fromInputStream(getClass.getClassLoader.getResourceAsStream(path))
      .fold(throw _, identity)
  }

  /** Converts a decoded package to a [[StablePackage]] */
  private def toStablePackage(pkgId: Ref.PackageId, pkgAst: Ast.Package): StablePackage = {
    assert(pkgAst.modules.size == 1)
    StablePackage(
      moduleName = pkgAst.modules.head._1,
      packageId = pkgId,
      pkg = pkgAst
    )
  }
}
