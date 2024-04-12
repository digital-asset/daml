// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package stablepackages

import com.daml.lf.archive.ArchiveDecoder
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.PackageSignature
import com.daml.lf.language.{Ast, LanguageVersion, LanguageMajorVersion, Util => AstUtil}

import scala.collection.MapView

private[daml] sealed case class StablePackage(
    moduleName: Ref.ModuleName,
    packageId: Ref.PackageId,
    name: Ref.PackageName,
    languageVersion: LanguageVersion,
) {
  def identifier(idName: Ref.DottedName): Ref.Identifier =
    Ref.Identifier(packageId, Ref.QualifiedName(moduleName, idName))

  @throws[IllegalArgumentException]
  def assertIdentifier(idName: String): Ref.Identifier =
    identifier(Ref.DottedName.assertFromString(idName))

}

private[daml] sealed abstract class StablePackages {

  import Ordering.Implicits._

  val allPackages: Map[Ref.PackageId, Ast.Package]

  def packages(maxVersion: LanguageVersion): MapView[Ref.PackageId, Ast.Package] =
    allPackages.view.filter { case (_, pkg) => pkg.languageVersion <= maxVersion }

  final lazy val allPackageSignatures: Map[PackageId, PackageSignature] =
    AstUtil.toSignatures(allPackages)

  def packageSignatures(maxVersion: LanguageVersion): MapView[Ref.PackageId, Ast.PackageSignature] =
    allPackageSignatures.view.filter { case (_, pkg) => pkg.languageVersion <= maxVersion }

  val stablePackagesByName: Map[String, StablePackage]

  val ArithmeticError: Ref.TypeConName
  val AnyChoice: Ref.TypeConName
  val AnyContractKey: Ref.TypeConName
  val AnyTemplate: Ref.TypeConName
  val TemplateTypeRep: Ref.TypeConName
  val NonEmpty: Ref.TypeConName
  val Tuple2: Ref.TypeConName
  val Tuple3: Ref.TypeConName
  val Either: Ref.TypeConName
}

private[daml] final object StablePackagesV2
    extends StablePackagesImpl("compiler/damlc/stable-packages/stable-packages-manifest-v2.txt")

private[daml] object StablePackages {
  def apply(languageMajorVersion: LanguageMajorVersion): StablePackages =
    languageMajorVersion match {
      case LanguageMajorVersion.V1 => throw new IllegalArgumentException("LF1 is not supported")
      case LanguageMajorVersion.V2 => StablePackagesV2
    }
}

/** @param manifestResourcePath the path of a resource that contains a newline-separated list of
  *                             paths to stable package dalf resources.
  */
private[daml] sealed class StablePackagesImpl(
    protected val manifestResourcePath: String
) extends StablePackages {

  override lazy val allPackages: Map[Ref.PackageId, Ast.Package] =
    scala.io.Source
      .fromResource(manifestResourcePath)
      .getLines()
      .map(decodeDalfResource)
      .toMap

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

  private lazy val DA_Exception_ArithmeticError = stablePackagesByName(
    "DA.Exception.ArithmeticError"
  )
  private lazy val DA_Internal_Any = stablePackagesByName("DA.Internal.Any")
  private lazy val DA_NonEmpty_Types = stablePackagesByName("DA.NonEmpty.Types")
  private lazy val DA_Types = stablePackagesByName("DA.Types")
  private lazy val GHC_Tuple = stablePackagesByName("GHC.Tuple")

  /** All stable packages, indexed by module name. */
  override lazy val stablePackagesByName: Map[String, StablePackage] =
    allPackages
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
      name = pkgAst.name,
      languageVersion = pkgAst.languageVersion,
    )
  }
}
