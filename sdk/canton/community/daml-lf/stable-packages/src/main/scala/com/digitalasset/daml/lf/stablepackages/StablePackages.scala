// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package stablepackages

import com.digitalasset.daml.lf.VersionRange
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.archive.ArchiveDecoder
import com.digitalasset.daml.lf.data.{Bytes, Ref}

import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, StablePackage, StablePackages}

final object StablePackagesV2
    extends StablePackagesImpl("compiler/damlc/stable-packages/stable-packages-manifest-v2.txt")

private[daml] object StablePackages {
  val stablePackages: StablePackages = StablePackagesV2

  /** The IDs of stable packages compatible with the provided version range. */
  def ids(allowedLanguageVersions: VersionRange[LanguageVersion]): Set[Ref.PackageId] = {

    StablePackages.stablePackages.allPackages.view
      .filter(p => allowedLanguageVersions.contains(p.pkg.languageVersion))
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

  override lazy val ArithmeticError: Ref.TypeConId =
    DA_Exception_ArithmeticError.assertIdentifier("ArithmeticError")
  override lazy val AnyChoice: Ref.TypeConId = DA_Internal_Any.assertIdentifier("AnyChoice")
  override lazy val AnyContractKey: Ref.TypeConId =
    DA_Internal_Any.assertIdentifier("AnyContractKey")
  override lazy val AnyTemplate: Ref.TypeConId = DA_Internal_Any.assertIdentifier("AnyTemplate")
  override lazy val TemplateTypeRep: Ref.TypeConId =
    DA_Internal_Any.assertIdentifier("TemplateTypeRep")
  override lazy val NonEmpty: Ref.TypeConId = DA_NonEmpty_Types.assertIdentifier("NonEmpty")
  override lazy val Tuple2: Ref.TypeConId = DA_Types.assertIdentifier("Tuple2")
  override lazy val Tuple3: Ref.TypeConId = DA_Types.assertIdentifier("Tuple3")
  override lazy val Either: Ref.TypeConId = DA_Types.assertIdentifier("Either")
  override lazy val FailureStatus: Ref.TypeConId =
    DA_Internal_Fail_Types.assertIdentifier("FailureStatus")

  private lazy val DA_Exception_ArithmeticError = allPackagesByName("DA.Exception.ArithmeticError")
  private lazy val DA_Internal_Any = allPackagesByName("DA.Internal.Any")
  private lazy val DA_NonEmpty_Types = allPackagesByName("DA.NonEmpty.Types")
  private lazy val DA_Types = allPackagesByName("DA.Types")
  private lazy val DA_Internal_Fail_Types = allPackagesByName("DA.Internal.Fail.Types")

  /** All stable packages, indexed by module name. */
  private lazy val allPackagesByName: Map[String, StablePackage] =
    scala.io.Source
      .fromResource(manifestResourcePath)
      .getLines()
      .map { path =>
        val (pkgId, pkg, bytes) = decodeDalfResource(path)
        val stablePkg = toStablePackage(pkgId, pkg, bytes)
        stablePkg.moduleName.dottedName -> stablePkg
      }
      .toMap

  def values = allPackagesByName.values

  /** Loads and decodes a dalf embedded as a resource.
    */
  @throws[IllegalArgumentException]("if the resource cannot be found")
  @throws[archive.Error]("if the dalf cannot be decoded")
  private def decodeDalfResource(path: String): (Ref.PackageId, Ast.Package, data.Bytes) = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(path)
    require(inputStream != null, s"Resource not found: $path")
    val bytes = data.Bytes.fromInputStream(inputStream)
    val (pkgId, pkg) = ArchiveDecoder.assertFromBytes(bytes)
    (pkgId, pkg, bytes)
  }

  /** Converts a decoded package to a [[StablePackage]] */
  private def toStablePackage(
      pkgId: Ref.PackageId,
      pkgAst: Ast.Package,
      bytes: Bytes,
  ): StablePackage = {
    assert(pkgAst.modules.size == 1)
    StablePackage(
      moduleName = pkgAst.modules.head._1,
      packageId = pkgId,
      pkg = pkgAst,
      bytes = bytes,
    )
  }
}
