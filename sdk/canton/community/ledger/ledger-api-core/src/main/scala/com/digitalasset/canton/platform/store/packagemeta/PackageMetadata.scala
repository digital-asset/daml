// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.kernel.Semigroup
import cats.syntax.semigroup.*
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  InterfacesImplementedBy,
  PackageResolution,
}

final case class PackageMetadata(
    interfaces: Set[Ref.Identifier] = Set.empty,
    templates: Set[Ref.Identifier] = Set.empty,
    interfacesImplementedBy: InterfacesImplementedBy = Map.empty,
    packageNameMap: Map[Ref.PackageName, PackageResolution] = Map.empty,
    packageIdVersionMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
)

object PackageMetadata {
  type InterfacesImplementedBy = Map[Ref.Identifier, Set[Ref.Identifier]]

  final case class LocalPackagePreference(
      version: Ref.PackageVersion,
      packageId: Ref.PackageId,
  )

  final case class PackageResolution(
      preference: LocalPackagePreference,
      allPackageIdsForName: NonEmpty[Set[Ref.PackageId]],
  )

  def from(archive: DamlLf.Archive): PackageMetadata = {
    val packageInfo = Decode.assertDecodeInfoPackage(archive)

    val packageName = packageInfo.mainPackageMetadata.name
    val packageVersion = packageInfo.mainPackageMetadata.version
    val packageId = packageInfo.mainPackageId
    val packageNameMap = Map(
      packageName -> PackageResolution(
        preference = LocalPackagePreference(packageVersion, packageId),
        allPackageIdsForName = NonEmpty(Set, packageId),
      )
    )

    PackageMetadata(
      packageNameMap = packageNameMap,
      interfaces = packageInfo.definedInterfaces,
      templates = packageInfo.definedTemplates,
      interfacesImplementedBy = packageInfo.interfaceInstances,
      packageIdVersionMap = Map(packageId -> (packageName, packageVersion)),
    )
  }

  object Implicits {
    // Although the combine for the semigroups below is safely commutative,
    // we care about the order of the operands for performance considerations.
    // Specifically, we must keep the complexity linear in the size of the appended package
    // and NOT in the size of the current PackageMetadata state.
    implicit def packageMetadataSemigroup: Semigroup[PackageMetadata] =
      Semigroup.instance { case (x, y) =>
        PackageMetadata(
          packageNameMap = x.packageNameMap |+| y.packageNameMap,
          interfaces = x.interfaces |+| y.interfaces,
          templates = x.templates |+| y.templates,
          interfacesImplementedBy = x.interfacesImplementedBy |+| y.interfacesImplementedBy,
          packageIdVersionMap = y.packageIdVersionMap
            .foldLeft(x.packageIdVersionMap) { case (acc, (k, v)) =>
              acc.updatedWith(k) {
                case None => Some(v)
                case Some(existing) if existing == v => Some(v)
                case Some(existing) =>
                  throw new IllegalStateException(
                    s"Conflicting versioned package names for the same package id $k. Previous ($existing) vs uploaded($v)"
                  )
              }
            },
        )
      }

    implicit def upgradablePackageIdPriorityMapSemigroup: Semigroup[PackageResolution] =
      Semigroup.instance { case (x, y) =>
        PackageResolution(
          preference =
            if (y.preference.version > x.preference.version) y.preference else x.preference,
          allPackageIdsForName = x.allPackageIdsForName ++ y.allPackageIdsForName,
        )
      }
  }
}
