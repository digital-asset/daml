// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.kernel.Semigroup
import cats.syntax.semigroup.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  InterfacesImplementedBy,
  PackageResolution,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.util.PackageInfo
import com.digitalasset.daml.lf.language.{Ast, Util as LfUtil}

// TODO(#17635): Move to [[com.digitalasset.canton.participant.store.memory.PackageMetadataView]]
final case class PackageMetadata(
    interfaces: Set[Ref.Identifier] = Set.empty,
    templates: Set[Ref.Identifier] = Set.empty,
    interfacesImplementedBy: InterfacesImplementedBy = Map.empty,
    packageNameMap: Map[Ref.PackageName, PackageResolution] = Map.empty,
    packageIdVersionMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
    // TODO(#19671): Use [[com.digitalasset.daml.lf.language.PackageInterface]] once public
    packages: Map[Ref.PackageId, Ast.PackageSignature] = Map.empty,
) {

  /** Resolve all template or interface ids for (package-name, qualified-name).
    *
    * As context, package-level upgrading compatibility between two packages pkg1 and pkg2,
    * where pkg2 upgrades pkg1 and they both have the same package-name,
    * ensures that all templates and interfaces defined in pkg1 are present in pkg2.
    * Then, for resolving all the ids for (package-name, qualified-name):
    *
    * * we first create all possible ids by concatenation with the requested qualified-name
    *   of the known package-ids for the requested package-name.
    *
    * * Then, since some templates/interfaces can only be defined later (in a package with greater
    *   package-version), we filter the previous result by intersection with the set of all known
    *   identifiers (both template-ids and interface-ids).
    */
  def resolveTypeConRef(ref: Ref.TypeConRef): Set[Ref.Identifier] = ref match {
    case Ref.TypeConRef(Ref.PackageRef.Name(packageName), qualifiedName) =>
      packageNameMap
        .get(packageName)
        .map(_.allPackageIdsForName.iterator)
        .getOrElse(Iterator.empty)
        .map(packageId => Ref.Identifier(packageId, qualifiedName))
        .toSet
        .intersect(allTypeConIds)
    case Ref.TypeConRef(Ref.PackageRef.Id(packageId), qName) =>
      Set(Ref.Identifier(packageId, qName))
  }

  lazy val allTypeConIds: Set[Ref.Identifier] = templates.union(interfaces)
}

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

  def from(
      packageId: Ref.PackageId,
      packageAst: Ast.Package,
  ): PackageMetadata = {
    val packageMetadata = packageAst.metadata
    val packageName = packageMetadata.name
    val packageVersion = packageMetadata.version
    val packageNameMap = Map(
      packageName -> PackageResolution(
        preference = LocalPackagePreference(packageVersion, packageId),
        allPackageIdsForName = NonEmpty(Set, packageId),
      )
    )

    val packageInfo = new PackageInfo(Map(packageId -> packageAst))
    PackageMetadata(
      packageNameMap = packageNameMap,
      interfaces = packageInfo.definedInterfaces,
      templates = packageInfo.definedTemplates,
      interfacesImplementedBy = packageInfo.interfaceInstances,
      packageIdVersionMap = Map(packageId -> (packageName, packageVersion)),
      // TODO(#19671): Consider a size-bounded cache in case of memory pressure issues
      //               Consider unifying with the other package caches in the participant
      //               (e.g. [[com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader]])
      packages = Map(packageId -> LfUtil.toSignature(packageAst)),
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
          packages = x.packages ++ y.packages,
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
