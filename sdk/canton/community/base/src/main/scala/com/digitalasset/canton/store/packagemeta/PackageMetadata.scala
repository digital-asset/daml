// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.packagemeta

import cats.kernel.Semigroup
import cats.syntax.semigroup.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.store.packagemeta.PackageMetadata.{
  InterfacesImplementedBy,
  PackageResolution,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.util.PackageInfo
import com.digitalasset.daml.lf.language.{Ast, Util as LfUtil}

import scala.annotation.tailrec

// TODO(#17635): Move to [[com.digitalasset.canton.participant.store.memory.PackageMetadataView]]
final case class PackageMetadata(
    interfaces: Set[Ref.Identifier] = Set.empty,
    templates: Set[Ref.Identifier] = Set.empty,
    interfacesImplementedBy: InterfacesImplementedBy = Map.empty,
    packageNameMap: Map[Ref.PackageName, PackageResolution] = Map.empty,
    packageIdVersionMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
    // TODO(#21695): Use [[com.digitalasset.daml.lf.language.PackageInterface]] once public
    packages: Map[Ref.PackageId, Ast.PackageSignature] = Map.empty,
) {

  /** Compute the set of dependencies recursively. Assuming that the package store is closed under
    * dependencies, it throws an exception if a package is unknown.
    *
    * @param packageIds
    *   the set of packages from which to compute the dependencies
    * @return
    *   the set of packages and their dependencies, recursively
    */
  def allDependenciesRecursively(
      packageIds: Set[Ref.PackageId]
  ): Set[Ref.PackageId] = {
    @tailrec
    def go(
        packageIds: Set[Ref.PackageId],
        knownDependencies: Set[Ref.PackageId],
    ): Set[Ref.PackageId] =
      if (packageIds.isEmpty) knownDependencies
      else {
        val newDependencies =
          packageIds.flatMap(pkgId => tryGet(pkgId).directDeps) -- knownDependencies
        go(newDependencies, knownDependencies ++ newDependencies)
      }
    go(packageIds, packageIds)
  }

  private def tryGet(packageId: Ref.PackageId): Ast.PackageSignature =
    packages.getOrElse(
      packageId,
      throw new IllegalStateException(s"Missing package-id $packageId in package metadata view"),
    )

  /** Resolve all template or interface ids for (package-name, qualified-name).
    *
    * As context, package-level upgrading compatibility between two packages pkg1 and pkg2, where
    * pkg2 upgrades pkg1 and they both have the same package-name, ensures that all templates and
    * interfaces defined in pkg1 are present in pkg2. Then, for resolving all the ids for
    * (package-name, qualified-name):
    *
    * * we first create all possible ids by concatenation with the requested qualified-name of the
    * known package-ids for the requested package-name.
    *
    * * Then, since some templates/interfaces can only be defined later (in a package with greater
    * package-version), we filter the previous result by intersection with the set of all known
    * identifiers (both template-ids and interface-ids).
    */
  def resolveTypeConRef(ref: Ref.NameTypeConRef): Set[Ref.FullIdentifier] =
    packageNameMap
      .get(ref.pkg.name)
      .map(_.allPackageIdsForName.iterator)
      .getOrElse(Iterator.empty)
      .map(packageId => Ref.FullIdentifier(packageId, ref.pkg.name, ref.qualifiedName))
      .toSet
      .intersect(allTypeConIds)

  lazy val allTypeConIds: Set[Ref.FullIdentifier] = templates.union(interfaces).map { id =>
    val packageName =
      packageIdVersionMap
        .get(id.packageId)
        .map(_._1)
        .getOrElse(
          throw new IllegalArgumentException(s"Unknown package id: ${id.packageId}")
        )
    Ref.FullIdentifier(id.packageId, packageName, id.qualifiedName)
  }
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
      // TODO(#21695): Consider a size-bounded cache in case of memory pressure issues
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

    implicit def upgradablePackageIdPriorityMapSemigroup: Semigroup[PackageResolution] = {
      val preferenceOrdering = Ordering
        .by[LocalPackagePreference, (Ref.PackageVersion, Ref.PackageId)](pref =>
          // Sort by version then by package-id to ensure deterministic sorting
          pref.version -> pref.packageId
        )
      Semigroup.instance { case (x, y) =>
        PackageResolution(
          preference = preferenceOrdering.max(x.preference, y.preference),
          allPackageIdsForName = x.allPackageIdsForName ++ y.allPackageIdsForName,
        )
      }
    }
  }
}
