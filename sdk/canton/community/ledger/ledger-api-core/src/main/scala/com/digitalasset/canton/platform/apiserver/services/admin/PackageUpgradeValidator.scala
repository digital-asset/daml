// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CacheConfigWithSizeOnly
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.daml.lf.language.Util.{
  PkgIdWithNameAndVersion,
  dependenciesInTopologicalOrder,
}
import com.digitalasset.daml.lf.validation.{TypecheckUpgrades, UpgradeError}

import scala.concurrent.ExecutionContext

class PackageUpgradeValidator(
    cacheConfig: CacheConfigWithSizeOnly,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private case class PackageIdAndSignature(packageId: PackageId, signature: PackageSignature) {
    def version: Ref.PackageVersion = signature.metadata.version
    def name: Ref.PackageName = signature.metadata.name
    def supportsUpgrades: Boolean = signature.supportsUpgrades(packageId)
    def directDeps: Set[PackageId] = signature.directDeps
    def pkgIdWithNameAndVersion: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(
      (packageId, signature)
    )
    override def toString: String = s"$packageId ($name v$version)"
  }

  private val upgradeCompatCache =
    cacheConfig.buildScaffeine().build[(PackageId, PackageId), Either[TopologyManagerError, Unit]]()

  /** Validate the upgrade-compatibility of the vetted lineages that are affected by a new package
    * to vet. That is,
    *   - the lineage of the new package itself
    *   - the lineage of each dependency, direct and transitive, of the new package
    *
    * This validation fails if:
    *   - a dependency is unknown (not in the package store)
    *   - a package claims to be daml-prim or daml-stdlib, but it is not a utility package
    *   - two distinct packages have the same name and version
    *   - a package in the affected lineages is upgrade-incompatible
    *
    * @param newPackagesToVet
    *   new packages to vet
    * @param targetVettedPackages
    *   all packages in the next vetting state, including the new ones
    * @param storedPackageMap
    *   all packages in the package store
    * @param loggingContext
    * @return
    *   a topology manager error if the validation fails, unit otherwise
    */
  def validateUpgrade(
      newPackagesToVet: Set[PackageId],
      targetVettedPackages: Set[PackageId],
      storedPackageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] = {
    // Sort the packages in topological order to get the dependencies first. This is useful to get
    // the upgrade errors in a deterministic way.
    val packagesInTopologicalOrder =
      dependenciesInTopologicalOrder(newPackagesToVet.toList, storedPackageMap)

    // We ignore the dependencies that are not in the store. This is acceptable because
    // later we check that all required dependencies are known.
    val packageNamesToCheckInTopologicalOrder: Seq[Ref.PackageName] =
      packagesInTopologicalOrder.flatMap(storedPackageMap.get).map(_.metadata.name).distinct

    // We don't know yet if the dependencies are upgrade-compatible because of force flags.
    // Therefore we keep them all and check them.
    val packageNamesToCheck = packageNamesToCheckInTopologicalOrder.toSet

    def getPackageToCheck(packageId: PackageId): Option[PackageIdAndSignature] =
      // Here we ignore the package if it is not in the store. This is acceptable because
      // later we check that all required dependencies are known.
      storedPackageMap.get(packageId).collect {
        case packageSig if packageNamesToCheck.contains(packageSig.metadata.name) =>
          PackageIdAndSignature(packageId, packageSig)
      }

    // Get packages to check, group them by name, and sort them by version, to create their lineages
    val lineagesToCheck: Map[Ref.PackageName, List[PackageIdAndSignature]] =
      targetVettedPackages.toList
        .flatMap(getPackageToCheck)
        .groupBy(_.name)
        .view
        .mapValues(_.sortBy(pkg => (pkg.version, pkg.packageId)))
        .toMap

    // validate the upgradeability of each lineage
    packageNamesToCheckInTopologicalOrder
      .filter(
        lineagesToCheck.contains
      ) // some lineage can be missing if unvetted dependencies are allowed
      .traverse(name => validatePackageLineage(name, lineagesToCheck(name), storedPackageMap))
      .map(_ => ())
  }

  private def validatePackageLineage(
      name: Ref.PackageName,
      lineage: List[PackageIdAndSignature],
      storedPackageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] = {
    logger.info(
      s"Typechecking upgrades for lineage of package-name $name."
    )
    val upgradingPairs: List[(PackageIdAndSignature, PackageIdAndSignature)] =
      lineage
        .filter(_.supportsUpgrades)
        .sliding(2)
        .collect { case fst :: snd :: Nil => (fst, snd) }
        .toList
    for {
      _ <- lineage.traverse(validateDependencies(_, storedPackageMap))
      _ <- lineage.traverse(validateDamlPrimOrStdLib)
      _ <- upgradingPairs.traverse { case (fst, snd) => validateVersion(fst, snd) }
      _ <- upgradingPairs.traverse { case (fst, snd) =>
        cachedTypecheckUpgrades(fst, snd, storedPackageMap)
      }
      _ = logger.info(s"Typechecking upgrades for lineage of package-name $name succeeded.")
    } yield ()
  }

  private def validateDependencies(
      pkg: PackageIdAndSignature,
      storedPackageMap: Map[PackageId, PackageSignature],
  )(implicit loggingContext: LoggingContextWithTrace): Either[TopologyManagerError, Unit] =
    pkg.directDeps.toSeq
      .traverse { packageId =>
        // we cannot check the upgradability of a package if one of its dependency is unknown
        storedPackageMap
          .get(packageId)
          .toRight(CannotVetDueToMissingPackages.Missing(Set(packageId)))
      }
      .map(_ => ())

  private def validateDamlPrimOrStdLib(
      pkg: PackageIdAndSignature
  )(implicit loggingContext: LoggingContextWithTrace): Either[TopologyManagerError, Unit] =
    Either.cond(
      !pkg.signature.isInvalidDamlPrimOrStdlib(pkg.packageId),
      (),
      UpgradeDamlPrimIsNotAUtilityPackage.Error(pkg.pkgIdWithNameAndVersion),
    )

  private def validateVersion(
      fst: PackageIdAndSignature,
      snd: PackageIdAndSignature,
  )(implicit loggingContext: LoggingContextWithTrace): Either[TopologyManagerError, Unit] =
    Either.cond(
      fst.version != snd.version,
      (),
      UpgradeVersion.Error(fst.pkgIdWithNameAndVersion, snd.pkgIdWithNameAndVersion),
    )

  private def cachedTypecheckUpgrades(
      oldPackage: PackageIdAndSignature,
      newPackage: PackageIdAndSignature,
      storedPackageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] =
    upgradeCompatCache.get(
      (oldPackage.packageId, newPackage.packageId),
      _ => strictTypecheckUpgrades(oldPackage, newPackage, storedPackageMap),
    )

  private def strictTypecheckUpgrades(
      oldPackage: PackageIdAndSignature,
      newPackage: PackageIdAndSignature,
      storedPackageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] = {
    logger.info(s"Package $newPackage claims to upgrade package $oldPackage")
    TypecheckUpgrades
      .typecheckUpgrades(
        storedPackageMap,
        (newPackage.packageId, newPackage.signature),
        oldPackage.packageId,
        Some(oldPackage.signature),
      )
      .toEither
      .left
      .map {
        case err: UpgradeError =>
          Upgradeability.Error(
            newPackage = newPackage.pkgIdWithNameAndVersion,
            oldPackage = oldPackage.pkgIdWithNameAndVersion,
            upgradeError = err.prettyInternal,
          )
        case unhandledErr =>
          TopologyManagerError.InternalError.Unhandled(
            s"Typechecking upgrades from $oldPackage to $newPackage failed",
            unhandledErr,
          )
      }
  }
}
