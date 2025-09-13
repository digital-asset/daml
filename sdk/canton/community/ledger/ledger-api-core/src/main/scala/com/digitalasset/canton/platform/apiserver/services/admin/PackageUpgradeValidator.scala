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
    def pkgIdWithNameAndVersion: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(
      (packageId, signature)
    )
    override def toString: String = s"$packageId ($name v$version)"
  }

  private val upgradeCompatCache =
    cacheConfig.buildScaffeine().build[(PackageId, PackageId), Either[TopologyManagerError, Unit]]()

  def validateUpgrade(allPackages: List[(PackageId, PackageSignature)])(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] = {
    val packageMap = allPackages.toMap
    // Group packages by name and sort by version, to create the lineage of each package name
    val packageNameToLineage: Map[Ref.PackageName, List[PackageIdAndSignature]] = allPackages
      .map { case (id, sig) => PackageIdAndSignature(id, sig) }
      .groupBy(_.name)
      .view
      .mapValues(_.sortBy(_.version))
      .toMap

    // Sort the packages in topological order to get the dependencies first. This is useful to get
    // the upgrade errors in a deterministic way. We then filter on the packageMap to only keep the
    // packages to vet and not their dependencies which are already vetted.
    val packageNamesInTopologicalOrder: Seq[Ref.PackageName] =
      dependenciesInTopologicalOrder(allPackages.map(_._1), packageMap)
        .flatMap(packageMap.get)
        .map(_.metadata.name)
        .distinct

    // validate the upgradeability of each lineage
    packageNamesInTopologicalOrder
      .traverse(name => validatePackageLineage(name, packageNameToLineage(name), packageMap))
      .map(_ => ())
  }

  private def validatePackageLineage(
      name: Ref.PackageName,
      lineage: List[PackageIdAndSignature],
      packageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] = {
    logger.info(
      s"Typechecking upgrades for lineage of package-name $name."
    )
    val upgradingPairs: List[(PackageIdAndSignature, PackageIdAndSignature)] =
      lineage.sliding(2).collect { case fst :: snd :: Nil => (fst, snd) }.toList
    for {
      _ <- lineage.traverse(validateDamlPrimOrStdLib)
      _ <- upgradingPairs.traverse { case (fst, snd) => validateVersion(fst, snd) }
      _ <- upgradingPairs.traverse { case (fst, snd) =>
        cachedTypecheckUpgrades(fst, snd, packageMap)
      }
      _ = logger.info(s"Typechecking upgrades for lineage of package-name $name succeeded.")
    } yield ()
  }

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
      packageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] =
    upgradeCompatCache.get(
      (oldPackage.packageId, newPackage.packageId),
      _ => strictTypecheckUpgrades(oldPackage, newPackage, packageMap),
    )

  private def strictTypecheckUpgrades(
      oldPackage: PackageIdAndSignature,
      newPackage: PackageIdAndSignature,
      packageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Either[TopologyManagerError, Unit] = {
    logger.info(s"Package $newPackage claims to upgrade package $oldPackage")
    TypecheckUpgrades
      .typecheckUpgrades(
        packageMap,
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
