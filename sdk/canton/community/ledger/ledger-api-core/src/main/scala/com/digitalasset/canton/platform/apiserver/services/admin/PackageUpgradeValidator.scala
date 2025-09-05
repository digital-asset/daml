// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import com.daml.logging.entries.LoggingValue.OfString
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.daml.lf.language.Util.{
  PkgIdWithNameAndVersion,
  dependenciesInTopologicalOrder,
}
import com.digitalasset.daml.lf.validation.{TypecheckUpgrades, UpgradeError}

import scala.concurrent.ExecutionContext

class PackageUpgradeValidator(val loggerFactory: NamedLoggerFactory)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  def validateUpgrade(
      upgradingPackages: List[(PackageId, PackageSignature)],
      upgradablePackages: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val upgradingPackagesMap = upgradingPackages.toMap
    // We sort the upgrading packages in topological order in order to get a deterministic order
    // of the upgrading errors, on the dependencies first. We then need to filter out the
    // dependencies that are already vetted, as they don't need to be upgrade-checked anymore.
    val packagesInTopologicalOrder =
      dependenciesInTopologicalOrder(upgradingPackages.map(_._1), upgradingPackagesMap)
        .filter(upgradingPackagesMap.contains)
    def go(
        packageMap: Map[PackageId, PackageSignature],
        deps: List[PackageId],
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = deps match {
      case Nil => EitherT.pure[FutureUnlessShutdown, TopologyManagerError](())
      case pkgId :: rest =>
        val pkg = upgradingPackagesMap(pkgId)
        val supportsUpgrades = pkg.supportsUpgrades(pkgId)
        for {
          _ <- EitherTUtil.ifThenET(supportsUpgrades)(
            // This check will look for the closest neighbors of pkgId for the package versioning ordering and
            // will load them from the DB and decode them. If one were to vet many packages that upgrade each
            // other, we will end up decoding the same package many times. Some of these cases could be sped up
            // by a cache depending on the order in which the packages are vetted.
            validatePackageUpgrade((pkgId, pkg), packageMap)
          )
          res <- go(packageMap + (pkgId -> pkg), rest)
        } yield ()
    }
    go(upgradablePackages, packagesInTopologicalOrder).map(_ => ())
  }

  private def validatePackageUpgrade(
      toVetPackage: (PackageId, PackageSignature),
      packageMap: Map[PackageId, PackageSignature],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val (toVetPackageId, toVetPackageAst) = toVetPackage
    val optUpgradingDar = Some(toVetPackage)
    val toVetPackageIdWithMeta = PkgIdWithNameAndVersion(toVetPackage)
    logger.info(
      s"Vetting package $toVetPackageIdWithMeta in submission ID ${loggingContext.serializeFiltered("submissionId")}."
    )
    if (toVetPackageAst.isInvalidDamlPrimOrStdlib(toVetPackageId)) {
      EitherT.leftT[FutureUnlessShutdown, Unit](
        TopologyManagerError.ParticipantTopologyManagerError.UpgradeDamlPrimIsNotAUtilityPackage
          .Error(packageToBeVetted = toVetPackageIdWithMeta): TopologyManagerError
      )
    } else {
      existingVersionedPackageId(toVetPackageAst, packageMap) match {
        case Some(existingPackageId) =>
          if (existingPackageId == toVetPackageId) {
            logger.info(
              s"Bypassing upgrade check of package $toVetPackageIdWithMeta as it has been previously vetted"
            )
            EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())
          } else {
            EitherT.leftT[FutureUnlessShutdown, Unit](
              TopologyManagerError.ParticipantTopologyManagerError.UpgradeVersion
                .Error(
                  newPackage = toVetPackageIdWithMeta,
                  existingPackage = existingPackageId,
                  packageVersion = toVetPackageAst.metadata.version,
                ): TopologyManagerError
            )
          }

        case None =>
          val optMaximalDar = maximalVersionedDar(toVetPackageAst, packageMap)
          val optMinimalDar = minimalVersionedDar(toVetPackageAst, packageMap)
          for {
            _ <- typecheckUpgrades(
              TypecheckUpgrades.MaximalDarCheck,
              packageMap,
              optUpgradingDar,
              optMaximalDar,
            )
            _ <- typecheckUpgrades(
              TypecheckUpgrades.MinimalDarCheck,
              packageMap,
              optMinimalDar,
              optUpgradingDar,
            )
            _ = logger.info(s"Typechecking upgrades for $toVetPackageIdWithMeta succeeded.")
          } yield ()
      }
    }
  }

  private def existingVersionedPackageId(
      toVetPkg: PackageSignature,
      packageMap: Map[PackageId, PackageSignature],
  ): Option[PackageId] =
    packageMap.collectFirst {
      case (pkgId, pkg)
          if pkg.metadata.name == toVetPkg.metadata.name && pkg.metadata.version == toVetPkg.metadata.version =>
        pkgId
    }

  private def minimalVersionedDar(
      toVetPkg: PackageSignature,
      packageMap: Map[PackageId, PackageSignature],
  ): Option[(PackageId, PackageSignature)] =
    packageMap
      .collect {
        case (pkgId, pkg)
            if pkg.metadata.name == toVetPkg.metadata.name && pkg.metadata.version > toVetPkg.metadata.version =>
          (pkgId, pkg)
      }
      .minByOption { case (_, pkg) => pkg.metadata.version }

  private def maximalVersionedDar(
      toVetPkg: PackageSignature,
      packageMap: Map[PackageId, PackageSignature],
  ): Option[(PackageId, PackageSignature)] =
    packageMap
      .collect {
        case (pkgId, pkg)
            if pkg.metadata.name == toVetPkg.metadata.name && pkg.metadata.version < toVetPkg.metadata.version =>
          (pkgId, pkg)
      }
      .maxByOption { case (_, pkg) => pkg.metadata.version }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck,
      packageMap: Map[PackageId, PackageSignature],
      newDar1: (PackageId, PackageSignature),
      oldDar2: (PackageId, PackageSignature),
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    LoggingContextWithTrace
      .withEnrichedLoggingContext("upgradeTypecheckPhase" -> OfString(phase.toString)) {
        implicit loggingContext =>
          val (newPkgId1, newPkg1) = newDar1
          val newPkgId1WithMeta: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(newDar1)
          val (oldPkgId2, oldPkg2) = oldDar2
          val oldPkgId2WithMeta: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(oldDar2)
          logger.info(s"Package $newPkgId1WithMeta claims to upgrade package id $oldPkgId2WithMeta")
          EitherT(
            FutureUnlessShutdown.pure(
              TypecheckUpgrades
                .typecheckUpgrades(packageMap, (newPkgId1, newPkg1), oldPkgId2, Some(oldPkg2))
                .toEither
            )
          ).leftMap[TopologyManagerError] {
            case err: UpgradeError =>
              TopologyManagerError.ParticipantTopologyManagerError.Upgradeability.Error(
                newPackage = newPkgId1WithMeta,
                oldPackage = oldPkgId2WithMeta,
                upgradeError = err.prettyInternal,
                isUpgradeCheck = phase == TypecheckUpgrades.MaximalDarCheck,
              )
            case unhandledErr =>
              TopologyManagerError.InternalError.Unhandled(
                s"Typechecking upgrades for $oldPkgId2WithMeta failed",
                unhandledErr,
              )
          }
      }

  private def typecheckUpgrades(
      typecheckPhase: TypecheckUpgrades.UploadPhaseCheck,
      packageMap: Map[PackageId, PackageSignature],
      optNewDar1: Option[(PackageId, PackageSignature)],
      optOldDar2: Option[(PackageId, PackageSignature)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    (optNewDar1, optOldDar2) match {
      case (None, _) | (_, None) =>
        EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())

      case (Some((newPkgId1, newPkg1)), Some((oldPkgId2, oldPkg2))) =>
        strictTypecheckUpgrades(
          typecheckPhase,
          packageMap,
          (newPkgId1, newPkg1),
          (oldPkgId2, oldPkg2),
        )
    }
}
