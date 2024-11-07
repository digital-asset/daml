// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import com.daml.error.DamlError
import com.daml.logging.entries.LoggingValue.OfString
import com.digitalasset.canton.ledger.error.PackageServiceErrors.{InternalError, Validation}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPackageManagementService.ErrorValidations
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator.PackageMap
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Util.{
  PkgIdWithNameAndVersion,
  dependenciesInTopologicalOrder,
}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.validation.{TypecheckUpgrades, UpgradeError}
import scalaz.std.either.*
import scalaz.std.option.*
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse.*

import scala.concurrent.{ExecutionContext, Future}

object PackageUpgradeValidator {
  type PackageMap = Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]
}

// TODO(i16362): Should have unit tests on canton-side for this code as per discussion in https://github.com/DACH-NY/canton/pull/21040#discussion_r1734646573
// https://github.com/DACH-NY/canton/issues/16362
class PackageUpgradeValidator(
    getPackageMap: LoggingContextWithTrace => PackageMap,
    getLfArchive: LoggingContextWithTrace => Ref.PackageId => Future[Option[Archive]],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  def validateUpgrade(
      upgradingPackages: List[(Ref.PackageId, Ast.Package)]
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    val upgradingPackagesMap = upgradingPackages.toMap
    val packagesInTopologicalOrder =
      dependenciesInTopologicalOrder(upgradingPackages.map(_._1), upgradingPackagesMap)
    val packageMap = getPackageMap(loggingContext)

    def go(
        packageMap: PackageMap,
        deps: List[Ref.PackageId],
    ): EitherT[Future, DamlError, PackageMap] = deps match {
      case Nil => EitherT.pure[Future, DamlError](packageMap)
      case pkgId :: rest =>
        val pkg = upgradingPackagesMap(pkgId)
        val supportsUpgrades = pkg.supportsUpgrades(pkgId)
        for {
          _ <- EitherTUtil.ifThenET(supportsUpgrades)(
            // This check will look for the closest neighbors of pkgId for the package versioning ordering and
            // will load them from the DB and decode them. If one were to upload many packages that upgrade each
            // other, we will end up decoding the same package many times. Some of these cases could be sped up
            // by a cache depending on the order in which the packages are uploaded.
            validatePackageUpgrade((pkgId, pkg), packageMap, upgradingPackagesMap)
          )
          res <- go(packageMap + ((pkgId, (pkg.metadata.name, pkg.metadata.version))), rest)
        } yield res
    }
    go(packageMap, packagesInTopologicalOrder).map(_ => ())
  }

  private def validatePackageUpgrade(
      uploadedPackage: (Ref.PackageId, Ast.Package),
      packageMap: PackageMap,
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    val (uploadedPackageId, uploadedPackageAst) = uploadedPackage
    val optUpgradingDar = Some(uploadedPackage)
    val uploadedPackageIdWithMeta: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(
      uploadedPackage
    )
    logger.info(
      s"Uploading DAR file for $uploadedPackageIdWithMeta in submission ID ${loggingContext.serializeFiltered("submissionId")}."
    )
    existingVersionedPackageId(uploadedPackageAst, packageMap) match {
      case Some(existingPackageId) =>
        if (existingPackageId == uploadedPackageId) {
          logger.info(
            s"Ignoring upload of package $uploadedPackageIdWithMeta as it has been previously uploaded"
          )
          EitherT.rightT[Future, DamlError](())
        } else {
          EitherT.leftT[Future, Unit](
            Validation.UpgradeVersion
              .Error(
                uploadedPackage = uploadedPackageIdWithMeta,
                existingPackage = existingPackageId,
                packageVersion = uploadedPackageAst.metadata.version,
              ): DamlError
          )
        }

      case None =>
        for {
          optMaximalDar <- EitherT.right[DamlError](
            maximalVersionedDar(
              uploadedPackageAst,
              packageMap,
              upgradingPackagesMap,
            )
          )
          _ <- typecheckUpgrades(
            TypecheckUpgrades.MaximalDarCheck,
            packageMap,
            optUpgradingDar,
            optMaximalDar,
          )
          optMinimalDar <- EitherT.right[DamlError](
            minimalVersionedDar(uploadedPackageAst, packageMap, upgradingPackagesMap)
          )
          _ <- typecheckUpgrades(
            TypecheckUpgrades.MinimalDarCheck,
            packageMap,
            optMinimalDar,
            optUpgradingDar,
          )
          _ = logger.info(s"Typechecking upgrades for $uploadedPackageIdWithMeta succeeded.")
        } yield ()
    }
  }

  /** Looks up [[pkgId]], first in the [[upgradingPackagesMap]] and then in the package store.
    */
  private def lookupDar(
      pkgId: Ref.PackageId,
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] =
    upgradingPackagesMap.get(pkgId) match {
      case Some(pkg) => Future.successful(Some((pkgId, pkg)))
      case None =>
        for {
          optArchive <- getLfArchive(loggingContextWithTrace)(pkgId)
          optPackage <- Future.fromTry {
            optArchive
              .traverse(Decode.decodeArchive(_))
              .handleError(Validation.handleLfArchiveError)
          }
        } yield optPackage
    }

  private def existingVersionedPackageId(
      pkg: Ast.Package,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  ): Option[Ref.PackageId] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap.collectFirst { case (pkgId, (`pkgName`, `pkgVersion`)) => pkgId }
  }

  private def minimalVersionedDar(
      pkg: Ast.Package,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = pkg.metadata.name
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkg.metadata.version < version }
      .minByOption { case (_, version) => version }
      .traverse { case (pId, _) =>
        lookupDar(pId, upgradingPackagesMap).map(
          _.getOrElse {
            val errorMessage = s"failed to look up dar of package $pId present in package map"
            logger.error(errorMessage)
            throw new IllegalStateException(errorMessage)
          }
        )
      }
  }

  private def maximalVersionedDar(
      pkg: Ast.Package,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = pkg.metadata.name
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkg.metadata.version > version }
      .maxByOption { case (_, version) => version }
      .traverse { case (pId, _) =>
        lookupDar(pId, upgradingPackagesMap)(loggingContext).map(
          _.getOrElse {
            val errorMessage = s"failed to look up dar of package $pId present in package map"
            logger.error(errorMessage)
            throw new IllegalStateException(errorMessage)
          }
        )
      }
  }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck,
      packageMap: PackageMap,
      newDar1: (Ref.PackageId, Ast.Package),
      oldDar2: (Ref.PackageId, Ast.Package),
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] =
    LoggingContextWithTrace
      .withEnrichedLoggingContext("upgradeTypecheckPhase" -> OfString(phase.toString)) {
        implicit loggingContext =>
          val (newPkgId1, newPkg1) = newDar1
          val newPkgId1WithMeta: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(newDar1)
          val (oldPkgId2, oldPkg2) = oldDar2
          val oldPkgId2WithMeta: PkgIdWithNameAndVersion = PkgIdWithNameAndVersion(oldDar2)
          logger.info(s"Package $newPkgId1WithMeta claims to upgrade package id $oldPkgId2WithMeta")
          EitherT(
            Future(
              TypecheckUpgrades
                .typecheckUpgrades(packageMap, (newPkgId1, newPkg1), oldPkgId2, Some(oldPkg2))
                .toEither
            )
          ).leftMap[DamlError] {
            case err: UpgradeError =>
              Validation.Upgradeability.Error(
                newPackage = newPkgId1WithMeta,
                oldPackage = oldPkgId2WithMeta,
                upgradeError = err,
                phase = phase,
              )
            case unhandledErr =>
              InternalError.Unhandled(
                unhandledErr,
                Some(s"Typechecking upgrades for $oldPkgId2WithMeta failed with unknown error."),
              )
          }
      }

  private def typecheckUpgrades(
      typecheckPhase: TypecheckUpgrades.UploadPhaseCheck,
      packageMap: PackageMap,
      optNewDar1: Option[(Ref.PackageId, Ast.Package)],
      optOldDar2: Option[(Ref.PackageId, Ast.Package)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] =
    (optNewDar1, optOldDar2) match {
      case (None, _) | (_, None) =>
        EitherT.rightT[Future, DamlError](())

      case (Some((newPkgId1, newPkg1)), Some((oldPkgId2, oldPkg2))) =>
        strictTypecheckUpgrades(
          typecheckPhase,
          packageMap,
          (newPkgId1, newPkg1),
          (oldPkgId2, oldPkg2),
        )
    }
}
