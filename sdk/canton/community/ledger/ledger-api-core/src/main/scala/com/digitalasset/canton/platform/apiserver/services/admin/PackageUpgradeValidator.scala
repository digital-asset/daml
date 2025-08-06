// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import cats.implicits.toTraverseOps
import com.daml.logging.entries.LoggingValue.OfString
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ledger.error.PackageServiceErrors.{InternalError, Validation}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPackageManagementService.ErrorValidations
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator.PackageMap
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Util.{
  PkgIdWithNameAndVersion,
  dependenciesInTopologicalOrder,
}
import com.digitalasset.daml.lf.validation.{TypecheckUpgrades, UpgradeError}

import scala.concurrent.ExecutionContext

object PackageUpgradeValidator {
  type PackageMap = Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]
}

class PackageUpgradeValidator(val loggerFactory: NamedLoggerFactory)(implicit executionContext: ExecutionContext) extends NamedLogging {

  def validateUpgrade(
      upgradingPackages: List[(Ref.PackageId, Ast.PackageSignature)],
      packageMetadataSnapshot: PackageMetadata,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] = {
    val upgradingPackagesMap = upgradingPackages.toMap
    val packagesInTopologicalOrder =
      dependenciesInTopologicalOrder(upgradingPackages.map(_._1), upgradingPackagesMap)

    val packageMap = getUpgradablePackageIdVersionMap(packageMetadataSnapshot)

    def go(
        packageMap: PackageMap,
        packageSigs: Map[Ref.PackageId, Ast.PackageSignature],
        deps: List[Ref.PackageId],
    ): EitherT[FutureUnlessShutdown, RpcError, PackageMap] = deps match {
      case Nil => EitherT.pure[FutureUnlessShutdown, RpcError](packageMap)
      case pkgId :: rest =>
        val pkg = upgradingPackagesMap(pkgId)
        val supportsUpgrades = pkg.supportsUpgrades(pkgId)
        for {
          _ <- EitherTUtil.ifThenET(supportsUpgrades)(
            // This check will look for the closest neighbors of pkgId for the package versioning ordering and
            // will load them from the DB and decode them. If one were to upload many packages that upgrade each
            // other, we will end up decoding the same package many times. Some of these cases could be sped up
            // by a cache depending on the order in which the packages are uploaded.
            validatePackageUpgrade((pkgId, pkg), packageMap, packageSigs)
          )
          res <- go(
            packageMap + (pkgId -> (pkg.metadata.name, pkg.metadata.version)),
            packageSigs + (pkgId -> pkg),
            rest
          )
        } yield res
    }
    go(packageMap, packageMetadataSnapshot.packages, packagesInTopologicalOrder).map(_ => ())
  }

  private def getUpgradablePackageIdVersionMap(
      packageMetadataSnapshot: PackageMetadata
  ): Map[PackageId, (PackageName, Ref.PackageVersion)] =
    packageMetadataSnapshot.packageIdVersionMap.view.filterKeys { packageId =>
      packageMetadataSnapshot.packageUpgradabilityMap
        .getOrElse(
          packageId,
          throw new IllegalStateException(
            s"Inconsistent package metadata: package-id $packageId present in packageIdVersion map, missing from the package upgradability map $packageId"
          ),
        )
    }.toMap

  private def validatePackageUpgrade(
      uploadedPackage: (Ref.PackageId, Ast.PackageSignature),
      packageMap: PackageMap,
      packageSigs: Map[PackageId, Ast.PackageSignature]
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] = {
    val (uploadedPackageId, uploadedPackageAst) = uploadedPackage
    val optUpgradingDar = Some(uploadedPackage)
    val uploadedPackageIdWithMeta = PkgIdWithNameAndVersion(uploadedPackage)
    logger.info(
      s"Uploading DAR file for $uploadedPackageIdWithMeta in submission ID ${loggingContext.serializeFiltered("submissionId")}."
    )
    if (uploadedPackageAst.isInvalidDamlPrimOrStdlib(uploadedPackageId)) {
      EitherT.leftT[FutureUnlessShutdown, Unit](
        Validation.UpgradeDamlPrimIsNotAUtilityPackage
          .Error(
            uploadedPackage = uploadedPackageIdWithMeta
          ): RpcError
      )
    } else {
      existingVersionedPackageId(uploadedPackageAst, packageMap) match {
        case Some(existingPackageId) =>
          if (existingPackageId == uploadedPackageId) {
            logger.info(
              s"Ignoring upload of package $uploadedPackageIdWithMeta as it has been previously uploaded"
            )
            EitherT.rightT[FutureUnlessShutdown, RpcError](())
          } else {
            EitherT.leftT[FutureUnlessShutdown, Unit](
              Validation.UpgradeVersion
                .Error(
                  uploadedPackage = uploadedPackageIdWithMeta,
                  existingPackage = existingPackageId,
                  packageVersion = uploadedPackageAst.metadata.version,
                ): RpcError
            )
          }

        case None =>
          val optMaximalDar = maximalVersionedDar(uploadedPackageAst, packageMap, packageSigs)
          val optMinimalDar = minimalVersionedDar(uploadedPackageAst, packageMap, packageSigs)
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
            _ = logger.info(s"Typechecking upgrades for $uploadedPackageIdWithMeta succeeded.")
          } yield ()
      }
    }
  }

  private def existingVersionedPackageId(pkg: Ast.PackageSignature, packageMap: PackageMap): Option[Ref.PackageId] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap.collectFirst { case (pkgId, (`pkgName`, `pkgVersion`)) => pkgId }
  }

  private def minimalVersionedDar(
    pkg: Ast.PackageSignature,
    packageMap: PackageMap,
    packageSigs: Map[PackageId, Ast.PackageSignature],
  ): Option[(Ref.PackageId, Ast.PackageSignature)] =
    packageMap
      .collect { case (pkgId, (pkgName, pkgVersion)) if pkgName == pkg.metadata.name && pkgVersion > pkg.metadata.version =>
        (pkgId, pkgVersion)
      }
      .minByOption { case (_, version) => version }
      .map { case (pkgId, _) => 
        val pkgSig = packageSigs.getOrElse(
          pkgId,
          throw new IllegalStateException(
            s"Inconsistent package metadata: package-id $pkgId present in packageIdVersionMap, missing from packages"
          )
        )
        (pkgId, pkgSig)
  }

  private def maximalVersionedDar(
    pkg: Ast.PackageSignature,
    packageMap: PackageMap,
    packageSigs: Map[PackageId, Ast.PackageSignature],
  ): Option[(Ref.PackageId, Ast.PackageSignature)] =
    packageMap
      .collect { case (pkgId, (pkgName, pkgVersion)) if pkgName == pkg.metadata.name && pkgVersion < pkg.metadata.version =>
        (pkgId, pkgVersion)
      }
      .maxByOption { case (_, version) => version }
      .map { case (pkgId, _) => 
        val pkgSig = packageSigs.getOrElse(
          pkgId,
          throw new IllegalStateException(
            s"Inconsistent package metadata: package-id $pkgId present in packageIdVersionMap, missing from packages"
          )
        )
        (pkgId, pkgSig)
  }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck,
      packageMap: PackageMap,
      newDar1: (Ref.PackageId, Ast.PackageSignature),
      oldDar2: (Ref.PackageId, Ast.PackageSignature),
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
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
          ).leftMap[RpcError] {
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
      optNewDar1: Option[(Ref.PackageId, Ast.PackageSignature)],
      optOldDar2: Option[(Ref.PackageId, Ast.PackageSignature)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    (optNewDar1, optOldDar2) match {
      case (None, _) | (_, None) =>
        EitherT.rightT[FutureUnlessShutdown, RpcError](())

      case (Some((newPkgId1, newPkg1)), Some((oldPkgId2, oldPkg2))) =>
        strictTypecheckUpgrades(
          typecheckPhase,
          packageMap,
          (newPkgId1, newPkg1),
          (oldPkgId2, oldPkg2),
        )
    }
}
