// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlError
import com.daml.logging.entries.LoggingValue.OfString
import com.digitalasset.canton.ledger.error.PackageServiceErrors.{InternalError, Validation}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPackageManagementService.ErrorValidations
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.validation.{TypecheckUpgrades, UpgradeError}
import scalaz.std.either.*
import scalaz.std.option.*
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse.*

import scala.concurrent.{ExecutionContext, Future}

class PackageUpgradeValidator(
    getPackageMap: LoggingContextWithTrace => Map[
      Ref.PackageId,
      (Ref.PackageName, Ref.PackageVersion),
    ],
    getLfArchive: LoggingContextWithTrace => Ref.PackageId => Future[Option[Archive]],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  def validateUpgrade(uploadedPackage: (Ref.PackageId, Ast.Package))(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    val packageMap = getPackageMap(loggingContext)
    val (uploadedPackageId, uploadedPackageAst) = uploadedPackage
    val optUpgradingDar = Some(uploadedPackage)
    logger.info(
      s"Uploading DAR file for $uploadedPackageId in submission ID ${loggingContext.serializeFiltered("submissionId")}."
    )
    existingVersionedPackageId(uploadedPackageAst, packageMap) match {
      case Some(existingPackageId) =>
        if (existingPackageId == uploadedPackageId) {
          logger.info(
            s"Ignoring upload of package $uploadedPackageId as it has been previously uploaded"
          )
          EitherT.rightT[Future, DamlError](())
        } else {
          EitherT.leftT[Future, Unit](
            Validation.UpgradeVersion
              .Error(
                uploadedPackageId = uploadedPackageId,
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
            )
          )
          _ <- typecheckUpgrades(
            TypecheckUpgrades.MaximalDarCheck,
            optUpgradingDar,
            optMaximalDar,
          )
          optMinimalDar <- EitherT.right[DamlError](
            minimalVersionedDar(uploadedPackageAst, packageMap)
          )
          _ <- typecheckUpgrades(
            TypecheckUpgrades.MinimalDarCheck,
            optMinimalDar,
            optUpgradingDar,
          )
          _ = logger.info(s"Typechecking upgrades for $uploadedPackageId succeeded.")
        } yield ()
    }
  }

  private def lookupDar(pkgId: Ref.PackageId)(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] =
    for {
      optArchive <- getLfArchive(loggingContextWithTrace)(pkgId)
      optPackage <- Future.fromTry {
        optArchive
          .traverse(Decode.decodeArchive(_))
          .handleError(Validation.handleLfArchiveError)
      }
    } yield optPackage

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
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkgVersion < version }
      .minByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def maximalVersionedDar(
      pkg: Ast.Package,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkgVersion > version }
      .maxByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck,
      optNewDar1: Option[(Ref.PackageId, Ast.Package)],
      oldPkgId2: Ref.PackageId,
      optOldPkg2: Option[Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    LoggingContextWithTrace
      .withEnrichedLoggingContext("upgradeTypecheckPhase" -> OfString(phase.toString)) {
        implicit loggingContext =>
          optNewDar1 match {
            case None => EitherT.rightT(())

            case Some((newPkgId1, newPkg1)) =>
              logger.info(s"Package $newPkgId1 claims to upgrade package id $oldPkgId2")
              EitherT(
                Future(
                  TypecheckUpgrades
                    .typecheckUpgrades((newPkgId1, newPkg1), oldPkgId2, optOldPkg2)
                    .toEither
                )
              ).leftMap[DamlError] {
                case err: UpgradeError =>
                  Validation.Upgradeability.Error(
                    upgradingPackage = newPkgId1,
                    upgradedPackage = oldPkgId2,
                    upgradeError = err,
                  )
                case unhandledErr =>
                  InternalError.Unhandled(
                    unhandledErr,
                    Some(s"Typechecking upgrades for $oldPkgId2 failed with unknown error."),
                  )
              }
          }
      }
  }

  private def typecheckUpgrades(
      typecheckPhase: TypecheckUpgrades.UploadPhaseCheck,
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
          Some((newPkgId1, newPkg1)),
          oldPkgId2,
          Some(oldPkg2),
        )
    }
}
