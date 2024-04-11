// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.validation.{TypecheckUpgrades, UpgradeError}
import com.daml.logging.entries.LoggingValue.OfString
import com.digitalasset.canton.ledger.error.PackageServiceErrors.{InternalError, Validation}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPackageManagementService.ErrorValidations
import scalaz.std.either.*
import scalaz.std.option.*
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class PackageUpgradeValidator(
    getPackageMap: ContextualizedErrorLogger => Either[
      DamlError,
      Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
    ],
    getLfArchive: LoggingContextWithTrace => Ref.PackageId => Future[Option[Archive]],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  // TODO(##17635): Package upgrade validation is racy and can lead to actually allowing
  //                upgrade-conflicting packages to be uploaded when DARs are uploaded concurrently.
  def validateUpgrade(
      upgradingPackage: (Ref.PackageId, Ast.Package),
      upgradingPackageMetadata: Ast.PackageMetadata,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] =
    for {
      packageMap <- getPackageMap(errorLoggingContext).fold(
        err => Future.failed(err.asGrpcError),
        Future.successful,
      )
      (upgradingPackageId, upgradingPackageAst) = upgradingPackage
      optUpgradingDar = Some((upgradingPackageId, upgradingPackageAst))
      _ = logger.info(
        s"Uploading DAR file for $upgradingPackageId in submission ID ${loggingContext.serializeFiltered("submissionId")}."
      )
      result <- existingVersionedPackageId(upgradingPackageMetadata, packageMap) match {
        case Some(uploadedPackageId) =>
          if (uploadedPackageId == upgradingPackageId) {
            logger.info(
              s"Ignoring upload of package $upgradingPackageId as it has been previously uploaded"
            )
            Future.unit
          } else {
            Future.failed(
              Validation.UpgradeVersion
                .Error(uploadedPackageId, upgradingPackageId, upgradingPackageMetadata.version)
                .asGrpcError
            )
          }

        case None =>
          for {
            optMaximalDar <- maximalVersionedDar(
              upgradingPackageMetadata,
              packageMap,
            )
            _ <- typecheckUpgrades(
              TypecheckUpgrades.MaximalDarCheck,
              optUpgradingDar,
              optMaximalDar,
            )
            optMinimalDar <- minimalVersionedDar(upgradingPackageMetadata, packageMap)
            _ <- typecheckUpgrades(
              TypecheckUpgrades.MinimalDarCheck,
              optMinimalDar,
              optUpgradingDar,
            )
            _ = logger.info(s"Typechecking upgrades for $upgradingPackageId succeeded.")
          } yield ()
      }
    } yield result

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
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  ): Option[Ref.PackageId] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap.collectFirst { case (pkgId, (`pkgName`, `pkgVersion`)) => pkgId }
  }

  private def minimalVersionedDar(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
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
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
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
  ): Future[Unit] = {
    LoggingContextWithTrace.withEnrichedLoggingContext(
      "upgradeTypecheckPhase" -> OfString(phase.toString)
    ) { implicit loggingContext =>
      optNewDar1 match {
        case None =>
          Future.unit

        case Some((newPkgId1, newPkg1)) =>
          logger.info(s"Package $newPkgId1 claims to upgrade package id $oldPkgId2")
          Future
            .fromTry(
              TypecheckUpgrades.typecheckUpgrades((newPkgId1, newPkg1), oldPkgId2, optOldPkg2)
            )
            .recoverWith {
              case err: UpgradeError =>
                Future.failed(
                  Validation.Upgradeability
                    .Error(newPkgId1, oldPkgId2, err)
                    .asGrpcError
                )
              case NonFatal(err) =>
                Future.failed(
                  InternalError
                    .Unhandled(
                      err,
                      Some(
                        s"Typechecking upgrades for $oldPkgId2 failed with unknown error."
                      ),
                    )
                    .asGrpcError
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
  ): Future[Unit] = {
    (optNewDar1, optOldDar2) match {
      case (None, _) | (_, None) =>
        Future.unit

      case (Some((newPkgId1, newPkg1)), Some((oldPkgId2, oldPkg2))) =>
        strictTypecheckUpgrades(
          typecheckPhase,
          Some((newPkgId1, newPkg1)),
          oldPkgId2,
          Some(oldPkg2),
        )
    }
  }
}
