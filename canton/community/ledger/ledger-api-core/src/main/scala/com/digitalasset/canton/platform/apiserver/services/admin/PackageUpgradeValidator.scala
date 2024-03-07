// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.validation.{TypecheckUpgrades, UpgradeError, Upgrading}
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
  def validateUpgrade(upgradingPackage: (Ref.PackageId, Ast.Package))(implicit
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
      result <- existingVersionedPackageId(upgradingPackageAst, packageMap) match {
        case Some(uploadedPackageId) =>
          if (uploadedPackageId == upgradingPackageId) {
            logger.info(
              s"Ignoring upload of package $upgradingPackageId as it has been previously uploaded"
            )
            Future.unit
          } else {
            Future.failed(
              Validation.UpgradeVersion
                .Error(uploadedPackageId, upgradingPackageId, upgradingPackageAst.metadata.version)
                .asGrpcError
            )
          }

        case None =>
          upgradingPackageAst.metadata.upgradedPackageId match {
            case Some(upgradedPackageId) if packageMap.contains(upgradedPackageId) =>
              for {
                upgradedPackage <- lookupDar(upgradedPackageId).flatMap {
                  case Some((_, pkg)) =>
                    Future.successful(pkg)
                  case None =>
                    Future.failed(
                      Validation.Upgradeability
                        .Error(
                          upgradingPackageId,
                          upgradedPackageId,
                          UpgradeError(
                            UpgradeError.CouldNotResolveUpgradedPackageId(
                              Upgrading(upgradingPackageId, upgradedPackageId)
                            )
                          ),
                        )
                        .asGrpcError
                    )
                }
                _ <- strictTypecheckUpgrades(
                  TypecheckUpgrades.DarCheck,
                  optUpgradingDar,
                  upgradedPackageId,
                  Some(upgradedPackage),
                )
                optMaximalDar <- maximalVersionedDar(
                  upgradedPackageId,
                  upgradingPackageAst,
                  packageMap,
                )
                _ <- typecheckUpgrades(
                  TypecheckUpgrades.MaximalDarCheck,
                  optMaximalDar,
                  optUpgradingDar,
                )
                optMinimalDar <- minimalVersionedDar(upgradingPackageAst, packageMap)
                _ <- typecheckUpgrades(
                  TypecheckUpgrades.MinimalDarCheck,
                  optUpgradingDar,
                  optMinimalDar,
                )
                _ = logger.info(s"Typechecking upgrades for $upgradingPackageId succeeded.")
              } yield ()

            case Some(upgradedPackageId) =>
              Future.failed(
                Validation.Upgradeability
                  .Error(
                    upgradingPackageId,
                    upgradedPackageId,
                    UpgradeError(
                      UpgradeError.CouldNotResolveUpgradedPackageId(
                        Upgrading(upgradingPackageId, upgradedPackageId)
                      )
                    ),
                  )
                  .asGrpcError
              )

            case None =>
              logger.info(s"Package $upgradingPackageId does not upgrade anything.")
              Future.unit
          }
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
      upgradedPackageId: Ref.PackageId,
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
      // typecheckUpgrades has already been performed for upgradedPackageId, so we ignore this package Id for maximal
      // version checks
      .filter { case (pkgId, version) => pkgVersion > version && upgradedPackageId != pkgId }
      .maxByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck,
      optDar1: Option[(Ref.PackageId, Ast.Package)],
      pkgId2: Ref.PackageId,
      optPkg2: Option[Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    LoggingContextWithTrace.withEnrichedLoggingContext(
      "upgradeTypecheckPhase" -> OfString(phase.toString)
    ) { implicit loggingContext =>
      optDar1 match {
        case None =>
          Future.unit

        case Some((pkgId1, pkg1)) =>
          logger.info(s"Package $pkgId1 claims to upgrade package id $pkgId2")
          Future
            .fromTry(TypecheckUpgrades.typecheckUpgrades((pkgId1, pkg1), pkgId2, optPkg2))
            .recoverWith {
              case err: UpgradeError =>
                Future.failed(
                  Validation.Upgradeability
                    .Error(pkgId2, pkgId1, err)
                    .asGrpcError
                )
              case NonFatal(err) =>
                Future.failed(
                  InternalError
                    .Unhandled(
                      err,
                      Some(
                        s"Typechecking upgrades for $pkgId2 failed with unknown error."
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
      optDar1: Option[(Ref.PackageId, Ast.Package)],
      optDar2: Option[(Ref.PackageId, Ast.Package)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    (optDar1, optDar2) match {
      case (None, _) | (_, None) =>
        Future.unit

      case (Some((pkgId1, pkg1)), Some((pkgId2, pkg2))) =>
        strictTypecheckUpgrades(typecheckPhase, Some((pkgId1, pkg1)), pkgId2, Some(pkg2))
    }
  }
}
